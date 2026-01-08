import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import AppConfig
from binance_client import BinanceClient
from feishu_notifier import FeishuNotifier
from order_executor import OrderExecutor, OrderRequest, OrderResult
from live_ledger import LiveLedger
from paper_ledger import PaperLedger
from polymarket_data_api import PolymarketDataApiClient
from state_store import JsonStateStore
from fee_service import FeeService


@dataclass
class PositionSnapshot:
    token_sizes: Dict[str, float]
    token_values_usd: Dict[str, float]
    total_value_usd: float
    updated_at: float


class OrderManager:
    def __init__(
        self,
        cfg: AppConfig,
        executor: OrderExecutor,
        data_api: PolymarketDataApiClient,
        binance: BinanceClient,
        store: JsonStateStore,
        ledger: Optional[PaperLedger] = None,
        live_ledger: Optional[LiveLedger] = None,
        notifier: Optional[FeishuNotifier] = None,
        strategy: Optional[Any] = None,
        fee_service: Optional[FeeService] = None,
    ):
        self.cfg = cfg
        self.executor = executor
        self.data_api = data_api
        self.binance = binance
        self.store = store
        self.ledger = ledger
        self.live_ledger = live_ledger
        self.notifier = notifier
        self.strategy = strategy
        self.fee_service = fee_service
        self.positions = PositionSnapshot(token_sizes={}, token_values_usd={}, total_value_usd=0.0, updated_at=0.0)
        self.raw_positions: List[Dict] = []
        self.last_status_poll = 0.0
        self.equity_history: List[Dict] = [] # List of {"ts": float, "equity": float}
        self.last_history_record = 0.0
        self.live_cash_usd = float(cfg.live_cash_usd or 0.0)
        self.live_cash_updated_at = 0.0
        self.live_cash_spent_local_until = 0.0
        self.live_total_value_usd: Optional[float] = None
        self.live_positions_value_usd: Optional[float] = None
        self._live_activity_seen = set()
        seen = self.store.state.get("live_activity_seen") or []
        if isinstance(seen, list):
            for x in seen[-500:]:
                self._live_activity_seen.add(str(x))

    def _activity_key(self, ev: dict) -> str:
        for k in ("id", "txHash", "tx_hash", "hash", "orderID", "orderId", "order_id"):
            v = ev.get(k)
            if v:
                return str(v)
        ts = str(ev.get("timestamp") or ev.get("ts") or ev.get("createdAt") or ev.get("created_at") or "")
        t = str(ev.get("type") or ev.get("event") or "")
        m = str(ev.get("market") or ev.get("market_slug") or ev.get("conditionId") or "")
        return ts + "|" + t + "|" + m

    def _load_recent_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        path: str | None = None
        if self.cfg.mode == "paper":
            if self.ledger and hasattr(self.ledger, "paths"):
                path = getattr(self.ledger.paths, "trades_path", None)
        elif self.cfg.mode == "live":
            if self.live_ledger and hasattr(self.live_ledger, "trades_path"):
                path = getattr(self.live_ledger, "trades_path", None)
        if not path or not isinstance(path, str) or not path:
            return []
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return []
        if not isinstance(data, list) or not data:
            return []
        rows = data[-limit:]
        out: List[Dict[str, Any]] = []
        for r in reversed(rows):
            if not isinstance(r, dict):
                continue
            ts_raw = r.get("ts")
            ts_iso = None
            try:
                if ts_raw is not None:
                    ts_iso = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc).isoformat()
            except Exception:
                ts_iso = None

            if self.cfg.mode == "paper":
                ev = str(r.get("event") or "")
                if ev == "buy":
                    out.append(
                        {
                            "ts": ts_iso,
                            "type": "buy",
                            "market": r.get("market_slug") or "",
                            "symbol": r.get("asset_symbol") or "",
                            "outcome": r.get("outcome") or "",
                            "notional": r.get("notional"),
                            "price": r.get("price"),
                            "size": r.get("size"),
                            "fee": r.get("fee_paid"),
                            "pnl": None,
                            "result": "",
                            "detail": r.get("order_type") or "",
                            "link": f"https://polymarket.com/event/{(r.get('market_slug') or '')}" if r.get("market_slug") else "#",
                        }
                    )
                elif ev == "settle":
                    win = bool(r.get("win"))
                    out.append(
                        {
                            "ts": ts_iso,
                            "type": "settle",
                            "market": r.get("market_slug") or "",
                            "symbol": r.get("asset_symbol") or "",
                            "outcome": r.get("predicted_outcome") or "",
                            "notional": r.get("total_notional"),
                            "price": None,
                            "size": r.get("total_size"),
                            "fee": r.get("fee_paid"),
                            "pnl": r.get("pnl"),
                            "result": "win" if win else "lose",
                            "detail": f"{r.get('actual_outcome') or ''} {r.get('change_pct') or ''}",
                            "link": f"https://polymarket.com/event/{(r.get('market_slug') or '')}" if r.get("market_slug") else "#",
                        }
                    )
                else:
                    out.append({"ts": ts_iso, "type": ev or "paper", "detail": str(r)[:500]})
            else:
                typ = str(r.get("type") or "")
                if typ == "order_submit":
                    d = r.get("data") if isinstance(r.get("data"), dict) else {}
                    out.append(
                        {
                            "ts": ts_iso,
                            "type": "order_submit",
                            "market": d.get("market_slug") or "",
                            "symbol": d.get("asset_symbol") or "",
                            "outcome": d.get("outcome") or "",
                            "notional": d.get("notional"),
                            "price": d.get("price"),
                            "size": d.get("size"),
                            "fee": d.get("fee_estimated") or d.get("fee_est"),
                            "pnl": None,
                            "result": "",
                            "detail": d.get("order_id") or "",
                            "link": f"https://polymarket.com/event/{(d.get('market_slug') or '')}" if d.get("market_slug") else "#",
                        }
                    )
                elif typ == "activity":
                    raw = r.get("raw") if isinstance(r.get("raw"), dict) else {}
                    pnl = None
                    for k in ("pnl", "cash_pnl", "cashPnl", "realizedPnl", "realized_pnl"):
                        if k in raw:
                            try:
                                pnl = float(raw.get(k))
                            except Exception:
                                pnl = None
                            break
                    out.append(
                        {
                            "ts": ts_iso,
                            "type": "activity",
                            "market": "",
                            "symbol": raw.get("asset") or raw.get("symbol") or "",
                            "outcome": raw.get("outcome") or "",
                            "notional": raw.get("notional") or raw.get("amount") or raw.get("value"),
                            "price": raw.get("price") or raw.get("avgPrice"),
                            "size": raw.get("size"),
                            "fee": raw.get("fee") or raw.get("fees") or raw.get("feePaid") or raw.get("fee_paid"),
                            "pnl": pnl,
                            "result": "",
                            "detail": raw.get("type") or raw.get("action") or "",
                            "link": "#",
                        }
                    )
                else:
                    out.append({"ts": ts_iso, "type": typ or "live", "detail": str(r)[:500]})
        return out

    def _notify(self, text: str) -> None:
        n = self.notifier
        if not n:
            return
        try:
            n.send_text(text)
        except Exception:
            return

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def load(self) -> None:
        self.store.load()
        self.store.cleanup()
        if self.cfg.mode == "paper":
            paper = self.store.state.get("paper") or {}
            if "cash" not in paper:
                paper = {
                    "cash": float(self.cfg.paper_initial_cash),
                    "realized_pnl": 0.0,
                    "fees_paid": 0.0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "started_at": self._utc_now(),
                }
                self.store.state["paper"] = paper
        self.store.save()

    def refresh_positions_if_needed(self) -> None:
        wallet = (self.cfg.wallet_address or "").strip()
        if not wallet:
            return
        now = time.time()
        if now - self.positions.updated_at < self.cfg.strategy.positions_refresh_seconds:
            return
        raw = self.data_api.get_positions(wallet, size_threshold=0.0, limit=500, offset=0)
        self.raw_positions = raw
        token_sizes: Dict[str, float] = {}
        token_values_usd: Dict[str, float] = {}
        total_value_usd = 0.0
        for p in raw:
            token = str(
                p.get("asset")
                or p.get("tokenId")
                or p.get("token_id")
                or p.get("clobTokenId")
                or p.get("clob_token_id")
                or ""
            )
            if not token:
                continue
            try:
                size = float(p.get("size") or p.get("positionSize") or p.get("position_size") or 0.0)
            except Exception:
                size = 0.0
            if size:
                token_sizes[token] = token_sizes.get(token, 0.0) + size
            v = self._extract_position_value_usd(p)
            if v is None:
                v = self._estimate_position_value_usd(p)
            if v is not None:
                v = float(max(v, 0.0))
                token_values_usd[token] = token_values_usd.get(token, 0.0) + v
                total_value_usd += v
        self.positions = PositionSnapshot(
            token_sizes=token_sizes, token_values_usd=token_values_usd, total_value_usd=float(total_value_usd), updated_at=now
        )

    def _extract_cash_usd(self, value: dict) -> Optional[float]:
        candidates = [
            "cash",
            "cashUsd",
            "cash_usd",
            "cashBalance",
            "cashBalanceUsd",
            "usdc",
            "usdcBalance",
            "usdc_balance",
            "buyingPower",
            "buyingPowerUsd",
        ]
        for k in candidates:
            if k in value:
                try:
                    raw = value.get(k)
                    if raw is None:
                        continue
                    return float(raw)
                except Exception:
                    continue
        return None

    def _extract_total_value_usd(self, value: dict) -> Optional[float]:
        candidates = [
            "totalValue",
            "total_value",
            "accountValue",
            "account_value",
            "portfolioValue",
            "portfolio_value",
            "netWorth",
            "net_worth",
            "equity",
            "value",
        ]
        for k in candidates:
            if k in value:
                try:
                    raw = value.get(k)
                    if raw is None:
                        continue
                    return float(raw)
                except Exception:
                    continue
        return None

    def _extract_positions_value_usd(self, value: dict) -> Optional[float]:
        candidates = [
            "positionsValue",
            "positions_value",
            "positionValue",
            "position_value",
            "holdingsValue",
            "holdings_value",
        ]
        for k in candidates:
            if k in value:
                try:
                    raw = value.get(k)
                    if raw is None:
                        continue
                    return float(raw)
                except Exception:
                    continue
        return None

    def _extract_position_value_usd(self, p: dict) -> Optional[float]:
        candidates = [
            "currentValue",
            "current_value",
            "value",
            "usdValue",
            "usd_value",
            "positionValue",
            "position_value",
        ]
        for k in candidates:
            if k in p:
                try:
                    raw = p.get(k)
                    if raw is None:
                        continue
                    return float(raw)
                except Exception:
                    continue
        return None

    def _estimate_position_value_usd(self, p: dict) -> Optional[float]:
        try:
            size = float(p.get("size") or p.get("positionSize") or p.get("position_size") or 0.0)
        except Exception:
            size = 0.0
        avg = None
        for k in ("avgPrice", "avg_price", "averagePrice", "average_price", "costBasis", "cost_basis"):
            if k in p:
                try:
                    raw = p.get(k)
                    if raw is None:
                        continue
                    avg = float(raw)
                    break
                except Exception:
                    continue
        if avg is None:
            return None
        return float(abs(size) * avg)

    def refresh_live_cash_if_needed(self) -> None:
        if self.cfg.mode != "live":
            return
        if not self.cfg.live_enable_auto_balance:
            return
        wallet = (self.cfg.wallet_address or "").strip()
        if not wallet:
            return
        now = time.time()
        if now - self.live_cash_updated_at < float(self.cfg.live_balance_refresh_seconds):
            return
        try:
            v = self.data_api.get_positions_value(wallet)
        except Exception as e:
            self._notify(f"余额刷新失败：{str(e)}")
            return
        self.live_cash_updated_at = now
        if not isinstance(v, dict):
            return
        cash = self._extract_cash_usd(v)
        total_value = self._extract_total_value_usd(v)
        positions_value = self._extract_positions_value_usd(v)
        if cash is not None:
            cash = float(cash)
            if time.time() < float(self.live_cash_spent_local_until or 0.0):
                if cash < float(self.live_cash_usd or 0.0):
                    self.live_cash_usd = cash
            else:
                self.live_cash_usd = cash
        if total_value is not None:
            self.live_total_value_usd = float(total_value)
        if positions_value is not None:
            self.live_positions_value_usd = float(positions_value)
        if self.live_ledger:
            try:
                self.live_ledger.save_value_snapshot(v, cash_usd=cash)
            except Exception:
                pass

    def _order_key(self, order: OrderRequest) -> str:
        end_key = order.end_time.astimezone(timezone.utc).isoformat()
        return f"{order.market_slug}|{order.outcome}|{end_key}"

    def _cleanup_old(self) -> None:
        self.store.cleanup()

    def should_skip(self, order: OrderRequest) -> bool:
        key = self._order_key(order)
        st = self.store.get_order(key)
        if not st:
            return False
        status = (st.get("status") or "").lower()
        if status in ("submitted", "filled", "settled"):
            return True
        return False

    def _position_exposure(self, token_id: str) -> float:
        return float(self.positions.token_sizes.get(str(token_id)) or 0.0)

    def _plan_children(self, order: OrderRequest) -> List[OrderRequest]:
        max_child = float(self.cfg.strategy.max_child_notional)
        if max_child <= 0 or order.notional <= max_child:
            return [order]
        remaining = float(order.notional)
        children: List[OrderRequest] = []
        while remaining > 0:
            notional = min(max_child, remaining)
            size = notional / order.price
            children.append(
                OrderRequest(
                    market_slug=order.market_slug,
                    asset_symbol=order.asset_symbol,
                    outcome=order.outcome,
                    token_id=order.token_id,
                    price=order.price,
                    size=size,
                    notional=notional,
                    start_time=order.start_time,
                    end_time=order.end_time,
                    order_type=order.order_type,
                )
            )
            remaining -= notional
        return children

    def _paper_cash(self) -> float:
        paper = self.store.state.get("paper") or {}
        try:
            return float(paper.get("cash") or 0.0)
        except Exception:
            return 0.0

    def _paper_open_orders_count(self) -> int:
        orders = self.store.state.get("orders") or {}
        n = 0
        for _, st in list(orders.items()):
            status = (st.get("status") or "").lower()
            if status == "submitted":
                n += 1
        return n

    def _open_orders_notional(self, statuses: List[str]) -> float:
        orders = self.store.state.get("orders") or {}
        total = 0.0
        allow = {s.lower() for s in statuses}
        for _, st in list(orders.items()):
            status = (st.get("status") or "").lower()
            if status not in allow:
                continue
            try:
                total += float(st.get("total_notional") or 0.0)
            except Exception:
                continue
        return float(total)

    def _live_open_orders_notional(self) -> float:
        orders = self.store.state.get("orders") or {}
        total = 0.0
        for _, st in list(orders.items()):
            status = (st.get("status") or "").lower()
            if status == "submitted":
                try:
                    total += float(st.get("total_notional") or 0.0)
                except Exception:
                    continue
                continue
            if status == "filled":
                token_id = str(st.get("token_id") or "")
                if token_id:
                    try:
                        if float(self.positions.token_sizes.get(token_id) or 0.0) > 0.0:
                            continue
                    except Exception:
                        pass
                try:
                    total += float(st.get("total_notional") or 0.0)
                except Exception:
                    continue
        return float(total)

    def _max_total_exposure_usd(self) -> float:
        frac = float(getattr(self.cfg, "max_total_exposure_fraction", 0.0) or 0.0)
        if frac <= 0:
            return 0.0
        threshold = float(self.cfg.live_roll_threshold_usd or 0.0)
        below_cap = float(self.cfg.live_max_notional_below_threshold or 0.0)
        if self.cfg.mode == "paper":
            cash = self._paper_cash()
            open_notional = self._open_orders_notional(["submitted", "filled"])
            equity = float(cash) + float(open_notional)
            if threshold > 0 and equity < threshold:
                return float(max(below_cap, 0.0))
            return float(max(equity * frac, 0.0))
        cash = float(self.live_cash_usd or 0.0)
        open_orders = self._live_open_orders_notional()
        if self.live_total_value_usd is not None:
            equity = float(self.live_total_value_usd)
            if threshold > 0 and equity < threshold:
                return float(max(below_cap, 0.0))
            return float(max(equity * frac, 0.0))
        positions_value = float(
            self.live_positions_value_usd
            if self.live_positions_value_usd is not None
            else float(self.positions.total_value_usd or 0.0)
        )
        equity = cash + positions_value + float(open_orders)
        if threshold > 0 and equity < threshold:
            return float(max(below_cap, 0.0))
        return float(max(equity * frac, 0.0))

    def _current_total_exposure_usd(self) -> float:
        if self.cfg.mode == "paper":
            return float(max(self._open_orders_notional(["submitted", "filled"]), 0.0))
        positions_value = float(
            self.live_positions_value_usd
            if self.live_positions_value_usd is not None
            else float(self.positions.total_value_usd or 0.0)
        )
        open_orders = self._live_open_orders_notional()
        return float(max(positions_value + float(open_orders), 0.0))

    def _remaining_total_exposure_usd(self) -> float:
        cap = self._max_total_exposure_usd()
        used = self._current_total_exposure_usd()
        return float(max(cap - used, 0.0))

    def _paper_apply_buy(self, notional: float) -> float:
        if self.fee_service:
            fee_rate = self.fee_service.get_trade_fee_rate()
        else:
            fee_rate = float(self.cfg.strategy.fee_estimate)
        fee = fee_rate * float(notional)
        paper = self.store.state.get("paper") or {}
        cash = float(paper.get("cash") or 0.0)
        cash -= float(notional) + fee
        paper["cash"] = cash
        paper["fees_paid"] = float(paper.get("fees_paid") or 0.0) + fee
        paper["trades"] = int(paper.get("trades") or 0) + 1
        self.store.state["paper"] = paper
        return fee

    def _paper_apply_settlement(self, pnl: float, payout: float, win: bool, settlement_fee: float = 0.0) -> None:
        paper = self.store.state.get("paper") or {}
        paper["cash"] = float(paper.get("cash") or 0.0) + float(payout) - settlement_fee
        paper["realized_pnl"] = float(paper.get("realized_pnl") or 0.0) + float(pnl)
        paper["fees_paid"] = float(paper.get("fees_paid") or 0.0) + settlement_fee
        if win:
            paper["wins"] = int(paper.get("wins") or 0) + 1
        else:
            paper["losses"] = int(paper.get("losses") or 0) + 1
        self.store.state["paper"] = paper

    def _live_max_notional(self) -> float:
        cash = float(self.live_cash_usd or 0.0)
        if cash <= 0:
            return 0.0
        threshold = float(self.cfg.live_roll_threshold_usd or 0.0)
        if cash < threshold:
            return float(self.cfg.live_max_notional_below_threshold or 0.0)
        return cash * float(self.cfg.live_max_fraction_above_threshold or 0.0)

    def submit_with_risk(self, order: OrderRequest) -> List[OrderResult]:
        self.refresh_live_cash_if_needed()
        self.refresh_positions_if_needed()
        self._cleanup_old()

        if self.should_skip(order):
            return []

        existing_tokens = self._position_exposure(order.token_id)
        if existing_tokens > 0:
            return []

        results: List[OrderResult] = []
        key = self._order_key(order)
        if not self.store.get_order(key):
            self.store.upsert_order(
                key,
                {
                    "market_slug": order.market_slug,
                    "outcome": order.outcome,
                    "token_id": str(order.token_id),
                    "asset_symbol": order.asset_symbol,
                    "start_time": order.start_time.astimezone(timezone.utc).isoformat(),
                    "end_time": order.end_time.astimezone(timezone.utc).isoformat(),
                    "status": "planned",
                    "created_at": time.time(),
                    "order_ids": [],
                    "total_notional": 0.0,
                    "total_size": 0.0,
                    "fee_paid": 0.0,
                },
            )
            self.store.save()
            self._notify(
                "发现交易机会（已计划）\n"
                + f"时间(UTC)：{self._utc_now()}\n"
                + f"模式：{self.cfg.mode}\n"
                + f"标的：{order.asset_symbol}\n"
                + f"方向：{order.outcome}\n"
                + f"价格：{round(order.price, 4)}\n"
                + f"数量：{round(order.size, 6)}\n"
                + f"金额：{round(order.notional, 2)}\n"
                + f"TokenID：{order.token_id}\n"
                + f"市场：{order.market_slug}\n"
                + f"到期(UTC)：{order.end_time.astimezone(timezone.utc).isoformat()}"
            )

        children = self._plan_children(order)
        for child in children:
            if datetime.now(timezone.utc) >= child.end_time:
                continue
            exposure_remaining = self._remaining_total_exposure_usd()
            if exposure_remaining <= 0:
                break
            if self.cfg.mode == "paper":
                if self._paper_open_orders_count() >= int(self.cfg.paper_max_open_orders):
                    break
                cash = self._paper_cash()
                if cash <= 0:
                    break
                fee_rate = float(self.cfg.strategy.fee_estimate)
                spendable = cash / (1.0 + fee_rate) if fee_rate >= 0 else cash
                max_by_fraction = spendable * float(self.cfg.paper_max_fraction_per_trade)
                notional = min(child.notional, spendable, max_by_fraction, float(exposure_remaining))
                if notional < float(self.cfg.paper_min_notional):
                    break
                if abs(notional - child.notional) > 1e-9:
                    size = notional / child.price
                    child = OrderRequest(
                        market_slug=child.market_slug,
                        asset_symbol=child.asset_symbol,
                        outcome=child.outcome,
                        token_id=child.token_id,
                        price=child.price,
                        size=size,
                        notional=float(notional),
                        start_time=child.start_time,
                        end_time=child.end_time,
                        order_type=child.order_type,
                    )
            if self.cfg.mode == "live":
                cap = self._live_max_notional()
                cash = float(self.live_cash_usd or 0.0)
                if self.fee_service:
                    fee_rate = self.fee_service.get_trade_fee_rate()
                else:
                    fee_rate = float(self.cfg.strategy.fee_estimate)
                spendable = cash / (1.0 + fee_rate) if fee_rate >= 0 else cash
                if cap > 0:
                    notional = min(float(child.notional), float(cap), float(spendable), float(exposure_remaining))
                    if notional < float(self.cfg.live_min_notional):
                        break
                    if abs(notional - child.notional) > 1e-9:
                        size = notional / child.price
                        child = OrderRequest(
                            market_slug=child.market_slug,
                            asset_symbol=child.asset_symbol,
                            outcome=child.outcome,
                            token_id=child.token_id,
                            price=child.price,
                            size=size,
                            notional=float(notional),
                            start_time=child.start_time,
                            end_time=child.end_time,
                            order_type=child.order_type,
                        )
            resp = self.executor.submit(child)
            results.append(resp)
            if resp.success:
                self._notify(
                    "下单成功\n"
                    + f"时间(UTC)：{self._utc_now()}\n"
                    + f"模式：{self.cfg.mode}\n"
                    + f"标的：{child.asset_symbol}\n"
                    + f"方向：{child.outcome}\n"
                    + f"价格：{round(child.price, 4)}\n"
                    + f"数量：{round(child.size, 6)}\n"
                    + f"金额：{round(child.notional, 2)}\n"
                    + f"TokenID：{child.token_id}\n"
                    + f"市场：{child.market_slug}\n"
                    + f"订单号：{resp.order_id or '无'}"
                )
                if self.cfg.mode == "live":
                    cash_before = float(self.live_cash_usd or 0.0)
                    if self.fee_service:
                        fee_est = self.fee_service.get_trade_fee_rate() * float(child.notional)
                    else:
                        fee_est = float(self.cfg.strategy.fee_estimate) * float(child.notional)
                    cash_after = cash_before - float(child.notional) - float(fee_est)
                    self.live_cash_usd = float(max(cash_after, 0.0))
                    self.live_cash_spent_local_until = time.time() + float(self.cfg.live_balance_refresh_seconds) * 2.0
                    if self.live_ledger:
                        try:
                            self.live_ledger.append_order_submission(
                                {
                                    "order_key": key,
                                    "order_id": resp.order_id,
                                    "market_slug": child.market_slug,
                                    "asset_symbol": child.asset_symbol,
                                    "outcome": child.outcome,
                                    "token_id": str(child.token_id),
                                    "price": float(child.price),
                                    "size": float(child.size),
                                    "notional": float(child.notional),
                                    "fee_estimated": float(fee_est),
                                    "cash_before": float(cash_before),
                                    "cash_after": float(cash_after),
                                    "order_type": str(child.order_type or ""),
                                }
                            )
                        except Exception:
                            pass
            else:
                self._notify(
                    "下单失败\n"
                    + f"时间(UTC)：{self._utc_now()}\n"
                    + f"模式：{self.cfg.mode}\n"
                    + f"标的：{child.asset_symbol}\n"
                    + f"方向：{child.outcome}\n"
                    + f"价格：{round(child.price, 4)}\n"
                    + f"数量：{round(child.size, 6)}\n"
                    + f"金额：{round(child.notional, 2)}\n"
                    + f"TokenID：{child.token_id}\n"
                    + f"市场：{child.market_slug}\n"
                    + f"错误：{resp.message}"
                )
            st = self.store.get_order(key) or {}
            order_ids = list(st.get("order_ids") or [])
            if resp.order_id:
                order_ids.append(resp.order_id)
            total_notional = float(st.get("total_notional") or 0.0) + child.notional
            total_size = float(st.get("total_size") or 0.0) + child.size
            fee_paid = float(st.get("fee_paid") or 0.0)
            if resp.success and self.cfg.mode == "paper":
                cash_before = self._paper_cash()
                added_fee = self._paper_apply_buy(child.notional)
                fee_paid += added_fee
                cash_after = self._paper_cash()
                if self.ledger:
                    try:
                        self.ledger.record_buy(
                            order_key=key,
                            market_slug=child.market_slug,
                            asset_symbol=child.asset_symbol,
                            outcome=child.outcome,
                            token_id=str(child.token_id),
                            start_time_utc=child.start_time.astimezone(timezone.utc).isoformat(),
                            end_time_utc=child.end_time.astimezone(timezone.utc).isoformat(),
                            price=float(child.price),
                            size=float(child.size),
                            notional=float(child.notional),
                            fee_paid=float(added_fee),
                            cash_before=float(cash_before),
                            cash_after=float(cash_after),
                            order_type=str(child.order_type or ""),
                        )
                    except Exception:
                        pass
            self.store.upsert_order(
                key,
                {
                    "status": "submitted" if resp.success else "error",
                    "order_ids": order_ids,
                    "total_notional": total_notional,
                    "total_size": total_size,
                    "fee_paid": fee_paid,
                    "last_error": "" if resp.success else resp.message,
                },
            )
            self.store.save()
            time.sleep(float(self.cfg.strategy.child_order_spacing_seconds))
        return results

    def poll_status(self) -> None:
        now = time.time()
        if now - self.last_status_poll < self.cfg.strategy.poll_order_status_seconds:
            return
        self.last_status_poll = now
        self.refresh_live_cash_if_needed()
        self.refresh_positions_if_needed()
        if self.cfg.mode == "live" and self.live_ledger:
            wallet = (self.cfg.wallet_address or "").strip()
            if wallet:
                try:
                    acts = self.data_api.get_activity(wallet, limit=100, offset=0)
                    for ev in acts:
                        k = self._activity_key(ev if isinstance(ev, dict) else {})
                        if k and k not in self._live_activity_seen:
                            self._live_activity_seen.add(k)
                            self.live_ledger.append_activity(ev)
                    self.store.state["live_activity_seen"] = list(self._live_activity_seen)[-500:]
                    self.store.save()
                except Exception:
                    pass
        orders = self.store.state.get("orders") or {}
        for key, st in list(orders.items()):
            status = (st.get("status") or "").lower()
            if status != "submitted":
                continue
            end_time = st.get("end_time") or ""
            token_id = str(st.get("token_id") or "")
            asset_symbol = str(st.get("asset_symbol") or "")
            outcome = str(st.get("outcome") or "")
            if token_id:
                try:
                    if float(self.positions.token_sizes.get(token_id) or 0.0) > 0.0:
                        orders[key]["status"] = "filled"
                        orders[key]["updated_at"] = now
                        self._notify(
                            "仓位更新：疑似已成交（检测到仓位）\n"
                            + f"时间(UTC)：{self._utc_now()}\n"
                            + f"模式：{self.cfg.mode}\n"
                            + f"标的方向：{st.get('outcome') or ''}\n"
                            + f"TokenID：{token_id}\n"
                            + f"市场：{st.get('market_slug') or ''}\n"
                            + f"累计下单金额：{round(float(st.get('total_notional') or 0.0), 2)}\n"
                            + f"累计下单数量：{round(float(st.get('total_size') or 0.0), 6)}"
                        )
                        continue
                except Exception:
                    pass
            if end_time:
                try:
                    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")).astimezone(timezone.utc)
                    if datetime.now(timezone.utc) > end_dt:
                        # 通用逻辑：获取Binance价格判断胜负，用于更新策略风控状态
                        start_raw = str(st.get("start_time") or "")
                        win = False
                        actual = "Unknown"
                        open_p, close_p, chg = 0.0, 0.0, 0.0
                        
                        try:
                            if start_raw:
                                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
                                open_p, close_p, chg = self.binance.get_open_close_change(asset_symbol, start_dt, end_dt)
                                actual = "Up" if chg > 0 else "Down"
                                win = actual == outcome
                                
                                # 更新策略风控状态 (无论是 Paper 还是 Live)
                                if self.strategy and hasattr(self.strategy, 'update_trade_result'):
                                    try:
                                        self.strategy.update_trade_result(asset_symbol, win)
                                    except Exception:
                                        pass
                        except Exception as e:
                            # 如果获取价格失败，可能稍后重试，或者标记为未知
                            pass

                        if self.cfg.mode == "paper":
                            if start_raw:
                                total_notional = float(st.get("total_notional") or 0.0)
                                total_size = float(st.get("total_size") or 0.0)
                                fee_paid = float(st.get("fee_paid") or 0.0)
                                payout = total_size if win else 0.0
                                settlement_fee = 0.0
                                if self.fee_service:
                                    settlement_fee = self.fee_service.get_settlement_fee_usd()
                                
                                pnl = payout - total_notional - fee_paid - settlement_fee
                                self._paper_apply_settlement(pnl=pnl, payout=payout, win=win, settlement_fee=settlement_fee)
                                cash_after = self._paper_cash()
                                if self.ledger:
                                    try:
                                        self.ledger.record_settlement(
                                            order_key=key,
                                            market_slug=str(st.get("market_slug") or ""),
                                            asset_symbol=asset_symbol,
                                            predicted_outcome=outcome,
                                            actual_outcome=actual,
                                            token_id=token_id,
                                            start_time_utc=start_dt.astimezone(timezone.utc).isoformat(),
                                            end_time_utc=end_dt.astimezone(timezone.utc).isoformat(),
                                            open_price=float(open_p),
                                            close_price=float(close_p),
                                            change_pct=float(chg),
                                            total_notional=float(total_notional),
                                            total_size=float(total_size),
                                            fee_paid=float(fee_paid),
                                            payout=float(payout),
                                            pnl=float(pnl),
                                            cash_after=float(cash_after),
                                        )
                                    except Exception:
                                        pass
                                orders[key]["status"] = "settled"
                                orders[key]["result"] = "win" if win else "lose"
                                orders[key]["settled_at"] = self._utc_now()
                                
                                self._notify(
                                    "模拟仓结算\n"
                                    + f"时间(UTC)：{self._utc_now()}\n"
                                    + f"标的：{asset_symbol}\n"
                                    + f"预测方向：{outcome}\n"
                                    + f"实际方向：{actual}\n"
                                    + f"起始价：{round(float(open_p), 4)}\n"
                                    + f"结束价：{round(float(close_p), 4)}\n"
                                    + f"区间涨跌：{round(float(chg) * 100.0, 4)}%\n"
                                    + f"投入金额：{round(total_notional, 2)}\n"
                                    + f"份额：{round(total_size, 6)}\n"
                                    + f"费用：{round(fee_paid, 4)}\n"
                                    + f"结算返还：{round(payout, 6)}\n"
                                    + f"本笔盈亏：{round(pnl, 6)}\n"
                                    + f"余额：{round(cash_after, 6)}\n"
                                    + f"累计盈亏：{round(float((self.store.state.get('paper') or {}).get('realized_pnl') or 0.0), 6)}"
                                )
                                self.store.save()
                                continue
                        
                        # Live 模式下的处理 (保留原有的过期通知，但现在已经更新了策略状态)
                        orders[key]["status"] = "expired"
                        orders[key]["updated_at"] = now
                        self._notify(
                            "订单到期：未检测到仓位（已过期）\n"
                            + f"时间(UTC)：{self._utc_now()}\n"
                            + f"模式：{self.cfg.mode}\n"
                            + f"标的方向：{st.get('outcome') or ''}\n"
                            + f"TokenID：{token_id}\n"
                            + f"市场：{st.get('market_slug') or ''}\n"
                            + f"到期(UTC)：{end_dt.isoformat()}\n"
                            + f"累计下单金额：{round(float(st.get('total_notional') or 0.0), 2)}\n"
                            + f"累计下单数量：{round(float(st.get('total_size') or 0.0), 6)}"
                        )
                except Exception:
                    pass
        self.store.state["orders"] = orders
        self.store.save()

    def _update_history(self, equity: float):
        now = time.time()
        # Record every 60 seconds
        if now - self.last_history_record >= 60:
            self.equity_history.append({"ts": now * 1000, "value": equity})
            # Keep last 1440 points
            if len(self.equity_history) > 1440:
                self.equity_history.pop(0)
            self.last_history_record = now

    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Return stats for web dashboard."""
        mode = self.cfg.mode
        stats = {
            "mode": mode,
            "ts": datetime.now(timezone.utc).isoformat(),
            "cash": 0.0,
            "equity": 0.0,
            "pnl": 0.0,
            "positions": [],
            "recent_trades": [],
            "history": []
        }
        stats["recent_trades"] = self._load_recent_trades(limit=50)

        if mode == "paper":
            paper = self.store.state.get("paper") or {}
            cash = float(paper.get("cash") or 0.0)
            stats["cash"] = cash
            stats["pnl"] = float(paper.get("realized_pnl") or 0.0)
            
            orders = self.store.state.get("orders") or {}
            pos_list = []
            equity_adjustment = 0.0
            
            for k, v in orders.items():
                status = (v.get("status") or "").lower()
                if status not in ("submitted", "filled"):
                    continue
                size = float(v.get("total_size") or 0.0)
                if size <= 0:
                    continue
                
                notional = float(v.get("total_notional") or 0.0)
                price = notional / size if size > 0 else 0.0
                
                symbol = v.get("asset_symbol") or "?"
                outcome = v.get("outcome") or "?"
                market = v.get("market_slug") or ""
                val = size * price 
                equity_adjustment += val
                
                pos_list.append({
                    "market": market,
                    "symbol": symbol,
                    "outcome": outcome,
                    "size": size,
                    "entry_price": price,
                    "value": val,
                    "link": f"https://polymarket.com/event/{market}" if market else "#"
                })
            
            stats["equity"] = cash + equity_adjustment
            stats["positions"] = pos_list

        elif mode == "live":
            stats["cash"] = self.live_cash_usd
            stats["equity"] = self.live_total_value_usd if self.live_total_value_usd else self.live_cash_usd
            
            pos_list = []
            for p in self.raw_positions:
                size = float(p.get("size") or 0.0)
                if size < 0.000001: continue
                asset = p.get("asset") or "?"
                title = p.get("title") or asset
                market_slug = p.get("slug") or ""
                
                if not market_slug and "market" in p and isinstance(p["market"], dict):
                    market_slug = p["market"].get("slug") or ""

                cur_val = self._extract_position_value_usd(p)
                if cur_val is None:
                    cur_val = self._estimate_position_value_usd(p) or 0.0
                
                pos_list.append({
                    "market": title,
                    "symbol": asset,
                    "outcome": p.get("outcome") or "?",
                    "size": size,
                    "entry_price": float(p.get("avgPrice") or 0.0),
                    "value": cur_val,
                    "link": f"https://polymarket.com/event/{market_slug}" if market_slug else "#"
                })
            stats["positions"] = pos_list
        
        self._update_history(stats["equity"])
        stats["history"] = self.equity_history
        return stats
