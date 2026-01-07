import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict


def _load_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _guess_kind(market_slug: str) -> str:
    s = (market_slug or "").lower()
    if "15m" in s or "updown-15m" in s:
        return "15m"
    if "hour" in s or "hourly" in s:
        return "hourly"
    return "other"


@dataclass
class PaperLedgerPaths:
    trades_path: str
    stats_path: str


class PaperLedger:
    def __init__(self, paths: PaperLedgerPaths, initial_cash: float):
        self.paths = paths
        self.initial_cash = float(initial_cash)
        self._ensure_stats()
        self._ensure_trades()

    def _ensure_trades(self) -> None:
        if os.path.exists(self.paths.trades_path):
            return
        _save_json(self.paths.trades_path, [])

    def _ensure_stats(self) -> None:
        st = _load_json(self.paths.stats_path)
        if isinstance(st, dict) and "initial_cash" in st:
            return
        now = time.time()
        _save_json(
            self.paths.stats_path,
            {
                "initial_cash": float(self.initial_cash),
                "cash": float(self.initial_cash),
                "realized_pnl": 0.0,
                "fees_paid": 0.0,
                "buy_count": 0,
                "settle_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "total_buy_notional": 0.0,
                "total_payout": 0.0,
                "created_at": now,
                "updated_at": now,
            },
        )

    def _append_trade(self, record: Dict[str, Any]) -> None:
        trades = _load_json(self.paths.trades_path)
        if not isinstance(trades, list):
            trades = []
        trades.append(record)
        _save_json(self.paths.trades_path, trades)

    def _update_stats(self, patch: Dict[str, Any]) -> None:
        st = _load_json(self.paths.stats_path)
        if not isinstance(st, dict):
            st = {}
        st.update(patch)
        st["updated_at"] = time.time()
        _save_json(self.paths.stats_path, st)

    def record_buy(
        self,
        order_key: str,
        market_slug: str,
        asset_symbol: str,
        outcome: str,
        token_id: str,
        start_time_utc: str,
        end_time_utc: str,
        price: float,
        size: float,
        notional: float,
        fee_paid: float,
        cash_before: float,
        cash_after: float,
        order_type: str,
    ) -> None:
        now = time.time()
        rec = {
            "ts": now,
            "event": "buy",
            "kind": _guess_kind(market_slug),
            "order_key": order_key,
            "market_slug": market_slug,
            "asset_symbol": asset_symbol,
            "outcome": outcome,
            "token_id": str(token_id),
            "start_time_utc": start_time_utc,
            "end_time_utc": end_time_utc,
            "price": float(price),
            "size": float(size),
            "notional": float(notional),
            "fee_paid": float(fee_paid),
            "cash_before": float(cash_before),
            "cash_after": float(cash_after),
            "order_type": str(order_type or ""),
        }
        self._append_trade(rec)
        st = _load_json(self.paths.stats_path) or {}
        self._update_stats(
            {
                "cash": float(cash_after),
                "fees_paid": float(st.get("fees_paid") or 0.0) + float(fee_paid),
                "buy_count": int(st.get("buy_count") or 0) + 1,
                "total_buy_notional": float(st.get("total_buy_notional") or 0.0) + float(notional),
            }
        )

    def record_settlement(
        self,
        order_key: str,
        market_slug: str,
        asset_symbol: str,
        predicted_outcome: str,
        actual_outcome: str,
        token_id: str,
        start_time_utc: str,
        end_time_utc: str,
        open_price: float,
        close_price: float,
        change_pct: float,
        total_notional: float,
        total_size: float,
        fee_paid: float,
        payout: float,
        pnl: float,
        cash_after: float,
    ) -> None:
        now = time.time()
        win = bool(pnl >= 0 and payout > 0)
        rec = {
            "ts": now,
            "event": "settle",
            "kind": _guess_kind(market_slug),
            "order_key": order_key,
            "market_slug": market_slug,
            "asset_symbol": asset_symbol,
            "predicted_outcome": predicted_outcome,
            "actual_outcome": actual_outcome,
            "token_id": str(token_id),
            "start_time_utc": start_time_utc,
            "end_time_utc": end_time_utc,
            "open_price": float(open_price),
            "close_price": float(close_price),
            "change_pct": float(change_pct),
            "total_notional": float(total_notional),
            "total_size": float(total_size),
            "fee_paid": float(fee_paid),
            "payout": float(payout),
            "pnl": float(pnl),
            "win": bool(win),
            "cash_after": float(cash_after),
        }
        self._append_trade(rec)
        st = _load_json(self.paths.stats_path) or {}
        self._update_stats(
            {
                "cash": float(cash_after),
                "realized_pnl": float(st.get("realized_pnl") or 0.0) + float(pnl),
                "settle_count": int(st.get("settle_count") or 0) + 1,
                "win_count": int(st.get("win_count") or 0) + (1 if win else 0),
                "loss_count": int(st.get("loss_count") or 0) + (0 if win else 1),
                "total_payout": float(st.get("total_payout") or 0.0) + float(payout),
            }
        )

