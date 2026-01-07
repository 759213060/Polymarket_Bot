from datetime import datetime, timezone
from typing import Dict, List, Set

from config import StrategyConfig, BinanceConfig, PolymarketConfig
from binance_client import BinanceClient
from polymarket_gamma import UpDownMarket
from polymarket_clob import PolymarketClobClient
from order_executor import OrderRequest


class UpDownLagArbStrategy:
    def __init__(
        self,
        cfg: StrategyConfig,
        binance_cfg: BinanceConfig,
        poly_cfg: PolymarketConfig,
        binance_client: BinanceClient,
        clob_client: PolymarketClobClient,
    ):
        self.cfg = cfg
        self.binance_cfg = binance_cfg
        self.poly_cfg = poly_cfg
        self.binance_client = binance_client
        self.clob_client = clob_client
        self.traded_keys: Set[str] = set()
        self.last_trade_ts_by_asset: Dict[str, float] = {}

    def generate_orders(self, markets: List[UpDownMarket]) -> List[OrderRequest]:
        now = datetime.now(timezone.utc)
        orders: List[OrderRequest] = []
        total_notional = 0.0
        for m in markets:
            key = m.market_slug + "|" + m.asset_symbol
            if key in self.traded_keys:
                continue
            last_ts = float(self.last_trade_ts_by_asset.get(m.asset_symbol) or 0.0)
            if last_ts and (now.timestamp() - last_ts) < float(getattr(self.cfg, "trade_cooldown_seconds", 0.0) or 0.0):
                continue
            time_to_end_min = (m.end_time - now).total_seconds() / 60.0
            if time_to_end_min < self.cfg.min_time_to_end_minutes:
                continue
            if time_to_end_min > self.cfg.max_time_to_end_minutes:
                continue
            if m.asset_symbol not in self.binance_cfg.symbol_map:
                continue
            if now <= m.start_time:
                continue
            try:
                open_price, close_price, change_pct, vol = self.binance_client.get_open_close_change_and_volatility(
                    m.asset_symbol, m.start_time, now
                )
            except Exception:
                continue
            vol_mult = float(getattr(self.cfg, "volatility_mult", 0.0) or 0.0)
            dynamic_threshold = float(self.cfg.min_abs_return_pct)
            if vol_mult > 0:
                dynamic_threshold = max(dynamic_threshold, vol_mult * float(vol or 0.0))
            if abs(change_pct) < dynamic_threshold:
                continue
            if change_pct > 0:
                desired = "Up"
            else:
                desired = "Down"
            if desired not in m.outcomes:
                continue
            idx = m.outcomes.index(desired)
            token_id = m.outcome_token_ids[idx]
            best_ask = self.clob_client.get_best_ask(token_id)
            if best_ask is None:
                continue
            if best_ask > self.cfg.max_entry_price:
                continue
            edge = 1.0 - best_ask - self.cfg.fee_estimate
            if edge < self.cfg.min_edge:
                continue
            remaining = self.cfg.max_total_notional - total_notional
            if remaining <= 0:
                break
            per_market = self.cfg.max_notional_per_market
            per_trade = self.cfg.max_notional_per_trade
            notional = min(remaining, per_market, per_trade)
            size = notional / best_ask
            order = OrderRequest(
                market_slug=m.market_slug,
                asset_symbol=m.asset_symbol,
                outcome=desired,
                token_id=token_id,
                price=best_ask,
                size=size,
                notional=notional,
                start_time=m.start_time,
                end_time=m.end_time,
            )
            orders.append(order)
            total_notional += notional
            self.traded_keys.add(key)
            self.last_trade_ts_by_asset[m.asset_symbol] = now.timestamp()
        return orders
