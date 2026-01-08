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
        self.traded_keys: Dict[str, float] = {}
        self.last_trade_ts_by_asset: Dict[str, float] = {}
        self.price_history: Dict[str, List[float]] = {}
        self.consecutive_losses: Dict[str, int] = {}  # 连续亏损计数
        self.asset_performance: Dict[str, Dict[str, int]] = {}  # 资产表现统计

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
            # 多维度交易信号
            should_trade = False
            desired = "Up" if change_pct > 0 else "Down"
            
            # 信号1: 传统阈值突破
            if abs(change_pct) >= dynamic_threshold:
                should_trade = True
            
            # 信号2: 中等幅度但高置信度 (0.5%-1%)
            elif 0.5 <= abs(change_pct) < dynamic_threshold and abs(change_pct) > 0.8 * float(vol or 0.0):
                should_trade = True
            
            # 信号3: 趋势延续 (同方向连续波动)
            elif abs(change_pct) >= 0.3 and self._check_trend_continuation(m.asset_symbol, change_pct > 0):
                should_trade = True
            
            # 风险管理: 检查连续亏损
            if should_trade and self._has_too_many_losses(m.asset_symbol):
                should_trade = False
            
            if not should_trade:
                continue
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
            self.traded_keys[key] = m.end_time.timestamp()
            self.last_trade_ts_by_asset[m.asset_symbol] = now.timestamp()
            
            # 更新价格历史
            if m.asset_symbol not in self.price_history:
                self.price_history[m.asset_symbol] = []
            self.price_history[m.asset_symbol].append(close_price)
            if len(self.price_history[m.asset_symbol]) > 20:  # 保留最近20个价格
                self.price_history[m.asset_symbol].pop(0)
        return orders
    
    def update_trade_result(self, asset_symbol: str, is_win: bool) -> None:
        """更新交易结果，用于风险管理"""
        if asset_symbol not in self.consecutive_losses:
            self.consecutive_losses[asset_symbol] = 0
        if asset_symbol not in self.asset_performance:
            self.asset_performance[asset_symbol] = {"wins": 0, "losses": 0}
        
        if is_win:
            self.consecutive_losses[asset_symbol] = 0
            self.asset_performance[asset_symbol]["wins"] += 1
        else:
            self.consecutive_losses[asset_symbol] += 1
            self.asset_performance[asset_symbol]["losses"] += 1
    
    def _has_too_many_losses(self, asset_symbol: str) -> bool:
        """检查是否连续亏损过多"""
        if asset_symbol not in self.consecutive_losses:
            return False
        return self.consecutive_losses[asset_symbol] >= 3  # 连续3次亏损后暂停交易
    
    def _check_trend_continuation(self, asset_symbol: str, is_up: bool) -> bool:
        """检查趋势是否延续"""
        if asset_symbol not in self.price_history or len(self.price_history[asset_symbol]) < 5:
            return False
        
        prices = self.price_history[asset_symbol]
        recent_prices = prices[-5:]  # 最近5个价格
        
        # 计算短期趋势
        if is_up:
            # 上涨趋势：最近价格应该持续上涨或高位震荡
            return all(recent_prices[i] >= recent_prices[i-1] * 0.98 for i in range(1, len(recent_prices)))
        else:
            # 下跌趋势：最近价格应该持续下跌或低位震荡
            return all(recent_prices[i] <= recent_prices[i-1] * 1.02 for i in range(1, len(recent_prices)))
