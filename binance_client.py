from datetime import datetime
from typing import List, Tuple
import urllib.parse
import math

from http_client import HttpClient
from config import BinanceConfig


class BinanceClient:
    def __init__(self, cfg: BinanceConfig, http: HttpClient):
        self.cfg = cfg
        self.http = http

    def get_open_close_change(self, symbol_key: str, start_time: datetime, end_time: datetime) -> Tuple[float, float, float]:
        data = self._get_klines_1m(symbol_key, start_time, end_time)
        open_price, close_price = self._open_close_from_klines(data)
        change_pct = (close_price - open_price) / open_price
        return open_price, close_price, change_pct

    def get_open_close_change_and_volatility(
        self, symbol_key: str, start_time: datetime, end_time: datetime
    ) -> Tuple[float, float, float, float]:
        data = self._get_klines_1m(symbol_key, start_time, end_time)
        open_price, close_price = self._open_close_from_klines(data)
        change_pct = (close_price - open_price) / open_price
        vol_pct = self._stdev_minute_log_returns(data)
        return open_price, close_price, change_pct, vol_pct

    def get_price(self, symbol: str) -> float:
        """
        Get current price for a symbol (e.g. 'MATICUSDT').
        Does not use symbol_map, expects raw Binance symbol.
        """
        base = f"{self.cfg.base_url}/api/v3/ticker/price"
        params = {"symbol": symbol}
        url = base + "?" + urllib.parse.urlencode(params)
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data or "price" not in data:
            raise RuntimeError(f"Failed to get price for {symbol}")
        return float(data["price"])

    def _get_klines_1m(self, symbol_key: str, start_time: datetime, end_time: datetime) -> List[list]:
        symbol = self.cfg.symbol_map[symbol_key]
        base = f"{self.cfg.base_url}/api/v3/klines"
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": "1000",
        }
        url = base + "?" + urllib.parse.urlencode(params)
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            raise RuntimeError("No kline data")
        return data

    def _open_close_from_klines(self, data: List[list]) -> Tuple[float, float]:
        first = data[0]
        last = data[-1]
        open_price = float(first[1])
        close_price = float(last[4])
        return open_price, close_price

    def _stdev_minute_log_returns(self, data: List[list]) -> float:
        if len(data) < 3:
            return 0.0
        prev_close = float(data[0][4])
        returns: List[float] = []
        for k in data[1:]:
            close = float(k[4])
            if prev_close > 0 and close > 0:
                returns.append(math.log(close / prev_close))
            prev_close = close
        n = len(returns)
        if n < 2:
            return 0.0
        mean = sum(returns) / n
        var = sum((r - mean) ** 2 for r in returns) / (n - 1)
        return math.sqrt(var)
