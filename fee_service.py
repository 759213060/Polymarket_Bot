import time
from typing import Optional
from http_client import HttpClient
from binance_client import BinanceClient
from config import StrategyConfig

class FeeService:
    def __init__(self, http: HttpClient, binance: BinanceClient, cfg: StrategyConfig):
        self.http = http
        self.binance = binance
        self.cfg = cfg
        self.last_update = 0.0
        self.cached_gas_price_gwei = 50.0 # Conservative default
        self.cached_matic_price = 0.5     # Conservative default
        self._gas_station_url = "https://gasstation.polygon.technology/v2"

    def update(self):
        # Update every 60 seconds
        if time.time() - self.last_update < 60:
            return
        
        # 1. Get Gas Price (Fast)
        try:
            # Polymarket recommends using "Fast" or "Standard" + margin
            # Gas Station V2: {"safeLow":..., "standard":..., "fast":..., "estimatedBaseFee":..., ...}
            # Values are usually in Gwei.
            # Example response:
            # { "safeLow": { "maxPriorityFee": 30.0, "maxFee": 30.0 }, ... }
            # Or simplified format depending on endpoint.
            # Let's try to handle common format.
            data = self.http.get_json(self._gas_station_url)
            if data:
                if "fast" in data and isinstance(data["fast"], dict):
                    # V2 format
                    self.cached_gas_price_gwei = float(data["fast"].get("maxFee", 50.0))
                elif "fast" in data and isinstance(data["fast"], (int, float)):
                    # V1 format?
                    self.cached_gas_price_gwei = float(data["fast"])
        except Exception:
            # Fallback
            pass

        # 2. Get MATIC Price
        try:
            price = self.binance.get_price("MATICUSDT")
            if price > 0:
                self.cached_matic_price = price
        except Exception:
            pass
        
        self.last_update = time.time()

    def get_trade_fee_rate(self) -> float:
        """
        Return the fee rate for trading (opening/closing positions on CLOB).
        Polymarket is generally 0 fees for Limit/Market orders (Maker/Taker).
        """
        # Could make this configurable, but 0 is correct for Polymarket CLOB
        return 0.0

    def get_settlement_fee_usd(self, gas_limit: int = 200000) -> float:
        """
        Estimate the cost of settlement (Redeem) in USD.
        This is a blockchain transaction.
        Default gas limit 200k is conservative for CTF exchange redeem.
        """
        self.update()
        eth_per_gwei = 1e-9
        fee_in_matic = float(gas_limit) * self.cached_gas_price_gwei * eth_per_gwei
        return fee_in_matic * self.cached_matic_price
