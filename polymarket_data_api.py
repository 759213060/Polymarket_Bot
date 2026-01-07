from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import urllib.parse

from config import PolymarketConfig
from http_client import HttpClient


@dataclass
class UserPosition:
    condition_id: str
    token_id: str
    size: float
    avg_price: float
    current_value: float
    cash_pnl: float
    title: str
    outcome: str
    end_date: str


class PolymarketDataApiClient:
    def __init__(self, cfg: PolymarketConfig, http: HttpClient):
        self.cfg = cfg
        self.http = http

    def get_positions(
        self,
        proxy_wallet: str,
        size_threshold: float = 1.0,
        limit: int = 200,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        params = {
            "user": proxy_wallet,
            "sizeThreshold": str(size_threshold),
            "limit": str(limit),
            "offset": str(offset),
        }
        url = self.cfg.data_api_base_url + "/positions?" + urllib.parse.urlencode(params)
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return []
        return data

    def get_positions_value(self, proxy_wallet: str) -> Optional[Dict[str, Any]]:
        params = {"user": proxy_wallet}
        url = self.cfg.data_api_base_url + "/value?" + urllib.parse.urlencode(params)
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return None
        return data

    def get_activity(self, proxy_wallet: str, limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
        params = {"user": proxy_wallet, "limit": str(limit), "offset": str(offset)}
        url = self.cfg.data_api_base_url + "/activity?" + urllib.parse.urlencode(params)
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return []
        return data
