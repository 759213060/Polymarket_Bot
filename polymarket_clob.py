from typing import Optional
import urllib.parse

from http_client import HttpClient
from config import PolymarketConfig


class PolymarketClobClient:
    def __init__(self, cfg: PolymarketConfig, http: HttpClient):
        self.cfg = cfg
        self.http = http

    def get_best_ask(self, token_id: str) -> Optional[float]:
        params = {"token_id": token_id, "side": "buy"}
        url = self.cfg.clob_base_url + "/price?" + urllib.parse.urlencode(params)
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return None
        price_str = data.get("price")
        if price_str is None:
            return None
        return float(price_str)

