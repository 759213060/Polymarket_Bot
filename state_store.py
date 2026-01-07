import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class OrderState:
    key: str
    market_slug: str
    outcome: str
    end_time: str
    status: str
    created_at: float
    updated_at: float
    total_notional: float
    total_size: float
    order_ids: list[str]


class JsonStateStore:
    def __init__(self, path: str, max_age_hours: float = 48.0):
        self.path = path
        self.max_age_seconds = max_age_hours * 3600.0
        self.state: Dict[str, Any] = {"version": 1, "orders": {}}

    def load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            self.state = json.load(f) or {"version": 1, "orders": {}}
        if "orders" not in self.state:
            self.state["orders"] = {}

    def save(self) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    def cleanup(self) -> None:
        now = time.time()
        orders = self.state.get("orders") or {}
        keys = list(orders.keys())
        for k in keys:
            updated_at = float((orders.get(k) or {}).get("updated_at") or 0)
            if updated_at and now - updated_at > self.max_age_seconds:
                orders.pop(k, None)
        self.state["orders"] = orders

    def get_order(self, key: str) -> Optional[Dict[str, Any]]:
        return (self.state.get("orders") or {}).get(key)

    def upsert_order(self, key: str, patch: Dict[str, Any]) -> None:
        orders = self.state.get("orders") or {}
        cur = orders.get(key) or {}
        cur.update(patch)
        cur["updated_at"] = time.time()
        orders[key] = cur
        self.state["orders"] = orders

