import json
import os
import time
from typing import Any, Dict, List, Optional


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

def _max_records() -> int:
    try:
        v = int(os.getenv("LEDGER_MAX_RECORDS", "5000"))
        return max(100, v)
    except Exception:
        return 5000


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _extract_number(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for k in keys:
        if k in d:
            v = _to_float(d.get(k))
            if v is not None:
                return v
    return None


class LiveLedger:
    def __init__(self, trades_path: str, stats_path: str):
        self.trades_path = trades_path
        self.stats_path = stats_path
        self._ensure()

    def _ensure(self) -> None:
        if not os.path.exists(self.trades_path):
            _save_json(self.trades_path, [])
        st = _load_json(self.stats_path)
        if isinstance(st, dict) and "created_at" in st:
            return
        now = time.time()
        _save_json(
            self.stats_path,
            {
                "created_at": now,
                "updated_at": now,
                "events": 0,
                "trade_events": 0,
                "fee_paid": 0.0,
                "fee_estimated": 0.0,
                "notional": 0.0,
                "pnl_reported": 0.0,
                "last_activity_ts": None,
                "last_value_snapshot": None,
            },
        )

    def append_order_submission(self, data: Dict[str, Any]) -> None:
        now = time.time()
        trades = _load_json(self.trades_path)
        if not isinstance(trades, list):
            trades = []
        record = {"ts": now, "type": "order_submit", "data": data}
        trades.append(record)
        max_n = _max_records()
        if len(trades) > max_n:
            trades = trades[-max_n:]
        _save_json(self.trades_path, trades)

        st = _load_json(self.stats_path) or {}
        st["events"] = int(st.get("events") or 0) + 1
        st["trade_events"] = int(st.get("trade_events") or 0) + 1

        notional = _extract_number(data, ["notional", "usd_notional"])
        if notional is not None:
            st["notional"] = float(st.get("notional") or 0.0) + float(notional)

        fee_est = _extract_number(data, ["fee_estimated", "fee_est"])
        if fee_est is not None:
            st["fee_estimated"] = float(st.get("fee_estimated") or 0.0) + float(fee_est)

        st["updated_at"] = now
        _save_json(self.stats_path, st)

    def append_activity(self, ev: Dict[str, Any]) -> None:
        now = time.time()
        trades = _load_json(self.trades_path)
        if not isinstance(trades, list):
            trades = []
        record = {"ts": now, "type": "activity", "raw": ev}
        trades.append(record)
        max_n = _max_records()
        if len(trades) > max_n:
            trades = trades[-max_n:]
        _save_json(self.trades_path, trades)

        st = _load_json(self.stats_path) or {}
        fee = _extract_number(ev, ["fee", "fees", "feePaid", "fee_paid"])
        notional = _extract_number(ev, ["notional", "amount", "usdAmount", "usdcAmount", "value"])
        pnl = _extract_number(ev, ["pnl", "cash_pnl", "cashPnl", "realizedPnl", "realized_pnl"])

        st["events"] = int(st.get("events") or 0) + 1
        if fee is not None:
            st["fee_paid"] = float(st.get("fee_paid") or 0.0) + float(fee)
        if notional is not None:
            st["notional"] = float(st.get("notional") or 0.0) + float(notional)
        if pnl is not None:
            st["pnl_reported"] = float(st.get("pnl_reported") or 0.0) + float(pnl)
        act_ts = _extract_number(ev, ["timestamp", "ts", "createdAt", "created_at"])
        if act_ts is not None:
            st["last_activity_ts"] = act_ts
        st["updated_at"] = now
        _save_json(self.stats_path, st)

    def save_value_snapshot(self, value: Dict[str, Any], cash_usd: Optional[float]) -> None:
        now = time.time()
        st = _load_json(self.stats_path) or {}
        st["last_value_snapshot"] = {"ts": now, "cash_usd": cash_usd, "raw": value}
        st["updated_at"] = now
        _save_json(self.stats_path, st)
