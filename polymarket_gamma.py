from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Optional
import json
import math

from http_client import HttpClient
from config import PolymarketConfig


@dataclass
class UpDownMarket:
    asset_symbol: str
    event_slug: str
    market_id: str
    market_slug: str
    question: str
    end_time: datetime
    start_time: datetime
    outcomes: List[str]
    outcome_token_ids: List[str]
    neg_risk: bool = False
    tick_size: str = "0.01"


class PolymarketGammaClient:
    def __init__(self, cfg: PolymarketConfig, http: HttpClient):
        self.cfg = cfg
        self.http = http

    def _parse_datetime(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    def discover_updown_markets_15m(self, asset_symbol: str, horizon_minutes: float) -> List[UpDownMarket]:
        prefix = {
            "BTC": "btc-updown-15m",
            "ETH": "eth-updown-15m",
            "XRP": "xrp-updown-15m",
            "SOL": "sol-updown-15m",
        }.get(asset_symbol)
        if not prefix:
            return []
        now = datetime.now(timezone.utc)
        horizon_end = now.timestamp() + horizon_minutes * 60.0
        start_epoch = int(math.floor(now.timestamp() / (15 * 60)) * (15 * 60))
        results: List[UpDownMarket] = []
        t = start_epoch
        while t <= horizon_end:
            slug = f"{prefix}-{t}"
            market = self._get_market_by_slug(slug, asset_symbol=asset_symbol)
            if market:
                results.append(market)
            t += 15 * 60
        return results

    def discover_updown_markets_hourly(self, asset_symbol: str, horizon_minutes: float) -> List[UpDownMarket]:
        series_slug = self.cfg.series_slugs_hourly.get(asset_symbol)
        if not series_slug:
            return []
        url = f"{self.cfg.gamma_base_url}/series?slug={series_slug}&limit=1"
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return []
        if isinstance(data, list):
            series = data[0]
        else:
            series = data
        events = series.get("events") or []
        now = datetime.now(timezone.utc)
        result: List[UpDownMarket] = []
        for ev in events:
            end_raw = ev.get("endDate")
            if not end_raw:
                continue
            end_time = self._parse_datetime(end_raw)
            delta_min = (end_time - now).total_seconds() / 60.0
            if delta_min < 0 or delta_min > horizon_minutes:
                continue
            slug = ev.get("slug")
            if not slug:
                continue
            event_detail = self._get_event_by_slug(slug)
            if not event_detail:
                continue
            markets = event_detail.get("markets") or []
            for m in markets:
                start_time = self._extract_market_start_time(m, event_detail, end_time)
                outcomes_raw = m.get("outcomes") or "[]"
                tokens_raw = m.get("clobTokenIds") or "[]"
                try:
                    outcomes = json.loads(outcomes_raw)
                    token_ids = json.loads(tokens_raw)
                except Exception:
                    continue
                if not outcomes or not token_ids or len(outcomes) != len(token_ids):
                    continue
                market = UpDownMarket(
                    asset_symbol=asset_symbol,
                    event_slug=slug,
                    market_id=str(m.get("id")),
                    market_slug=m.get("slug") or slug,
                    question=m.get("question") or ev.get("title") or "",
                    end_time=end_time,
                    start_time=start_time,
                    outcomes=outcomes,
                    outcome_token_ids=token_ids,
                    neg_risk=bool(m.get("negRisk") or event_detail.get("negRisk") or False),
                )
                result.append(market)
        return result

    def _get_event_by_slug(self, slug: str) -> Dict:
        url = f"{self.cfg.gamma_base_url}/events/slug/{slug}"
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return {}
        if isinstance(data, list):
            return data[0]
        return data

    def _get_market_by_slug(self, slug: str, asset_symbol: str = "") -> Optional[UpDownMarket]:
        url = f"{self.cfg.gamma_base_url}/markets?slug={slug}"
        data = self.http.get_json(url, {"accept": "application/json"})
        if not data:
            return None
        m = data[0] if isinstance(data, list) else data
        end_raw = m.get("endDate")
        question = m.get("question") or m.get("title") or slug
        if not end_raw:
            return None
        end_time = self._parse_datetime(end_raw)
        start_time = self._derive_start_time_from_slug_or_market(slug, m, end_time)
        outcomes_raw = m.get("outcomes") or "[]"
        tokens_raw = m.get("clobTokenIds") or "[]"
        try:
            outcomes = json.loads(outcomes_raw)
            token_ids = json.loads(tokens_raw)
        except Exception:
            return None
        if not outcomes or not token_ids or len(outcomes) != len(token_ids):
            return None
        return UpDownMarket(
            asset_symbol=asset_symbol or self._infer_asset_from_question(question),
            event_slug=m.get("eventSlug") or slug,
            market_id=str(m.get("id")),
            market_slug=m.get("slug") or slug,
            question=question,
            end_time=end_time,
            start_time=start_time,
            outcomes=outcomes,
            outcome_token_ids=token_ids,
            neg_risk=bool(m.get("negRisk") or False),
        )

    def _infer_asset_from_question(self, question: str) -> str:
        q = question.lower()
        if "bitcoin" in q or " btc " in (" " + q + " "):
            return "BTC"
        if "ethereum" in q or " eth " in (" " + q + " "):
            return "ETH"
        if "xrp" in q:
            return "XRP"
        if "solana" in q or " sol " in (" " + q + " "):
            return "SOL"
        return ""

    def _derive_start_time_from_slug_or_market(self, slug: str, market: Dict, end_time: datetime) -> datetime:
        parts = slug.split("-")
        if len(parts) >= 1:
            last = parts[-1]
            if last.isdigit():
                return datetime.fromtimestamp(int(last), tz=timezone.utc)
        for k in ("eventStartTime", "startTime", "startDate"):
            v = market.get(k)
            if isinstance(v, str) and v:
                return self._parse_datetime(v)
        return end_time

    def _extract_market_start_time(self, market: Dict, event_detail: Dict, end_time: datetime) -> datetime:
        for k in ("eventStartTime", "startTime"):
            v = market.get(k)
            if isinstance(v, str) and v:
                return self._parse_datetime(v)
        for k in ("startTime", "startDate"):
            v = event_detail.get(k)
            if isinstance(v, str) and v:
                return self._parse_datetime(v)
        return end_time
