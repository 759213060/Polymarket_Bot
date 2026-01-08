import os
import json
from dataclasses import dataclass, field
from typing import Dict
from pathlib import Path


# 从同目录 .env 加载环境变量（会覆盖同名环境变量，以 .env 为准）
def _strip_quotes(v: str) -> str:
    if len(v) >= 2 and ((v[0] == v[-1] == "'") or (v[0] == v[-1] == '"')):
        return v[1:-1]
    return v


def _load_dotenv(path: str = ".env") -> None:
    p = Path(path)
    if not p.exists() or not p.is_file():
        return
    try:
        text = p.read_text(encoding="utf-8")
    except Exception:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        if not k:
            continue
        v = _strip_quotes(v.strip())
        if k not in os.environ or (os.environ.get(k) or "") == "":
            os.environ[k] = v


def _load_markets_config() -> Dict:
    p = Path("markets.json")
    if p.exists() and p.is_file():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _get_binance_symbol_map() -> Dict[str, str]:
    data = _load_markets_config()
    return data.get("binance", {
        "BTC": "BTCUSDT",
        "ETH": "ETHUSDT",
        "XRP": "XRPUSDT",
        "SOL": "SOLUSDT",
    })


def _get_polymarket_slugs() -> Dict[str, str]:
    data = _load_markets_config()
    return data.get("polymarket", {
        "BTC": "btc-up-or-down-hourly",
        "ETH": "eth-up-or-down-hourly",
        "XRP": "xrp-up-or-down-hourly",
        "SOL": "sol-up-or-down-hourly",
    })


@dataclass
class BinanceConfig:
    base_url: str = "https://api.binance.com"
    symbol_map: Dict[str, str] = field(default_factory=_get_binance_symbol_map)


@dataclass
class PolymarketConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    data_api_base_url: str = "https://data-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    series_slugs_hourly: Dict[str, str] = field(default_factory=_get_polymarket_slugs)
    enable_hourly: bool = True
    enable_15m: bool = True


@dataclass
class StrategyConfig:
    lookback_seconds: int = 3600
    min_abs_return_pct: float = 0.0008  # 降低阈值，捕捉更多机会
    max_entry_price: float = 0.92       # 提高最大入场价格
    fee_estimate: float = 0.01
    min_edge: float = 0.003             # 降低最小边缘要求
    min_time_to_end_minutes: float = 2.0 # 增加最小剩余时间
    max_time_to_end_minutes: float = 90.0 # 延长最大剩余时间
    volatility_mult: float = 2.0         # 增加波动率乘数
    trade_cooldown_seconds: float = 90.0 # 减少交易冷却时间
    max_notional_per_trade: float = 500.0
    max_notional_per_market: float = 2000.0
    max_total_notional: float = 5000.0
    hourly_horizon_minutes: float = 120.0
    m15_horizon_minutes: float = 45.0
    max_child_notional: float = 150.0
    child_order_spacing_seconds: float = 0.35
    poll_order_status_seconds: float = 2.0
    positions_refresh_seconds: float = 15.0
    state_file_path: str = ".bot_state.json"
    max_state_age_hours: float = 48.0


@dataclass
class AppConfig:
    mode: str
    wallet_address: str
    binance: BinanceConfig
    polymarket: PolymarketConfig
    strategy: StrategyConfig
    poll_interval_seconds: float = 5.0
    feishu_webhook_url: str = ""
    max_total_exposure_fraction: float = 0.1
    paper_initial_cash: float = 10.0
    paper_max_fraction_per_trade: float = 0.1
    paper_max_open_orders: int = 1
    paper_min_notional: float = 1.0
    paper_trades_file_path: str = ".paper_trades.json"
    paper_stats_file_path: str = ".paper_stats.json"
    live_cash_usd: float = 0.0
    live_roll_threshold_usd: float = 10.0
    live_max_notional_below_threshold: float = 1.0
    live_max_fraction_above_threshold: float = 0.1
    live_min_notional: float = 1.0
    live_balance_refresh_seconds: float = 30.0
    live_enable_auto_balance: bool = True
    live_trades_file_path: str = ".live_trades.json"
    live_stats_file_path: str = ".live_stats.json"


def load_config() -> AppConfig:
    _load_dotenv()
    # BOT_MODE: paper（模拟）/ live（真实下单）
    mode = os.getenv("BOT_MODE", "paper")
    # POLYMARKET_WALLET: 用于 Data API 查询仓位的地址
    # POLYMARKET_FUNDER_ADDRESS: live 下单时的 funder 地址（也会作为 wallet fallback）
    wallet = os.getenv("POLYMARKET_WALLET", "") or os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
    binance = BinanceConfig()
    polymarket = PolymarketConfig()
    strategy = StrategyConfig()
    if os.getenv("STRAT_MIN_ABS_RETURN_PCT"):
        strategy.min_abs_return_pct = float(os.getenv("STRAT_MIN_ABS_RETURN_PCT", str(strategy.min_abs_return_pct)))
    if os.getenv("STRAT_MAX_ENTRY_PRICE"):
        strategy.max_entry_price = float(os.getenv("STRAT_MAX_ENTRY_PRICE", str(strategy.max_entry_price)))
    if os.getenv("STRAT_FEE_ESTIMATE"):
        strategy.fee_estimate = float(os.getenv("STRAT_FEE_ESTIMATE", str(strategy.fee_estimate)))
    if os.getenv("STRAT_MIN_EDGE"):
        strategy.min_edge = float(os.getenv("STRAT_MIN_EDGE", str(strategy.min_edge)))
    if os.getenv("STRAT_MIN_TIME_TO_END_MINUTES"):
        strategy.min_time_to_end_minutes = float(
            os.getenv("STRAT_MIN_TIME_TO_END_MINUTES", str(strategy.min_time_to_end_minutes))
        )
    if os.getenv("STRAT_MAX_TIME_TO_END_MINUTES"):
        strategy.max_time_to_end_minutes = float(
            os.getenv("STRAT_MAX_TIME_TO_END_MINUTES", str(strategy.max_time_to_end_minutes))
        )
    if os.getenv("STRAT_VOLATILITY_MULT"):
        strategy.volatility_mult = float(os.getenv("STRAT_VOLATILITY_MULT", str(strategy.volatility_mult)))
    if os.getenv("STRAT_TRADE_COOLDOWN_SECONDS"):
        strategy.trade_cooldown_seconds = float(
            os.getenv("STRAT_TRADE_COOLDOWN_SECONDS", str(strategy.trade_cooldown_seconds))
        )
    # FEISHU_WEBHOOK_URL: 留空则不发送飞书提醒
    feishu_webhook_url = os.getenv("FEISHU_WEBHOOK_URL", "")
    max_total_exposure_fraction = float(os.getenv("MAX_TOTAL_EXPOSURE_FRACTION", "0.1"))
    paper_initial_cash = float(os.getenv("PAPER_INITIAL_CASH", "10"))
    paper_max_fraction_per_trade = float(os.getenv("PAPER_MAX_FRACTION_PER_TRADE", "0.5"))
    paper_min_notional = float(os.getenv("PAPER_MIN_NOTIONAL", "1"))
    paper_max_open_orders = int(os.getenv("PAPER_MAX_OPEN_ORDERS", "3"))
    paper_trades_file_path = os.getenv("PAPER_TRADES_FILE_PATH", ".paper_trades.json")
    paper_stats_file_path = os.getenv("PAPER_STATS_FILE_PATH", ".paper_stats.json")
    live_cash_usd = float(os.getenv("LIVE_CASH_USD", "0"))
    live_roll_threshold_usd = float(os.getenv("LIVE_ROLL_THRESHOLD_USD", "10"))
    live_max_notional_below_threshold = float(os.getenv("LIVE_MAX_NOTIONAL_BELOW_THRESHOLD", "1"))
    live_max_fraction_above_threshold = float(os.getenv("LIVE_MAX_FRACTION_ABOVE_THRESHOLD", "0.1"))
    live_min_notional = float(os.getenv("LIVE_MIN_NOTIONAL", "1"))
    live_balance_refresh_seconds = float(os.getenv("LIVE_BALANCE_REFRESH_SECONDS", "30"))
    live_enable_auto_balance = os.getenv("LIVE_ENABLE_AUTO_BALANCE", "1") not in ("0", "false", "False")
    live_trades_file_path = os.getenv("LIVE_TRADES_FILE_PATH", ".live_trades.json")
    live_stats_file_path = os.getenv("LIVE_STATS_FILE_PATH", ".live_stats.json")
    return AppConfig(
        mode=mode,
        wallet_address=wallet,
        binance=binance,
        polymarket=polymarket,
        strategy=strategy,
        feishu_webhook_url=feishu_webhook_url,
        max_total_exposure_fraction=max_total_exposure_fraction,
        paper_initial_cash=paper_initial_cash,
        paper_max_fraction_per_trade=paper_max_fraction_per_trade,
        paper_max_open_orders=paper_max_open_orders,
        paper_min_notional=paper_min_notional,
        paper_trades_file_path=paper_trades_file_path,
        paper_stats_file_path=paper_stats_file_path,
        live_cash_usd=live_cash_usd,
        live_roll_threshold_usd=live_roll_threshold_usd,
        live_max_notional_below_threshold=live_max_notional_below_threshold,
        live_max_fraction_above_threshold=live_max_fraction_above_threshold,
        live_min_notional=live_min_notional,
        live_balance_refresh_seconds=live_balance_refresh_seconds,
        live_enable_auto_balance=live_enable_auto_balance,
        live_trades_file_path=live_trades_file_path,
        live_stats_file_path=live_stats_file_path,
    )
