import time
from datetime import datetime, timezone
import os
import concurrent.futures

from http_client import HttpClient
from config import load_config
from polymarket_gamma import PolymarketGammaClient
from binance_client import BinanceClient
from polymarket_clob import PolymarketClobClient
from strategy_updown_lag_arb import UpDownLagArbStrategy
from order_executor import PaperExecutor, LiveClobExecutor, OrderExecutor
from polymarket_data_api import PolymarketDataApiClient
from state_store import JsonStateStore
from order_manager import OrderManager
from feishu_notifier import FeishuNotifier
from paper_ledger import PaperLedger, PaperLedgerPaths
from live_ledger import LiveLedger
import threading
from fee_service import FeeService
import web_server

MARKET_FETCH_TIMEOUT_SECONDS = float(os.getenv("MARKET_FETCH_TIMEOUT_SECONDS", "30"))
SKIP_MARKET_FETCH = os.getenv("SKIP_MARKET_FETCH", "0") in ("1", "true", "True")
WEB_PORT = int(os.getenv("WEB_PORT", "8848"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "5"))


def build_executor(mode: str) -> OrderExecutor:
    if mode == "live":
        return LiveClobExecutor()
    return PaperExecutor()


def fetch_asset_markets(asset, cfg, gamma_client):
    res = []
    try:
        if cfg.polymarket.enable_hourly:
            res.extend(
                gamma_client.discover_updown_markets_hourly(asset, cfg.strategy.hourly_horizon_minutes)
            )
        if cfg.polymarket.enable_15m:
            res.extend(gamma_client.discover_updown_markets_15m(asset, cfg.strategy.m15_horizon_minutes))
    except Exception:
        pass
    return res


def main():
    cfg = load_config()
    http = HttpClient(timeout=HTTP_TIMEOUT_SECONDS)
    gamma_client = PolymarketGammaClient(cfg.polymarket, http)
    binance_client = BinanceClient(cfg.binance, http)
    clob_client = PolymarketClobClient(cfg.polymarket, http)
    strategy = UpDownLagArbStrategy(cfg.strategy, cfg.binance, cfg.polymarket, binance_client, clob_client)
    executor = build_executor(cfg.mode)
    data_api = PolymarketDataApiClient(cfg.polymarket, http)
    fee_service = FeeService(http, binance_client, cfg.strategy)
    store = JsonStateStore(cfg.strategy.state_file_path, cfg.strategy.max_state_age_hours)
    notifier = FeishuNotifier(webhook_url=cfg.feishu_webhook_url, enabled=bool(cfg.feishu_webhook_url))
    ledger = None
    live_ledger = None
    if cfg.mode == "paper":
        ledger = PaperLedger(
            PaperLedgerPaths(trades_path=cfg.paper_trades_file_path, stats_path=cfg.paper_stats_file_path),
            initial_cash=cfg.paper_initial_cash,
        )
    if cfg.mode == "live":
        live_ledger = LiveLedger(trades_path=cfg.live_trades_file_path, stats_path=cfg.live_stats_file_path)
    manager = OrderManager(
        cfg, executor, data_api, binance_client, store, ledger=ledger, live_ledger=live_ledger, notifier=notifier, strategy=strategy, fee_service=fee_service
    )
    manager.load()

    # Start Web Dashboard
    t = threading.Thread(target=web_server.start_server, args=(manager, WEB_PORT), daemon=True)
    t.start()
    print(f"Web Dashboard started at http://localhost:{WEB_PORT}")

    assets = list(cfg.polymarket.series_slugs_hourly.keys())
    run_once = os.getenv("RUN_ONCE", "0") == "1"
    start_ts = datetime.now(timezone.utc).isoformat()
    notifier.send_text(
        "机器人已启动\n"
        + f"时间(UTC)：{start_ts}\n"
        + f"模式：{cfg.mode}\n"
        + f"资产：{','.join(assets)}\n"
        + f"运行方式：{'单轮' if run_once else '循环'}\n"
        + f"轮询间隔(秒)：{cfg.poll_interval_seconds}\n"
        + f"查询地址：{(cfg.wallet_address or '').strip() or '未配置'}"
    )
    while True:
        try:
            all_markets = []

            if SKIP_MARKET_FETCH:
                if run_once:
                    return
                time.sleep(cfg.poll_interval_seconds)
                continue
            
            # 使用多线程并行获取市场数据，避免单线程阻塞
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(assets) + 1, 8))
            try:
                futures = [executor.submit(fetch_asset_markets, asset, cfg, gamma_client) for asset in assets]
                done, not_done = concurrent.futures.wait(futures, timeout=MARKET_FETCH_TIMEOUT_SECONDS)
                for future in done:
                    try:
                        markets = future.result()
                        if markets:
                            all_markets.extend(markets)
                    except Exception:
                        pass
                for future in not_done:
                    try:
                        future.cancel()
                    except Exception:
                        pass
            finally:
                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    executor.shutdown(wait=False)

            if cfg.mode == "paper":
                current_balance = manager._paper_cash()
            else:
                current_balance = manager.live_cash_usd
            
            orders = strategy.generate_orders(all_markets, current_balance)
            for order in orders:
                manager.submit_with_risk(order)
            manager.poll_status()
            if run_once:
                return
            time.sleep(cfg.poll_interval_seconds)
        except Exception as e:
            err_ts = datetime.now(timezone.utc).isoformat()
            notifier.send_text(
                "主循环异常\n"
                + f"时间(UTC)：{err_ts}\n"
                + f"模式：{cfg.mode}\n"
                + f"错误：{e}"
            )
            if run_once:
                raise
            time.sleep(1.0)


if __name__ == "__main__":
    main()
