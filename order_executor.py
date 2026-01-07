from dataclasses import dataclass
from typing import Optional
from datetime import datetime
import os
import importlib


@dataclass
class OrderRequest:
    market_slug: str
    asset_symbol: str
    outcome: str
    token_id: str
    price: float
    size: float
    notional: float
    start_time: datetime
    end_time: datetime
    order_type: str = "FOK"


@dataclass
class OrderResult:
    success: bool
    message: str
    request: OrderRequest
    order_id: Optional[str] = None


class OrderExecutor:
    def submit(self, order: OrderRequest) -> OrderResult:
        raise NotImplementedError


class PaperExecutor(OrderExecutor):
    def submit(self, order: OrderRequest) -> OrderResult:
        msg = (
            "PAPER ORDER "
            + order.asset_symbol
            + " "
            + order.outcome
            + " size="
            + str(round(order.size, 4))
            + " price="
            + str(round(order.price, 4))
            + " notional="
            + str(round(order.notional, 2))
            + " market="
            + order.market_slug
        )
        print(msg)
        return OrderResult(success=True, message=msg, request=order, order_id=None)


class LiveClobExecutor(OrderExecutor):
    def __init__(self):
        self._client = None

    def _get_client(self):
        if self._client is not None:
            return self._client
        client_mod = importlib.import_module("py_clob_client.client")
        clob_types_mod = importlib.import_module("py_clob_client.clob_types")
        ClobClient = getattr(client_mod, "ClobClient")
        ApiCreds = getattr(clob_types_mod, "ApiCreds")

        host = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        funder = os.getenv("POLYMARKET_FUNDER_ADDRESS", "")
        signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
        if not private_key:
            raise RuntimeError("Missing POLYMARKET_PRIVATE_KEY")
        client = ClobClient(
            host,
            key=private_key,
            chain_id=chain_id,
            signature_type=signature_type,
            funder=funder or None,
        )
        api_key = os.getenv("POLYMARKET_API_KEY", "")
        api_secret = os.getenv("POLYMARKET_API_SECRET", "")
        api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "")
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        else:
            creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        self._client = client
        return client

    def submit(self, order: OrderRequest) -> OrderResult:
        try:
            client = self._get_client()
            clob_types_mod = importlib.import_module("py_clob_client.clob_types")
            constants_mod = importlib.import_module("py_clob_client.order_builder.constants")
            OrderArgs = getattr(clob_types_mod, "OrderArgs")
            OrderType = getattr(clob_types_mod, "OrderType")
            BUY = getattr(constants_mod, "BUY")

            args = OrderArgs(
                token_id=str(order.token_id),
                price=float(order.price),
                size=float(order.size),
                side=BUY,
            )
            signed = client.create_order(args)
            ot = OrderType.FOK if (order.order_type or "").upper() == "FOK" else OrderType.GTC
            resp = client.post_order(signed, ot)
            ok = bool(getattr(resp, "success", False) or resp.get("success"))
            order_id = getattr(resp, "orderID", None) or getattr(resp, "orderId", None) or resp.get("orderID") or resp.get("orderId")
            msg = getattr(resp, "errorMsg", None) or resp.get("errorMsg") or ""
            if ok:
                return OrderResult(success=True, message="ok", request=order, order_id=order_id)
            return OrderResult(success=False, message=msg or "failed", request=order, order_id=order_id)
        except Exception as e:
            return OrderResult(success=False, message=str(e), request=order, order_id=None)
