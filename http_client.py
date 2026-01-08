import time
import typing
import httpx


class HttpClient:
    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout, connect=timeout, read=timeout, write=timeout, pool=timeout),
            headers={"user-agent": "Mozilla/5.0 (compatible; PolymarketHFTBot/0.1)"},
        )

    def get_json(self, url: str, headers: typing.Optional[dict] = None, retries: int = 2) -> typing.Any:
        last_err: Exception | None = None
        for attempt in range(retries + 1):
            try:
                merged_headers = {}
                if headers:
                    merged_headers.update(headers)
                resp = self._client.get(url, headers=merged_headers)
                if resp.status_code in (404, 422):
                    return None
                if resp.status_code in (403, 429, 500, 502, 503, 504) and attempt < retries:
                    time.sleep(0.25 * (2**attempt))
                    last_err = RuntimeError(f"HTTP {resp.status_code}")
                    continue
                resp.raise_for_status()
                data = resp.content
                if not data:
                    return None
                return resp.json()
            except Exception as e:
                if attempt < retries:
                    time.sleep(0.25 * (2**attempt))
                    last_err = e
                    continue
                raise
        if last_err:
            raise last_err
        return None

