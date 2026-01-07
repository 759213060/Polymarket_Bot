import json
import ssl
import time
import typing
import urllib.error
import urllib.request


class HttpClient:
    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        self.context = ssl.create_default_context()

    def get_json(self, url: str, headers: typing.Optional[dict] = None, retries: int = 2) -> typing.Any:
        req = urllib.request.Request(url)
        req.add_header("user-agent", "Mozilla/5.0 (compatible; PolymarketHFTBot/0.1)")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        last_err: Exception | None = None
        for attempt in range(retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout, context=self.context) as resp:
                    data = resp.read()
                if not data:
                    return None
                return json.loads(data.decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code in (404, 422):
                    return None
                if e.code in (403, 429, 500, 502, 503, 504) and attempt < retries:
                    time.sleep(0.25 * (2**attempt))
                    last_err = e
                    continue
                raise
            except Exception as e:
                if attempt < retries:
                    time.sleep(0.25 * (2**attempt))
                    last_err = e
                    continue
                raise
        if last_err:
            raise last_err
        return None

