import json
import time
import urllib.request
from dataclasses import dataclass
from typing import Optional


@dataclass
class FeishuNotifier:
    webhook_url: str
    enabled: bool = True
    timeout_seconds: float = 5.0
    retries: int = 2

    def send_text(self, text: str) -> bool:
        if not self.enabled or not self.webhook_url:
            return False
        payload = {"msg_type": "text", "content": {"text": text}}
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(self.webhook_url, data=body, method="POST")
        req.add_header("content-type", "application/json")
        req.add_header("user-agent", "PolymarketHFTBot/0.1")
        for attempt in range(self.retries + 1):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    _ = resp.read()
                return True
            except Exception as e:
                if attempt < self.retries:
                    time.sleep(0.25 * (2**attempt))
                    continue
                return False
        return False
