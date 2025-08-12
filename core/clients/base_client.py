import time
import logging
from typing import Any, Dict, Optional
import requests
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

class OpenAPIClient:
    def __init__(self, base_url: str, service_key: str, default_type: str = "json"):
        self.base_url = base_url.rstrip("/")
        self.service_key = service_key
        self.default_type = default_type
        self.session = requests.Session()

    def get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        q = {"ServiceKey": self.service_key, "type": self.default_type}
        q.update({k: v for k, v in params.items() if v is not None})
        url = f"{self.base_url}/{path}?{urlencode(q)}"
        for attempt in range(5):
            try:
                r = self.session.get(url, timeout=15)
                r.raise_for_status()
                return r.json() if self.default_type == "json" else r.text
            except Exception as e:
                wait = 2 ** attempt
                logger.warning("API GET failed(%s). retry in %ss: %s", path, wait, e)
                time.sleep(wait)
        raise RuntimeError(f"API GET failed after retries: {path}")