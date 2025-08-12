from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential


def load_api_key(env_var_name: str = "N2YO_API_KEY") -> str:
    """Load the N2YO API key from environment variables (supports .env)."""
    # Load from .env if present
    load_dotenv()
    api_key = os.getenv(env_var_name)
    if not api_key:
        raise RuntimeError(
            f"Missing API key: set {env_var_name} in your environment or config/.env"
        )
    return api_key


class N2YOClient:
    """Thin client for N2YO REST API with on-disk caching and retry/backoff."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.n2yo.com/rest/v1/satellite",
        cache_dir: Path | str = "cache_tle",
        session: Optional[requests.Session] = None,
        polite_delay_seconds: float = 0.25,
    ) -> None:
        self.api_key = api_key or load_api_key()
        self.base_url = base_url.rstrip("/")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = session or requests.Session()
        self.polite_delay_seconds = polite_delay_seconds

    @retry(wait=wait_exponential(multiplier=1, min=1, max=60), stop=stop_after_attempt(5))
    def get_tle(self, norad_id: int) -> Dict[str, Any]:
        """Fetch TLE payload for a NORAD ID with caching.

        Cache key: cache_dir/{norad_id}.json
        """
        cache_path = self.cache_dir / f"{int(norad_id)}.json"
        if cache_path.exists():
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                # Fall through to re-fetch if cache is corrupt
                pass

        url = f"{self.base_url}/tle/{int(norad_id)}&apiKey={self.api_key}"
        resp = self.session.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        # Be polite vs per-verb rate limits
        time.sleep(self.polite_delay_seconds)

        cache_path.write_text(json.dumps(data))
        return data


__all__ = ["N2YOClient", "load_api_key"]


