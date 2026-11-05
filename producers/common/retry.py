import time

import httpx

from producers.common.log import get_logger

log = get_logger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def get_json(
    client: httpx.Client,
    url: str,
    params: dict,
    attempts: int = 5,
    base_delay: float = 1.0,
) -> dict:
    # Errors mention only the URL, never params, so API keys stay out of logs.
    for attempt in range(1, attempts + 1):
        try:
            resp = client.get(url, params=params)
            if resp.status_code not in RETRYABLE_STATUS:
                if resp.is_error:
                    raise RuntimeError(f"GET {url} returned HTTP {resp.status_code}: {resp.text[:200]}")
                return resp.json()
            reason = f"HTTP {resp.status_code}"
        except httpx.TransportError as e:
            reason = type(e).__name__
        if attempt == attempts:
            raise RuntimeError(f"GET {url} failed after {attempts} attempts: {reason}")
        delay = base_delay * 2 ** (attempt - 1)
        log.warning("GET %s failed (%s), retry %d/%d in %.0fs", url, reason, attempt, attempts - 1, delay)
        time.sleep(delay)
