import os
import requests
from typing import Optional
from dotenv import load_dotenv

load_dotenv(override=True)

# api.elections.kalshi.com hosts ALL Kalshi markets (sports + elections).
# api.kalshi.com is the newer sports-only platform but may not be available everywhere.
_BASE_URLS = [
    "https://api.elections.kalshi.com/trade-api/v2",
    "https://api.kalshi.com/trade-api/v2",
]

# Confirmed series ticker for MLB game-outcome markets.
# Format: KXMLBGAME-{YY}{MON}{DD}{TIME}{AWAY}{HOME}
_MLB_SERIES = ("KXMLBGAME", "KXMLBSG", "KXMLB", "MLB")


class KalshiClient:
    """
    Thin wrapper around the Kalshi REST API v2.

    Order book format (as of 2026):
      GET /markets/{ticker}/orderbook → {"orderbook_fp": {"yes_dollars": [...], "no_dollars": [...]}}
      Each level: ["price_decimal", "dollar_amount"]   e.g. ["0.5700", "130371.44"]
      Prices are decimal fractions (0.57 = 57¢).  Levels sorted ascending.

    Trades endpoint returns 404 for sports markets — use market meta instead.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("KALSHI_API_KEY", "")
        self._s = requests.Session()
        self._s.headers.update({"Accept": "application/json"})
        if self.api_key:
            self._s.headers.update({"Authorization": f"Bearer {self.api_key}"})
        self.base_url: str = ""
        self._tried_urls: list[str] = []

    # ── internal ──────────────────────────────────────────────────────────────

    def _resolve_base(self) -> str:
        if self.base_url:
            return self.base_url
        self._tried_urls = []
        for url in _BASE_URLS:
            self._tried_urls.append(url)
            try:
                r = self._s.get(f"{url}/events", params={"limit": 1}, timeout=10)
                # Accept any response that isn't a connection error AND isn't the
                # old "API has moved" redirect body from trading-api.kalshi.com.
                if r.status_code in (200, 401, 403) and "moved" not in r.text[:80].lower():
                    self.base_url = url
                    return url
            except requests.RequestException:
                continue
        raise ConnectionError(
            f"Could not reach any Kalshi API endpoint. Tried: {_BASE_URLS}"
        )

    def _get(self, path: str, params: dict = None) -> dict:
        base = self._resolve_base()
        resp = self._s.get(f"{base}{path}", params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ── public helpers ────────────────────────────────────────────────────────

    def get_mlb_events(self) -> list[dict]:
        """
        Return open MLB game-outcome events.
        Tries known series tickers, then falls back to keyword search.
        """
        for series in _MLB_SERIES:
            try:
                data = self._get("/events", params={
                    "series_ticker": series,
                    "status": "open",
                    "limit": 200,
                })
                events = data.get("events", [])
                if events:
                    return events
            except (requests.HTTPError, requests.RequestException):
                continue

        # Broad keyword fallback
        try:
            data = self._get("/events", params={"status": "open", "limit": 200})
            keywords = ("mlb", "baseball", " vs ", " @ ")
            return [
                e for e in data.get("events", [])
                if any(
                    kw in (e.get("title") or "").lower()
                    or kw in (e.get("event_ticker") or "").lower()
                    or kw in (e.get("series_ticker") or "").lower()
                    for kw in keywords
                )
            ]
        except (requests.HTTPError, requests.RequestException):
            return []

    def get_markets_for_event(self, event_ticker: str) -> list[dict]:
        data = self._get("/markets", params={
            "event_ticker": event_ticker,
            "status": "open",
            "limit": 100,
        })
        return data.get("markets", [])

    def get_orderbook(self, market_ticker: str, depth: int = 20) -> dict:
        """
        Returns the raw orderbook_fp dict:
          {
            "yes_dollars": [["0.5700", "130371.44"], ...],   sorted ascending
            "no_dollars":  [["0.4200", "1062411.13"], ...],  sorted ascending
          }
        """
        data = self._get(
            f"/markets/{market_ticker}/orderbook", params={"depth": depth}
        )
        ob = data.get("orderbook_fp") or data.get("orderbook") or {}
        return ob

    def get_market(self, market_ticker: str) -> dict:
        data = self._get(f"/markets/{market_ticker}")
        return data.get("market", {})

    def get_trades(self, market_ticker: str, limit: int = 100) -> list[dict]:
        """Trades endpoint returns 404 for sports markets; returns [] on failure."""
        try:
            data = self._get(
                f"/markets/{market_ticker}/trades", params={"limit": limit}
            )
            return data.get("trades", [])
        except (requests.HTTPError, requests.RequestException):
            return []
