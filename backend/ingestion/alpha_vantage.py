"""
Alpha Vantage historical data ingester.

Why Alpha Vantage for training data:
  - Free tier: 25 years of daily OHLCV (split/dividend adjusted)
  - Free tier: intraday 1-min/5-min/15-min/30-min/60-min (last 30 days)
  - Also provides: earnings calendar, economic indicators (GDP, CPI, Fed funds rate)
  - Rate limit: 25 calls/day free, 75 calls/min on paid ($50/mo)

Strategy: use Alpha Vantage for the LONG history (10-25 years of daily bars)
          and Polygon for high-resolution intraday.
          Together they give you both regime diversity and sample density.
"""
import asyncio
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)

AV_BASE = "https://www.alphavantage.co/query"

# Symbols that benefit most from 20+ year history for regime coverage
LONG_HISTORY_SYMBOLS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "META",
    "NVDA", "TSLA", "SPY", "QQQ", "GLD", "TLT",
]


class AlphaVantageIngester(BaseIngester):
    """
    Fetch full daily history from Alpha Vantage.

    outputsize="full" returns up to 20+ years of daily adjusted closes.
    This is the primary source for long-horizon model training.

    Free tier is rate-limited to 25 requests/day — run this as a one-time
    backfill (scripts/backfill_alpha_vantage.py), not on a frequent schedule.
    """

    def __init__(self, symbols: list[str] | None = None):
        self.symbols = symbols or LONG_HISTORY_SYMBOLS

    async def ingest(self) -> int:
        if not settings.alpha_vantage_api_key:
            logger.warning("ALPHA_VANTAGE_API_KEY not set; skipping Alpha Vantage ingestion")
            return 0
        count = 0
        for symbol in self.symbols:
            try:
                count += await self._ingest_daily(symbol)
                # Free tier: 25 calls/day → space them out if running a batch
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"Alpha Vantage ingestion failed for {symbol}: {e}")
        return count

    async def _ingest_daily(self, symbol: str) -> int:
        """Fetch full daily adjusted OHLCV history."""
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(AV_BASE, params={
                "function": "TIME_SERIES_DAILY_ADJUSTED",
                "symbol": symbol,
                "outputsize": "full",        # 20+ years vs "compact" = 100 days
                "datatype": "json",
                "apikey": settings.alpha_vantage_api_key,
            })
            resp.raise_for_status()
            data = resp.json()

        if "Error Message" in data or "Note" in data:
            # "Note" = rate limit hit, "Error Message" = bad symbol
            msg = data.get("Note") or data.get("Error Message", "unknown")
            logger.warning(f"Alpha Vantage response for {symbol}: {msg}")
            return 0

        series = data.get("Time Series (Daily)", {})
        if not series:
            return 0

        records = []
        for date_str, bar in series.items():
            try:
                ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                records.append({
                    "symbol": symbol,
                    "source": "alpha_vantage_daily",
                    "timestamp": ts,
                    "open": float(bar["1. open"]),
                    "high": float(bar["2. high"]),
                    "low": float(bar["3. low"]),
                    "close": float(bar["5. adjusted close"]),   # split-adjusted
                    "volume": float(bar["6. volume"]),
                })
            except (KeyError, ValueError) as e:
                logger.debug(f"Skipping malformed bar {date_str} for {symbol}: {e}")

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(OHLCV).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "source", "timestamp"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Alpha Vantage: ingested {len(records)} daily bars for {symbol} "
                    f"({records[-1]['timestamp'].year}–{records[0]['timestamp'].year})")
        return len(records)

    async def ingest_macro_indicators(self) -> dict[str, int]:
        """
        Fetch key macro economic indicators useful as model features.
        Stored separately — used to enrich training feature vectors.

        Returns: dict of indicator → count of records stored
        """
        if not settings.alpha_vantage_api_key:
            return {}

        indicators = {
            "FEDERAL_FUNDS_RATE": "fed_funds_rate",
            "CPI": "cpi",
            "INFLATION": "inflation",
            "UNEMPLOYMENT": "unemployment",
            "REAL_GDP": "real_gdp",
        }

        results = {}
        for function, name in indicators.items():
            try:
                count = await self._ingest_indicator(function, name)
                results[name] = count
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"Alpha Vantage macro {function} failed: {e}")
                results[name] = 0

        return results

    async def _ingest_indicator(self, function: str, name: str) -> int:
        """
        Fetch a macro indicator time series.
        Stored in OHLCV table with symbol=indicator name and source=alpha_vantage_macro.
        Close column holds the indicator value; other OHLC columns are null.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(AV_BASE, params={
                "function": function,
                "interval": "monthly",
                "apikey": settings.alpha_vantage_api_key,
            })
            resp.raise_for_status()
            data = resp.json()

        series = data.get("data", [])
        if not series:
            return 0

        records = []
        for point in series:
            try:
                ts = datetime.strptime(point["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                records.append({
                    "symbol": f"MACRO_{name.upper()}",
                    "source": "alpha_vantage_macro",
                    "timestamp": ts,
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": float(point["value"]),
                    "volume": None,
                })
            except (KeyError, ValueError):
                pass

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(OHLCV).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "source", "timestamp"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Alpha Vantage macro: ingested {len(records)} {name} data points")
        return len(records)
