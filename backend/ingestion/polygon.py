"""
Polygon.io historical data ingester.

Why Polygon for training data:
  - Free tier: 2 years of 1-minute aggregate bars (unlimited symbols)
  - Paid ($29/mo): full tick history back to 2003 for US equities
  - Clean, split/dividend adjusted data — critical for accurate backtesting
  - Intraday resolution means ~195k bars/symbol/2y vs ~500 for daily

Use cases:
  - Backfilling 1-min bars for model training (vastly more samples than daily)
  - Fetching adjusted close prices (avoids split artifacts in price-based features)
  - Options data (future: unusual options flow as a signal)
"""
import asyncio
import logging
from datetime import date, timedelta

import httpx
from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)

POLYGON_BASE = "https://api.polygon.io/v2"

DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "GOOGL", "META", "SPY", "QQQ", "GLD",
]


class PolygonIngester(BaseIngester):
    """
    Fetch aggregate bars from Polygon.io.

    timespan: "minute" | "hour" | "day" | "week"
    multiplier: e.g. multiplier=5, timespan="minute" → 5-min bars

    Free tier allows up to 5 API calls/min but no rate on bar data itself.
    We add a small sleep between symbols to stay well within limits.
    """

    def __init__(
        self,
        symbols: list[str] | None = None,
        timespan: str = "day",
        multiplier: int = 1,
        from_date: str | None = None,
        to_date: str | None = None,
    ):
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.timespan = timespan
        self.multiplier = multiplier
        self.from_date = from_date or (date.today() - timedelta(days=730)).isoformat()
        self.to_date = to_date or date.today().isoformat()

    async def ingest(self) -> int:
        if not settings.polygon_api_key:
            logger.warning("POLYGON_API_KEY not set; skipping Polygon ingestion")
            return 0
        count = 0
        for symbol in self.symbols:
            try:
                count += await self._ingest_symbol(symbol)
                await asyncio.sleep(0.3)   # 5 req/min free tier buffer
            except Exception as e:
                logger.error(f"Polygon ingestion failed for {symbol}: {e}")
        return count

    async def _ingest_symbol(self, symbol: str) -> int:
        url = (
            f"{POLYGON_BASE}/aggs/ticker/{symbol}/range"
            f"/{self.multiplier}/{self.timespan}"
            f"/{self.from_date}/{self.to_date}"
        )
        params = {
            "adjusted": "true",   # split + dividend adjusted
            "sort": "asc",
            "limit": 50000,
            "apiKey": settings.polygon_api_key,
        }

        all_results = []
        next_url: str | None = url

        # Polygon paginates via next_url cursor
        async with httpx.AsyncClient(timeout=30.0) as client:
            while next_url:
                if next_url == url:
                    resp = await client.get(next_url, params=params)
                else:
                    resp = await client.get(next_url, params={"apiKey": settings.polygon_api_key})
                resp.raise_for_status()
                data = resp.json()

                results = data.get("results", [])
                all_results.extend(results)

                next_url = data.get("next_url")   # None when last page

        if not all_results:
            logger.debug(f"No Polygon data for {symbol}")
            return 0

        source = f"polygon_{self.multiplier}{self.timespan}"
        records = [
            {
                "symbol": symbol,
                "source": source,
                "timestamp": _ms_to_utc(bar["t"]),
                "open": bar["o"],
                "high": bar["h"],
                "low": bar["l"],
                "close": bar["c"],
                "volume": bar["v"],
                "vwap": bar.get("vw"),
                "trade_count": bar.get("n"),
            }
            for bar in all_results
        ]

        async with AsyncSessionLocal() as session:
            stmt = insert(OHLCV).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "source", "timestamp"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Polygon: ingested {len(records)} {self.multiplier}{self.timespan} bars for {symbol}")
        return len(records)


def _ms_to_utc(ts_ms: int):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
