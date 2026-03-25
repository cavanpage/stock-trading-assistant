import logging
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.news import NewsArticle
from backend.processing.deduplicator import content_hash

logger = logging.getLogger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Symbols to pull company-specific news and sentiment for
DEFAULT_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META",
    "SPY", "QQQ", "BINANCE:BTCUSDT", "BINANCE:ETHUSDT",
]


class FinnhubIngester(BaseIngester):
    """
    Ingest from Finnhub:
      - Company news per symbol (with pre-computed sentiment)
      - General market news
      - Earnings calendar (stored as special news entries)
    """

    def __init__(self, symbols: list[str] | None = None):
        self.symbols = symbols or DEFAULT_SYMBOLS

    def _headers(self) -> dict:
        return {"X-Finnhub-Token": settings.finnhub_api_key}

    async def ingest(self) -> int:
        if not settings.finnhub_api_key:
            logger.warning("FINNHUB_API_KEY not set; skipping Finnhub ingestion")
            return 0
        count = 0
        count += await self._ingest_market_news()
        for symbol in self.symbols:
            # Skip Binance crypto symbols for news — Finnhub uses plain tickers
            if ":" in symbol:
                continue
            try:
                count += await self._ingest_company_news(symbol)
            except Exception as e:
                logger.error(f"Finnhub news failed for {symbol}: {e}")
        count += await self._ingest_earnings_calendar()
        return count

    async def _ingest_market_news(self) -> int:
        url = f"{FINNHUB_BASE}/news"
        data = await self.fetch_with_retry(
            url,
            params={"category": "general"},
            headers=self._headers(),
        )
        return await self._store_news_items(data, symbols=[])

    async def _ingest_company_news(self, symbol: str) -> int:
        from datetime import date, timedelta

        today = date.today()
        from_date = (today - timedelta(days=7)).isoformat()
        to_date = today.isoformat()

        url = f"{FINNHUB_BASE}/company-news"
        data = await self.fetch_with_retry(
            url,
            params={"symbol": symbol, "from": from_date, "to": to_date},
            headers=self._headers(),
        )
        return await self._store_news_items(data, symbols=[symbol])

    async def _ingest_earnings_calendar(self) -> int:
        """Fetch upcoming earnings and store as high-priority news entries."""
        from datetime import date, timedelta

        today = date.today()
        to_date = (today + timedelta(days=14)).isoformat()

        url = f"{FINNHUB_BASE}/calendar/earnings"
        data = await self.fetch_with_retry(
            url,
            params={"from": today.isoformat(), "to": to_date},
            headers=self._headers(),
        )

        earnings_list = data.get("earningsCalendar", [])
        if not earnings_list:
            return 0

        records = []
        for entry in earnings_list:
            symbol = entry.get("symbol", "")
            date_str = entry.get("date", "")
            eps_est = entry.get("epsEstimate")
            rev_est = entry.get("revenueEstimate")

            title = f"Earnings Report: {symbol} on {date_str}"
            content = (
                f"{symbol} is expected to report earnings on {date_str}. "
                f"EPS estimate: {eps_est}. Revenue estimate: {rev_est}."
            )
            url_str = f"https://finnhub.io/earnings/{symbol}/{date_str}"

            records.append({
                "title": title,
                "url": url_str,
                "source": "finnhub_earnings",
                "published_at": datetime.now(timezone.utc),
                "content_hash": content_hash(title),
                "symbols": [symbol] if symbol else [],
                "raw_text": content,
            })

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(NewsArticle).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} earnings calendar entries")
        return len(records)

    async def _store_news_items(self, items: list, symbols: list[str]) -> int:
        if not items:
            return 0

        records = []
        for item in items:
            url = item.get("url", "")
            headline = item.get("headline") or item.get("summary") or ""
            summary = item.get("summary") or ""
            if not url or not headline:
                continue

            ts = item.get("datetime")
            if ts:
                published_at = datetime.fromtimestamp(ts, tz=timezone.utc)
            else:
                published_at = datetime.now(timezone.utc)

            # Finnhub provides a pre-computed sentiment score on some endpoints
            sentiment = item.get("sentiment")
            sentiment_score: float | None = None
            if isinstance(sentiment, dict):
                # bullishPercent / bearishPercent form
                bullish = sentiment.get("bullishPercent", 0)
                bearish = sentiment.get("bearishPercent", 0)
                sentiment_score = float(bullish - bearish)
            elif isinstance(sentiment, (int, float)):
                sentiment_score = float(sentiment)

            item_symbols = list(symbols)
            related = item.get("related", "")
            if related:
                item_symbols = list(set(item_symbols + [s.strip() for s in related.split(",") if s.strip()]))

            records.append({
                "title": headline[:500],
                "url": url,
                "source": f"finnhub_{item.get('source', 'news')}",
                "published_at": published_at,
                "content_hash": content_hash(headline + summary),
                "symbols": item_symbols,
                "sentiment_score": sentiment_score,
                "raw_text": (headline + "\n\n" + summary)[:20000],
            })

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(NewsArticle).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} Finnhub news items")
        return len(records)
