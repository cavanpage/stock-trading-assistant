import logging
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.news import NewsArticle
from backend.processing.deduplicator import content_hash

logger = logging.getLogger(__name__)

NEWSDATA_BASE = "https://newsdata.io/api/1"

# Broad financial categories supported by NewsData.io
FINANCE_CATEGORIES = ["business", "technology"]

# Country codes for broad global coverage
COUNTRIES = "us,gb,ca,au,sg,in"


class NewsDataIngester(BaseIngester):
    """
    Ingest financial news from NewsData.io.
    Aggregates 90,000+ global sources — useful for international market context
    and broader coverage than NewsAPI alone.
    """

    def __init__(self, queries: list[str] | None = None):
        # Both keyword queries and category-based fetches
        self.queries = queries or [
            "stock market", "cryptocurrency Bitcoin", "Federal Reserve interest rates",
            "earnings report", "IPO", "SEC regulation", "merger acquisition",
        ]

    async def ingest(self) -> int:
        if not settings.newsdata_api_key:
            logger.warning("NEWSDATA_API_KEY not set; skipping NewsData ingestion")
            return 0
        count = 0
        for query in self.queries:
            try:
                count += await self._ingest_query(query)
            except Exception as e:
                logger.error(f"NewsData ingestion failed for query '{query}': {e}")
        return count

    async def _ingest_query(self, query: str) -> int:
        url = f"{NEWSDATA_BASE}/news"
        data = await self.fetch_with_retry(
            url,
            params={
                "apikey": settings.newsdata_api_key,
                "q": query,
                "language": "en",
                "category": ",".join(FINANCE_CATEGORIES),
                "country": COUNTRIES,
            },
        )

        articles = data.get("results", [])
        if not articles:
            return 0

        records = []
        for article in articles:
            url_str = article.get("link", "")
            title = article.get("title") or ""
            description = article.get("description") or ""
            content = article.get("content") or description
            if not url_str or not title:
                continue

            pub_str = article.get("pubDate", "")
            try:
                published_at = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                published_at = datetime.now(timezone.utc)

            # NewsData returns keywords which we can use as symbol hints
            keywords = article.get("keywords") or []
            symbols: list[str] = []
            if isinstance(keywords, list):
                # Keep anything that looks like a ticker (short, uppercase)
                symbols = [k.upper() for k in keywords if k and k.isupper() and 1 <= len(k) <= 5]

            source_name = article.get("source_id") or article.get("source_name") or ""

            records.append({
                "title": title[:500],
                "url": url_str,
                "source": f"newsdata_{source_name}"[:100],
                "published_at": published_at,
                "content_hash": content_hash(title + content),
                "symbols": symbols,
                "raw_text": content[:20000],
            })

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(NewsArticle).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} NewsData articles for query '{query}'")
        return len(records)
