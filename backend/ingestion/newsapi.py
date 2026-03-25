import logging
from datetime import datetime, timezone

from newsapi import NewsApiClient
from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.news import NewsArticle
from backend.processing.deduplicator import content_hash

logger = logging.getLogger(__name__)

FINANCE_QUERIES = [
    "stock market", "cryptocurrency", "Federal Reserve", "earnings",
    "IPO", "merger acquisition", "SEC", "trading", "Wall Street",
]


class NewsAPIIngester(BaseIngester):
    def __init__(self, queries: list[str] | None = None):
        self.queries = queries or FINANCE_QUERIES
        self._client: NewsApiClient | None = None

    def _get_client(self) -> NewsApiClient:
        if self._client is None:
            self._client = NewsApiClient(api_key=settings.newsapi_key)
        return self._client

    async def ingest(self) -> int:
        if not settings.newsapi_key:
            logger.warning("NEWSAPI_KEY not set; skipping NewsAPI ingestion")
            return 0
        count = 0
        client = self._get_client()
        for query in self.queries:
            try:
                count += await self._ingest_query(client, query)
            except Exception as e:
                logger.error(f"NewsAPI ingestion failed for query '{query}': {e}")
        return count

    async def _ingest_query(self, client: NewsApiClient, query: str) -> int:
        response = client.get_everything(
            q=query, language="en", sort_by="publishedAt", page_size=20
        )
        articles = response.get("articles", [])

        records = []
        for article in articles:
            url = article.get("url", "")
            title = article.get("title") or ""
            content = article.get("content") or article.get("description") or ""
            if not url or not title:
                continue
            published_str = article.get("publishedAt", "")
            try:
                published_at = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                published_at = datetime.now(timezone.utc)

            records.append({
                "title": title[:500],
                "url": url,
                "source": article.get("source", {}).get("name", ""),
                "published_at": published_at,
                "content_hash": content_hash(title + content),
                "symbols": [],
                "raw_text": content[:20000],
            })

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(NewsArticle).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} articles for query '{query}'")
        return len(records)
