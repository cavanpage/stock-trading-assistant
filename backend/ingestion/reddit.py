import hashlib
import logging
from datetime import datetime, timezone

import praw
from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.social_post import SocialPost

logger = logging.getLogger(__name__)

TARGET_SUBREDDITS = [
    "wallstreetbets", "stocks", "investing", "options",
    "cryptocurrency", "bitcoin", "ethtrader", "CryptoMarkets",
]

FLAIR_BLACKLIST = {"Meme", "Shitpost"}


class RedditIngester(BaseIngester):
    def __init__(self, subreddits: list[str] | None = None, post_limit: int = 100):
        self.subreddits = subreddits or TARGET_SUBREDDITS
        self.post_limit = post_limit
        self._reddit: praw.Reddit | None = None

    def _get_reddit(self) -> praw.Reddit:
        if self._reddit is None:
            self._reddit = praw.Reddit(
                client_id=settings.reddit_client_id,
                client_secret=settings.reddit_client_secret,
                user_agent=settings.reddit_user_agent,
                read_only=True,
            )
        return self._reddit

    async def ingest(self) -> int:
        count = 0
        reddit = self._get_reddit()
        for sub_name in self.subreddits:
            try:
                count += await self._ingest_subreddit(reddit, sub_name)
            except Exception as e:
                logger.error(f"Failed to ingest r/{sub_name}: {e}")
        return count

    async def _ingest_subreddit(self, reddit: praw.Reddit, sub_name: str) -> int:
        subreddit = reddit.subreddit(sub_name)
        records = []

        for post in subreddit.hot(limit=self.post_limit):
            if post.stickied:
                continue
            if hasattr(post, "link_flair_text") and post.link_flair_text in FLAIR_BLACKLIST:
                continue

            external_id = f"reddit_{post.id}"
            body = post.selftext or ""
            records.append({
                "platform": "reddit",
                "external_id": external_id,
                "title": post.title[:500],
                "body": body[:10000],
                "author": str(post.author),
                "url": f"https://reddit.com{post.permalink}",
                "subreddit": sub_name,
                "upvotes": post.score,
                "symbols": [],  # Symbol extraction happens in processing step
                "posted_at": datetime.fromtimestamp(post.created_utc, tz=timezone.utc),
            })

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(SocialPost).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["external_id"])
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} posts from r/{sub_name}")
        return len(records)
