import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class BaseIngester(ABC):
    """Abstract base class for all data source ingesters."""

    max_retries: int = 3
    retry_delay: float = 1.0

    async def fetch_with_retry(self, url: str, **kwargs) -> dict[str, Any]:
        """Fetch a URL with exponential backoff retry."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            for attempt in range(self.max_retries):
                try:
                    response = await client.get(url, **kwargs)
                    response.raise_for_status()
                    return response.json()
                except (httpx.HTTPError, httpx.TimeoutException) as e:
                    if attempt == self.max_retries - 1:
                        raise
                    wait = self.retry_delay * (2**attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}. Retrying in {wait}s")
                    await asyncio.sleep(wait)
        raise RuntimeError("Unreachable")

    @abstractmethod
    async def ingest(self) -> int:
        """Run ingestion. Returns count of records ingested."""
        ...
