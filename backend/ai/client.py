import asyncio
import logging
from functools import lru_cache

import anthropic

from backend.config import settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_anthropic_client() -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=settings.anthropic_api_key)


async def complete(
    prompt: str,
    system: str = "",
    model: str = "claude-haiku-4-5-20251001",
    max_tokens: int = 1024,
    max_retries: int = 3,
) -> str:
    """Call Claude with exponential backoff retry."""
    client = get_anthropic_client()
    messages = [{"role": "user", "content": prompt}]

    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=messages,
            )
            return response.content[0].text
        except anthropic.RateLimitError:
            wait = 2**attempt * 5
            logger.warning(f"Rate limited by Anthropic. Waiting {wait}s (attempt {attempt+1})")
            await asyncio.sleep(wait)
        except anthropic.APIError as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Anthropic API error: {e}. Retrying...")
            await asyncio.sleep(2**attempt)

    raise RuntimeError("Max retries exceeded for Anthropic API call")
