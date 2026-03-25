import asyncio
import logging
from functools import lru_cache

import google.generativeai as genai

from backend.config import settings

logger = logging.getLogger(__name__)

# Model tiers — mirrors the Claude tier structure
GEMINI_FLASH = "gemini-2.0-flash"       # Fast, low-cost — high-volume tasks
GEMINI_PRO = "gemini-2.0-flash-thinking-exp"  # Reasoning-heavy tasks


@lru_cache(maxsize=1)
def _configure() -> None:
    genai.configure(api_key=settings.gemini_api_key)


async def complete(
    prompt: str,
    system: str = "",
    model: str = GEMINI_FLASH,
    max_tokens: int = 1024,
    max_retries: int = 3,
) -> str:
    """Call Gemini with exponential backoff retry."""
    _configure()

    full_prompt = f"{system}\n\n{prompt}" if system else prompt

    for attempt in range(max_retries):
        try:
            gemini_model = genai.GenerativeModel(model)
            response = gemini_model.generate_content(
                full_prompt,
                generation_config=genai.GenerationConfig(max_output_tokens=max_tokens),
            )
            return response.text
        except Exception as e:
            error_str = str(e).lower()
            if "quota" in error_str or "rate" in error_str:
                wait = 2**attempt * 5
                logger.warning(f"Gemini rate limited. Waiting {wait}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
            elif attempt == max_retries - 1:
                raise
            else:
                logger.warning(f"Gemini API error: {e}. Retrying...")
                await asyncio.sleep(2**attempt)

    raise RuntimeError("Max retries exceeded for Gemini API call")
