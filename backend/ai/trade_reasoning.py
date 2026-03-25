import logging
from dataclasses import asdict

from backend.ai.client import complete
from backend.ai.prompts import TRADE_REASONING_SYSTEM, TRADE_REASONING_TEMPLATE
from backend.ml.signal_generator import SignalOutput

logger = logging.getLogger(__name__)


async def generate_reasoning(
    signal: SignalOutput,
    price_summary: str = "",
    news_headlines: str = "",
    social_context: str = "",
    congress_trades: str = "",
) -> str:
    """Generate a plain-English explanation for a trading signal using Claude."""
    prompt = TRADE_REASONING_TEMPLATE.format(
        symbol=signal.symbol,
        signal_type=signal.signal_type,
        confidence=signal.confidence,
        composite_score=signal.composite_score,
        quant_score=signal.quant_score,
        sentiment_score=signal.sentiment_score,
        congress_modifier=signal.congress_modifier,
        price_summary=price_summary or "No recent price data available.",
        news_headlines=news_headlines or "No recent news found.",
        social_context=social_context or "No recent social posts found.",
        congress_trades=congress_trades or "No congressional trades in the past 30 days.",
    )

    try:
        return await complete(
            prompt=prompt,
            system=TRADE_REASONING_SYSTEM,
            model="claude-sonnet-4-6",
            max_tokens=800,
        )
    except Exception as e:
        logger.error(f"Failed to generate reasoning for {signal.symbol}: {e}")
        return f"Signal: {signal.signal_type} | Score: {signal.composite_score:.3f} (reasoning unavailable)"
