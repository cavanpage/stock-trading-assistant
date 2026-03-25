"""
Multi-model analysis: run Claude and Gemini in parallel on the same signal,
then synthesize both perspectives into a unified report.

Use this when you want a second opinion or want to surface disagreements
between models as a risk signal.
"""
import asyncio
import logging

from backend.ai import client as claude
from backend.ai import gemini_client as gemini
from backend.ai.prompts import TRADE_REASONING_SYSTEM, TRADE_REASONING_TEMPLATE
from backend.ml.signal_generator import SignalOutput

logger = logging.getLogger(__name__)

SYNTHESIS_SYSTEM = """You are a senior quantitative analyst reviewing two independent
AI-generated trading signal analyses. Synthesize them into a single concise report.
Note where they agree (increases confidence) and where they diverge (flags risk).
Format: ## Consensus View, ## Divergence / Risk Flags, ## Final Recommendation"""

SYNTHESIS_TEMPLATE = """
**Symbol:** {symbol} | **Signal:** {signal_type} | **Confidence:** {confidence:.0%}

---
### Analysis A (Claude)
{claude_analysis}

---
### Analysis B (Gemini)
{gemini_analysis}

---
Synthesize the above into a unified report.
"""


async def dual_model_reasoning(
    signal: SignalOutput,
    price_summary: str = "",
    news_headlines: str = "",
    social_context: str = "",
    congress_trades: str = "",
) -> dict[str, str]:
    """
    Run trade reasoning through both Claude and Gemini concurrently.

    Returns a dict with keys:
      - "claude": Claude's raw analysis
      - "gemini": Gemini's raw analysis
      - "synthesis": Claude's synthesis of both (uses claude-sonnet for final quality)
    """
    shared_prompt = TRADE_REASONING_TEMPLATE.format(
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

    # Run Claude and Gemini concurrently
    claude_task = claude.complete(
        prompt=shared_prompt,
        system=TRADE_REASONING_SYSTEM,
        model="claude-sonnet-4-6",
        max_tokens=800,
    )
    gemini_task = gemini.complete(
        prompt=shared_prompt,
        system=TRADE_REASONING_SYSTEM,
        model=gemini.GEMINI_FLASH,
        max_tokens=800,
    )

    claude_result, gemini_result = await asyncio.gather(
        claude_task, gemini_task, return_exceptions=True
    )

    claude_text = claude_result if isinstance(claude_result, str) else f"Claude unavailable: {claude_result}"
    gemini_text = gemini_result if isinstance(gemini_result, str) else f"Gemini unavailable: {gemini_result}"

    # Synthesize — always use Claude for final synthesis quality
    synthesis_prompt = SYNTHESIS_TEMPLATE.format(
        symbol=signal.symbol,
        signal_type=signal.signal_type,
        confidence=signal.confidence,
        claude_analysis=claude_text,
        gemini_analysis=gemini_text,
    )

    try:
        synthesis = await claude.complete(
            prompt=synthesis_prompt,
            system=SYNTHESIS_SYSTEM,
            model="claude-sonnet-4-6",
            max_tokens=600,
        )
    except Exception as e:
        logger.error(f"Synthesis failed: {e}")
        synthesis = "Synthesis unavailable — see individual analyses above."

    return {
        "claude": claude_text,
        "gemini": gemini_text,
        "synthesis": synthesis,
    }
