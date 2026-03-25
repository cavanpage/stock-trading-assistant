import logging
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)

BUY_THRESHOLD = 0.3
SELL_THRESHOLD = -0.3


@dataclass
class SignalInput:
    symbol: str
    quant_score: float       # [-1, 1] from ML forecaster or technical rules
    sentiment_score: float   # [-1, 1] from FinBERT aggregation
    congress_modifier: float = 0.0  # [-1, 1] bias from recent congress trades
    vix: float = 20.0        # Current VIX level for regime detection


@dataclass
class SignalOutput:
    symbol: str
    signal_type: str         # "BUY" / "SELL" / "HOLD"
    confidence: float        # [0, 1]
    composite_score: float   # [-1, 1]
    quant_score: float
    sentiment_score: float
    congress_modifier: float


def generate_signal(inp: SignalInput) -> SignalOutput:
    """
    Combine quantitative, sentiment, and congressional signals into a final signal.

    Weights:
      - 50% quantitative (technical + ML forecast)
      - 30% sentiment (FinBERT on news + social)
      - 20% congressional modifier
    High VIX reduces effective score toward HOLD.
    """
    composite = (
        0.5 * inp.quant_score
        + 0.3 * inp.sentiment_score
        + 0.2 * inp.congress_modifier
    )

    # Dampen signal in high-volatility regime
    if inp.vix > 30:
        composite *= 0.6
    elif inp.vix > 25:
        composite *= 0.8

    if composite > BUY_THRESHOLD:
        signal_type = "BUY"
    elif composite < SELL_THRESHOLD:
        signal_type = "SELL"
    else:
        signal_type = "HOLD"

    confidence = min(abs(composite), 1.0)

    return SignalOutput(
        symbol=inp.symbol,
        signal_type=signal_type,
        confidence=confidence,
        composite_score=composite,
        quant_score=inp.quant_score,
        sentiment_score=inp.sentiment_score,
        congress_modifier=inp.congress_modifier,
    )
