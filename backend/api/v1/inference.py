"""
Internal inference endpoint — called by Flink's InferenceOperator.

This is NOT a public API. It's a lightweight endpoint that keeps the
XGBoost model loaded in memory and responds to Flink in < 50ms.

POST /internal/inference
Body: feature vector from FeatureWindowFunction
Returns: { symbol, signal_type, confidence, composite_score, ... }
"""
import logging
from functools import lru_cache

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.ml.signal_generator import SignalInput, SignalOutput, generate_signal

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/internal", tags=["internal"])


class FeatureVector(BaseModel):
    symbol: str
    timestamp: str | None = None
    trigger_event: str = "price"
    quant_features: dict[str, float]
    avg_sentiment: float = 0.0
    congress_modifier: float = 0.0
    bar_count: int = 0


class SignalResponse(BaseModel):
    symbol: str
    signal_type: str
    confidence: float
    composite_score: float
    quant_score: float
    sentiment_score: float
    congress_modifier: float
    trigger_event: str


@lru_cache(maxsize=1)
def _load_xgboost_model():
    """
    Load the serialised XGBoost model from disk (trained offline, Phase 3).
    Falls back to rule-based scoring if no model file exists yet.
    """
    import os
    model_path = "models/xgboost_signal.json"
    if os.path.exists(model_path):
        import xgboost as xgb
        model = xgb.XGBClassifier()
        model.load_model(model_path)
        logger.info(f"Loaded XGBoost model from {model_path}")
        return model
    logger.warning("No XGBoost model found — using rule-based quant scoring")
    return None


def _rule_based_quant_score(features: dict[str, float]) -> float:
    """
    Heuristic quant score from technical indicators.
    Used before the XGBoost model is trained (Phase 1/2).

    Returns a float in [-1, 1].
    """
    score = 0.0
    votes = 0

    rsi = features.get("rsi14", 50.0)
    if rsi < 30:
        score += 1.0   # oversold → bullish
    elif rsi > 70:
        score -= 1.0   # overbought → bearish
    votes += 1

    # EMA crossover
    ema_cross = features.get("ema9_cross", 0.5)
    score += (ema_cross - 0.5) * 2   # +1 if ema9>ema21, -1 if below
    votes += 1

    # MACD direction
    macd = features.get("macd", 0.0)
    score += max(min(macd / 2.0, 1.0), -1.0)   # normalise loosely
    votes += 1

    # Bollinger position — mean-revert signal
    bb_pos = features.get("bb_position", 0.5)
    if bb_pos < 0.1:
        score += 0.5   # near lower band → potential bounce
    elif bb_pos > 0.9:
        score -= 0.5   # near upper band → potential reversal
    votes += 1

    # Price vs EMA50 — trend confirmation
    pve50 = features.get("price_vs_ema50", 0.0)
    score += max(min(pve50 * 5, 1.0), -1.0)
    votes += 1

    return max(min(score / votes, 1.0), -1.0)


def _xgboost_quant_score(model, features: dict[str, float]) -> float:
    """
    Run XGBoost inference and return a score in [-1, 1].
    Model was trained to output: 0=SELL, 1=HOLD, 2=BUY
    """
    import numpy as np

    FEATURE_ORDER = [
        "close", "ema9", "ema21", "ema50", "ema9_cross",
        "price_vs_ema50", "rsi14", "rsi_oversold", "rsi_overbought",
        "macd", "bb_position", "atr", "vol_ratio",
    ]

    X = np.array([[features.get(f, 0.0) for f in FEATURE_ORDER]])
    probs = model.predict_proba(X)[0]  # [sell_prob, hold_prob, buy_prob]
    # Convert to [-1, 1]: buy_prob - sell_prob
    return float(probs[2] - probs[0])


@router.post("/inference", response_model=SignalResponse)
async def run_inference(fv: FeatureVector) -> SignalResponse:
    if not fv.quant_features:
        raise HTTPException(status_code=400, detail="Empty feature vector")

    model = _load_xgboost_model()

    if model is not None:
        quant_score = _xgboost_quant_score(model, fv.quant_features)
    else:
        quant_score = _rule_based_quant_score(fv.quant_features)

    signal_input = SignalInput(
        symbol=fv.symbol,
        quant_score=quant_score,
        sentiment_score=fv.avg_sentiment,
        congress_modifier=fv.congress_modifier,
    )

    signal: SignalOutput = generate_signal(signal_input)

    return SignalResponse(
        symbol=signal.symbol,
        signal_type=signal.signal_type,
        confidence=signal.confidence,
        composite_score=signal.composite_score,
        quant_score=signal.quant_score,
        sentiment_score=signal.sentiment_score,
        congress_modifier=signal.congress_modifier,
        trigger_event=fv.trigger_event,
    )
