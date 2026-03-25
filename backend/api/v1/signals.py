import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_db
from backend.models.signal import Signal

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/signals")
async def list_signals(
    symbol: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    """Return last 50 signals, optionally filtered by symbol."""
    query = select(Signal).order_by(Signal.generated_at.desc()).limit(50)
    if symbol:
        query = query.where(Signal.symbol == symbol)
    result = await db.execute(query)
    signals = result.scalars().all()
    return [
        {
            "id": str(s.id),
            "symbol": s.symbol,
            "signal_type": s.signal_type,
            "confidence": s.confidence,
            "quant_score": s.quant_score,
            "sentiment_score": s.sentiment_score,
            "congress_modifier": s.congress_modifier,
            "timeframe": s.timeframe,
            "reasoning": s.reasoning,
            "generated_at": s.generated_at.isoformat(),
        }
        for s in signals
    ]


@router.get("/signals/{symbol}/latest")
async def get_latest_signal(
    symbol: str,
    db: AsyncSession = Depends(get_db),
):
    """Return the most recent signal for a symbol."""
    result = await db.execute(
        select(Signal)
        .where(Signal.symbol == symbol)
        .order_by(Signal.generated_at.desc())
        .limit(1)
    )
    signal = result.scalar_one_or_none()
    if signal is None:
        raise HTTPException(status_code=404, detail=f"No signal found for symbol '{symbol}'")
    return {
        "id": str(signal.id),
        "symbol": signal.symbol,
        "signal_type": signal.signal_type,
        "confidence": signal.confidence,
        "quant_score": signal.quant_score,
        "sentiment_score": signal.sentiment_score,
        "congress_modifier": signal.congress_modifier,
        "timeframe": signal.timeframe,
        "reasoning": signal.reasoning,
        "generated_at": signal.generated_at.isoformat(),
    }
