import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_db
from backend.models.asset import Asset
from backend.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/assets")
async def list_assets(db: AsyncSession = Depends(get_db)):
    """List all active assets."""
    result = await db.execute(select(Asset).where(Asset.is_active == True))
    assets = result.scalars().all()
    return [
        {
            "id": str(asset.id),
            "symbol": asset.symbol,
            "name": asset.name,
            "asset_type": asset.asset_type,
        }
        for asset in assets
    ]


@router.get("/assets/{symbol}/ohlcv")
async def get_ohlcv(
    symbol: str,
    limit: int = Query(default=100, ge=1, le=5000),
    source: str = Query(default="yahoo_finance"),
    db: AsyncSession = Depends(get_db),
):
    """Return the last N OHLCV bars for a symbol."""
    # Verify the symbol exists
    asset_result = await db.execute(select(Asset).where(Asset.symbol == symbol))
    asset = asset_result.scalar_one_or_none()
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")

    result = await db.execute(
        select(OHLCV)
        .where(OHLCV.symbol == symbol, OHLCV.source == source)
        .order_by(OHLCV.timestamp.desc())
        .limit(limit)
    )
    bars = result.scalars().all()
    return [
        {
            "timestamp": bar.timestamp.isoformat(),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "vwap": bar.vwap,
            "trade_count": bar.trade_count,
        }
        for bar in reversed(bars)
    ]
