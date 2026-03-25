import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_db
from backend.models.congress_trade import CongressTrade

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/congress/trades")
async def list_congress_trades(
    ticker: Optional[str] = Query(default=None),
    member: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Return recent congressional trades, optionally filtered by ticker or member."""
    query = (
        select(CongressTrade)
        .order_by(CongressTrade.transaction_date.desc())
        .limit(limit)
    )
    if ticker:
        query = query.where(CongressTrade.ticker == ticker.upper())
    if member:
        query = query.where(CongressTrade.member_name.ilike(f"%{member}%"))

    result = await db.execute(query)
    trades = result.scalars().all()
    return [
        {
            "id": str(t.id),
            "member_name": t.member_name,
            "party": t.party,
            "state": t.state,
            "chamber": t.chamber,
            "ticker": t.ticker,
            "asset_type": t.asset_type,
            "transaction_type": t.transaction_type,
            "amount_range": t.amount_range,
            "transaction_date": t.transaction_date.isoformat(),
            "disclosure_date": t.disclosure_date.isoformat(),
            "notes": t.notes,
        }
        for t in trades
    ]


@router.get("/congress/members")
async def list_congress_members(db: AsyncSession = Depends(get_db)):
    """Return distinct member names that have trades in the DB."""
    result = await db.execute(
        select(distinct(CongressTrade.member_name)).order_by(CongressTrade.member_name)
    )
    members = result.scalars().all()
    return {"members": members}
