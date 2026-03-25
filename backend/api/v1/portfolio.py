import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.session import get_db
from backend.models.portfolio import Portfolio, Position

logger = logging.getLogger(__name__)
router = APIRouter()


class PortfolioCreate(BaseModel):
    name: str
    description: Optional[str] = None
    cash_balance: float = 0.0
    is_paper: bool = True


class PositionCreate(BaseModel):
    symbol: str
    quantity: float
    avg_cost: float
    asset_type: str


@router.get("/portfolios")
async def list_portfolios(db: AsyncSession = Depends(get_db)):
    """List all portfolios."""
    result = await db.execute(select(Portfolio).order_by(Portfolio.created_at.desc()))
    portfolios = result.scalars().all()
    return [
        {
            "id": str(p.id),
            "name": p.name,
            "description": p.description,
            "cash_balance": p.cash_balance,
            "is_paper": p.is_paper,
            "created_at": p.created_at.isoformat(),
        }
        for p in portfolios
    ]


@router.post("/portfolios", status_code=201)
async def create_portfolio(body: PortfolioCreate, db: AsyncSession = Depends(get_db)):
    """Create a new portfolio."""
    portfolio = Portfolio(
        name=body.name,
        description=body.description,
        cash_balance=body.cash_balance,
        is_paper=body.is_paper,
    )
    db.add(portfolio)
    await db.commit()
    await db.refresh(portfolio)
    return {
        "id": str(portfolio.id),
        "name": portfolio.name,
        "description": portfolio.description,
        "cash_balance": portfolio.cash_balance,
        "is_paper": portfolio.is_paper,
        "created_at": portfolio.created_at.isoformat(),
    }


@router.get("/portfolios/{portfolio_id}/positions")
async def list_positions(portfolio_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """List positions for a portfolio."""
    portfolio_result = await db.execute(
        select(Portfolio).where(Portfolio.id == portfolio_id)
    )
    portfolio = portfolio_result.scalar_one_or_none()
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    result = await db.execute(
        select(Position).where(Position.portfolio_id == portfolio_id)
    )
    positions = result.scalars().all()
    return [
        {
            "id": str(pos.id),
            "portfolio_id": str(pos.portfolio_id),
            "symbol": pos.symbol,
            "quantity": pos.quantity,
            "avg_cost": pos.avg_cost,
            "asset_type": pos.asset_type,
            "opened_at": pos.opened_at.isoformat(),
        }
        for pos in positions
    ]


@router.post("/portfolios/{portfolio_id}/positions", status_code=201)
async def add_position(
    portfolio_id: uuid.UUID,
    body: PositionCreate,
    db: AsyncSession = Depends(get_db),
):
    """Add a position to a portfolio."""
    portfolio_result = await db.execute(
        select(Portfolio).where(Portfolio.id == portfolio_id)
    )
    portfolio = portfolio_result.scalar_one_or_none()
    if portfolio is None:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    position = Position(
        portfolio_id=portfolio_id,
        symbol=body.symbol.upper(),
        quantity=body.quantity,
        avg_cost=body.avg_cost,
        asset_type=body.asset_type,
    )
    db.add(position)
    await db.commit()
    await db.refresh(position)
    return {
        "id": str(position.id),
        "portfolio_id": str(position.portfolio_id),
        "symbol": position.symbol,
        "quantity": position.quantity,
        "avg_cost": position.avg_cost,
        "asset_type": position.asset_type,
        "opened_at": position.opened_at.isoformat(),
    }


@router.delete("/portfolios/{portfolio_id}/positions/{position_id}", status_code=204)
async def delete_position(
    portfolio_id: uuid.UUID,
    position_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Remove a position from a portfolio."""
    result = await db.execute(
        select(Position).where(
            Position.id == position_id,
            Position.portfolio_id == portfolio_id,
        )
    )
    position = result.scalar_one_or_none()
    if position is None:
        raise HTTPException(status_code=404, detail="Position not found")

    await db.delete(position)
    await db.commit()
