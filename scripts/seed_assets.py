#!/usr/bin/env python3
"""Seed the assets table with the default watchlist."""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.db.session import AsyncSessionLocal
from backend.models.asset import Asset
from sqlalchemy.dialects.postgresql import insert


ASSETS = [
    {"symbol": "AAPL", "name": "Apple Inc.", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "MSFT", "name": "Microsoft Corporation", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "NVDA", "name": "NVIDIA Corporation", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "TSLA", "name": "Tesla Inc.", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "GOOGL", "name": "Alphabet Inc.", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "META", "name": "Meta Platforms Inc.", "asset_type": "stock", "exchange": "NASDAQ"},
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF", "asset_type": "etf", "exchange": "NYSE"},
    {"symbol": "QQQ", "name": "Invesco QQQ ETF", "asset_type": "etf", "exchange": "NASDAQ"},
    {"symbol": "BTC-USD", "name": "Bitcoin", "asset_type": "crypto", "exchange": "Binance"},
    {"symbol": "ETH-USD", "name": "Ethereum", "asset_type": "crypto", "exchange": "Binance"},
    {"symbol": "SOL-USD", "name": "Solana", "asset_type": "crypto", "exchange": "Binance"},
    {"symbol": "BNB-USD", "name": "BNB", "asset_type": "crypto", "exchange": "Binance"},
    {"symbol": "XRP-USD", "name": "XRP", "asset_type": "crypto", "exchange": "Binance"},
]


async def main():
    async with AsyncSessionLocal() as session:
        stmt = insert(Asset).values(ASSETS)
        stmt = stmt.on_conflict_do_nothing(index_elements=["symbol"])
        await session.execute(stmt)
        await session.commit()
    print(f"Seeded {len(ASSETS)} assets.")


if __name__ == "__main__":
    asyncio.run(main())
