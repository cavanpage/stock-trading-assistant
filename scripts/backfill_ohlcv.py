#!/usr/bin/env python3
"""Backfill 2 years of historical OHLCV data for the default watchlist."""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.ingestion.yahoo_finance import YahooFinanceIngester
from backend.ingestion.binance import BinanceIngester


async def main():
    print("Starting OHLCV backfill...")

    yf = YahooFinanceIngester(period="2y", interval="1d")
    count = await yf.ingest()
    print(f"Yahoo Finance: {count} records ingested")

    binance = BinanceIngester(limit=730)
    count = await binance.ingest()
    print(f"Binance: {count} records ingested")

    print("Backfill complete!")


if __name__ == "__main__":
    asyncio.run(main())
