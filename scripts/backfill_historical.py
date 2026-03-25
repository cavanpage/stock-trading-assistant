#!/usr/bin/env python3
"""
One-time historical data backfill for model training.

Run order matters:
  1. Polygon   — best quality, intraday, 2yr free
  2. Alpha Vantage — 20yr daily (rate limited: ~25 calls/day free)
  3. CoinGecko  — crypto since inception, no key required
  4. Yahoo Finance — fills any remaining gaps

This is a long-running script. Alpha Vantage free tier takes ~3 hours
for 12 symbols due to rate limits. Run overnight.

Usage:
    python scripts/backfill_historical.py             # all sources
    python scripts/backfill_historical.py --polygon   # polygon only
    python scripts/backfill_historical.py --crypto    # crypto only (CoinGecko)
    python scripts/backfill_historical.py --macro     # macro indicators only
"""
import asyncio
import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


async def run_polygon():
    from backend.ingestion.polygon import PolygonIngester
    from datetime import date, timedelta

    print("\n── Polygon: daily bars (2yr, split-adjusted) ──")
    ingester = PolygonIngester(
        timespan="day",
        multiplier=1,
        from_date=(date.today().replace(year=date.today().year - 5)).isoformat(),
    )
    count = await ingester.ingest()
    print(f"   → {count:,} bars ingested")

    print("\n── Polygon: 5-min bars (2yr, intraday) ──")
    ingester_5m = PolygonIngester(
        timespan="minute",
        multiplier=5,
    )
    count = await ingester_5m.ingest()
    print(f"   → {count:,} bars ingested")


async def run_alpha_vantage():
    from backend.ingestion.alpha_vantage import AlphaVantageIngester

    print("\n── Alpha Vantage: 20yr daily history ──")
    print("   Note: free tier = 25 calls/day → ~15 sec between requests")
    ingester = AlphaVantageIngester()
    count = await ingester.ingest()
    print(f"   → {count:,} bars ingested")

    print("\n── Alpha Vantage: macro indicators ──")
    results = await ingester.ingest_macro_indicators()
    for name, count in results.items():
        print(f"   {name}: {count} records")


async def run_coingecko():
    from backend.ingestion.coingecko import CoinGeckoIngester

    print("\n── CoinGecko: crypto since inception ──")
    ingester = CoinGeckoIngester(days="max")
    count = await ingester.ingest()
    print(f"   → {count:,} bars ingested")


async def run_yahoo_finance():
    from backend.ingestion.yahoo_finance import YahooFinanceIngester

    print("\n── Yahoo Finance: 10yr daily (gap fill) ──")
    ingester = YahooFinanceIngester(period="10y", interval="1d")
    count = await ingester.ingest()
    print(f"   → {count:,} bars ingested")


async def main(args):
    print("Historical data backfill")
    print("=" * 50)

    if args.polygon or args.all:
        await run_polygon()

    if args.alpha_vantage or args.all:
        await run_alpha_vantage()

    if args.crypto or args.all:
        await run_coingecko()

    if args.yahoo or args.all:
        await run_yahoo_finance()

    if args.macro:
        from backend.ingestion.alpha_vantage import AlphaVantageIngester
        ingester = AlphaVantageIngester()
        results = await ingester.ingest_macro_indicators()
        for name, count in results.items():
            print(f"  {name}: {count} records")

    print("\nBackfill complete.")
    print("Next: python scripts/build_training_dataset.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--polygon",       action="store_true")
    parser.add_argument("--alpha-vantage", action="store_true", dest="alpha_vantage")
    parser.add_argument("--crypto",        action="store_true")
    parser.add_argument("--yahoo",         action="store_true")
    parser.add_argument("--macro",         action="store_true")
    args = parser.parse_args()

    # Default: run everything
    args.all = not any([args.polygon, args.alpha_vantage, args.crypto, args.yahoo, args.macro])

    asyncio.run(main(args))
