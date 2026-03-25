"""
CoinGecko historical crypto data ingester.

Why CoinGecko for training data:
  - Free tier: daily OHLCV back to coin inception (BTC = 2013, ETH = 2015)
  - No API key required for public endpoints
  - Covers 10,000+ coins — useful for altcoin signal training
  - Rate limit: 10-30 calls/min on free tier

This gives crypto models far more regime data than 2 years:
  BTC: ~4,000 daily bars (2013–present) — includes 3 full bull/bear cycles
  ETH: ~3,200 daily bars (2015–present)
"""
import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)

COINGECKO_BASE = "https://api.coingecko.com/api/v3"

# CoinGecko coin ID → our internal symbol mapping
COIN_MAP = {
    "bitcoin":       "BTC-USD",
    "ethereum":      "ETH-USD",
    "solana":        "SOL-USD",
    "binancecoin":   "BNB-USD",
    "ripple":        "XRP-USD",
    "cardano":       "ADA-USD",
    "avalanche-2":   "AVAX-USD",
    "chainlink":     "LINK-USD",
    "polkadot":      "DOT-USD",
    "matic-network": "MATIC-USD",
}


class CoinGeckoIngester(BaseIngester):
    """
    Fetch full daily OHLCV history for crypto coins from CoinGecko.

    vs_currency: base currency for prices (usd recommended for USD-denominated models)
    days: "max" for full history, or integer (e.g. 365)
    """

    def __init__(
        self,
        coins: dict[str, str] | None = None,
        vs_currency: str = "usd",
        days: str = "max",
    ):
        self.coins = coins or COIN_MAP
        self.vs_currency = vs_currency
        self.days = days

    async def ingest(self) -> int:
        count = 0
        for coin_id, symbol in self.coins.items():
            try:
                count += await self._ingest_coin(coin_id, symbol)
                await asyncio.sleep(2.5)   # free tier: ~24 calls/min safe
            except Exception as e:
                logger.error(f"CoinGecko ingestion failed for {coin_id}: {e}")
        return count

    async def _ingest_coin(self, coin_id: str, symbol: str) -> int:
        url = f"{COINGECKO_BASE}/coins/{coin_id}/ohlc"
        data = await self.fetch_with_retry(
            url,
            params={
                "vs_currency": self.vs_currency,
                "days": self.days,
            },
        )

        if not data or not isinstance(data, list):
            logger.debug(f"No CoinGecko OHLC data for {coin_id}")
            return 0

        # CoinGecko OHLC format: [timestamp_ms, open, high, low, close]
        records = []
        for bar in data:
            if len(bar) < 5:
                continue
            ts_ms, open_, high, low, close = bar[0], bar[1], bar[2], bar[3], bar[4]
            records.append({
                "symbol": symbol,
                "source": "coingecko_daily",
                "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "volume": None,   # OHLC endpoint doesn't include volume; use market_chart for that
            })

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(OHLCV).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "source", "timestamp"]
            )
            await session.execute(stmt)
            await session.commit()

        start = records[0]["timestamp"].year
        end = records[-1]["timestamp"].year
        logger.info(f"CoinGecko: ingested {len(records)} daily bars for {symbol} ({start}–{end})")
        return len(records)

    async def ingest_market_chart(self, coin_id: str, symbol: str) -> int:
        """
        Fetch daily market chart data which includes volume.
        Complements the OHLC endpoint — run after _ingest_coin to fill volume.
        """
        url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
        data = await self.fetch_with_retry(
            url,
            params={
                "vs_currency": self.vs_currency,
                "days": self.days,
                "interval": "daily",
            },
        )

        volumes = {
            datetime.fromtimestamp(v[0] / 1000, tz=timezone.utc): v[1]
            for v in data.get("total_volumes", [])
        }

        if not volumes:
            return 0

        # Update existing OHLCV records with volume
        from sqlalchemy import update, and_
        from backend.models.ohlcv import OHLCV as OHLCVModel

        updated = 0
        async with AsyncSessionLocal() as session:
            for ts, vol in volumes.items():
                result = await session.execute(
                    update(OHLCVModel)
                    .where(
                        and_(
                            OHLCVModel.symbol == symbol,
                            OHLCVModel.source == "coingecko_daily",
                            OHLCVModel.timestamp == ts,
                        )
                    )
                    .values(volume=vol)
                )
                updated += result.rowcount
            await session.commit()

        logger.info(f"CoinGecko: updated {updated} volume records for {symbol}")
        return updated
