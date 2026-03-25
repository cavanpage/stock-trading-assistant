import logging
from datetime import timezone

from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.ohlcv import OHLCV
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)

BINANCE_BASE = "https://api.binance.com/api/v3"
DEFAULT_CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

BINANCE_TO_YAHOO = {
    "BTCUSDT": "BTC-USD",
    "ETHUSDT": "ETH-USD",
    "SOLUSDT": "SOL-USD",
    "BNBUSDT": "BNB-USD",
    "XRPUSDT": "XRP-USD",
}


class BinanceIngester(BaseIngester):
    def __init__(self, pairs: list[str] | None = None, interval: str = "1d", limit: int = 500):
        self.pairs = pairs or DEFAULT_CRYPTO_PAIRS
        self.interval = interval
        self.limit = limit

    async def ingest(self) -> int:
        count = 0
        for pair in self.pairs:
            try:
                count += await self._ingest_pair(pair)
            except Exception as e:
                logger.error(f"Failed to ingest Binance pair {pair}: {e}")
        return count

    async def _ingest_pair(self, pair: str) -> int:
        url = f"{BINANCE_BASE}/klines"
        data = await self.fetch_with_retry(
            url,
            params={"symbol": pair, "interval": self.interval, "limit": self.limit},
        )

        symbol = BINANCE_TO_YAHOO.get(pair, pair)
        records = []
        for candle in data:
            open_time_ms = candle[0]
            from datetime import datetime
            ts = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            records.append({
                "symbol": symbol,
                "source": "binance",
                "timestamp": ts,
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5]),
                "trade_count": int(candle[8]),
            })

        async with AsyncSessionLocal() as session:
            stmt = insert(OHLCV).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "source", "timestamp"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} candles for {pair}")
        return len(records)
