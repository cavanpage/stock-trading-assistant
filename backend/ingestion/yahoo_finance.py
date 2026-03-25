import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.ohlcv import OHLCV

logger = logging.getLogger(__name__)

DEFAULT_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "SPY", "QQQ",
    "BTC-USD", "ETH-USD", "SOL-USD",
]


class YahooFinanceIngester(BaseIngester):
    def __init__(self, symbols: list[str] | None = None, period: str = "2y", interval: str = "1d"):
        self.symbols = symbols or DEFAULT_WATCHLIST
        self.period = period
        self.interval = interval

    async def ingest(self) -> int:
        count = 0
        for symbol in self.symbols:
            try:
                count += await self._ingest_symbol(symbol)
            except Exception as e:
                logger.error(f"Failed to ingest {symbol}: {e}")
        return count

    async def _ingest_symbol(self, symbol: str) -> int:
        ticker = yf.Ticker(symbol)
        df: pd.DataFrame = ticker.history(period=self.period, interval=self.interval)
        if df.empty:
            return 0

        records = []
        for ts, row in df.iterrows():
            records.append({
                "symbol": symbol,
                "source": "yahoo_finance",
                "timestamp": ts.to_pydatetime().replace(tzinfo=timezone.utc),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            })

        async with AsyncSessionLocal() as session:
            stmt = insert(OHLCV).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["symbol", "source", "timestamp"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} candles for {symbol}")
        return len(records)
