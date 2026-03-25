import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy.dialects.postgresql import insert

from backend.config import settings
from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.congress_trade import CongressTrade

logger = logging.getLogger(__name__)

QUIVERQUANT_BASE = "https://api.quiverquant.com/beta"


class CongressIngester(BaseIngester):
    """Ingest congressional trading disclosures from QuiverQuant."""

    async def ingest(self) -> int:
        if not settings.quiverquant_api_key:
            logger.warning("QUIVERQUANT_API_KEY not set; skipping congress ingestion")
            return 0
        try:
            return await self._fetch_recent_trades()
        except Exception as e:
            logger.error(f"Congress ingestion failed: {e}")
            return 0

    async def _fetch_recent_trades(self) -> int:
        url = f"{QUIVERQUANT_BASE}/live/congresstrading"
        headers = {"Authorization": f"Token {settings.quiverquant_api_key}"}
        data = await self.fetch_with_retry(url, headers=headers)

        records = []
        for trade in data:
            try:
                tx_date_str = trade.get("TransactionDate") or trade.get("Date", "")
                disc_date_str = trade.get("DisclosureDate", "")
                records.append({
                    "member_name": trade.get("Representative", "Unknown"),
                    "party": trade.get("Party", ""),
                    "state": trade.get("State", ""),
                    "chamber": trade.get("Chamber", "House"),
                    "ticker": trade.get("Ticker", ""),
                    "asset_type": trade.get("AssetType", "Stock"),
                    "transaction_type": trade.get("Transaction", ""),
                    "amount_range": trade.get("Amount", ""),
                    "transaction_date": _parse_date(tx_date_str),
                    "disclosure_date": _parse_date(disc_date_str),
                    "notes": trade.get("Comment", ""),
                })
            except Exception as e:
                logger.debug(f"Skipping malformed trade record: {e}")

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(CongressTrade).values(records)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["member_name", "ticker", "transaction_date", "transaction_type"]
            )
            await session.execute(stmt)
            await session.commit()

        logger.info(f"Ingested {len(records)} congressional trades")
        return len(records)


def _parse_date(date_str: str) -> datetime:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return datetime.now(timezone.utc)
