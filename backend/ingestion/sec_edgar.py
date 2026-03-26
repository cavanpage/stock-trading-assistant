"""
SEC EDGAR Form 4 scraper — insider trading disclosures.

Flow:
  1. Fetch the SEC ticker→CIK map (cached locally, refreshed daily).
  2. For each watched symbol, pull recent Form 4 filings via the
     EDGAR submissions API (data.sec.gov).
  3. For each new filing, fetch and parse the primary XML document.
  4. Upsert into insider_trades, skipping anything already stored.

Rate limits: SEC asks for ≤ 10 req/s and a descriptive User-Agent.
We use a 0.15 s inter-request sleep which stays well under that.
"""

import asyncio
import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from sqlalchemy.dialects.postgresql import insert

from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.insider_trade import InsiderTrade

logger = logging.getLogger(__name__)

# EDGAR requires a meaningful User-Agent so they can contact you if needed.
EDGAR_HEADERS = {
    "User-Agent": "stock-trading-assistant contact@example.com",
    "Accept-Encoding": "gzip, deflate",
}

SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
TICKERS_URL     = "https://www.sec.gov/files/company_tickers.json"
ARCHIVES_BASE   = "https://www.sec.gov/Archives/edgar/data"
POLITE_DELAY    = 0.15   # seconds between requests

# Transaction codes we care about — open-market buys/sells carry the most signal.
# M = option exercise, G = gift, F = tax withholding (less interesting but kept)
SIGNAL_CODES = {"P", "S", "A", "D", "M", "G", "F"}

WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META",
    "BTC-USD", "ETH-USD",   # crypto has no Form 4 — filtered out automatically
]

_TICKER_CIK_CACHE: dict[str, str] = {}
_CACHE_PATH = Path("data/sec_ticker_cik.json")


async def _load_ticker_cik_map(client: httpx.AsyncClient) -> dict[str, str]:
    """Return ticker → zero-padded 10-digit CIK string."""
    global _TICKER_CIK_CACHE

    # Use disk cache if fresh (< 24 h)
    if _CACHE_PATH.exists():
        age = datetime.now().timestamp() - _CACHE_PATH.stat().st_mtime
        if age < 86400:
            raw = json.loads(_CACHE_PATH.read_text())
            _TICKER_CIK_CACHE = raw
            return raw

    logger.info("Fetching SEC ticker→CIK map from EDGAR")
    resp = await client.get(TICKERS_URL, headers=EDGAR_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()  # {str_idx: {cik_str, ticker, title}, ...}

    mapping = {}
    for entry in data.values():
        ticker = entry.get("ticker", "").upper()
        cik    = str(entry.get("cik_str", "")).zfill(10)
        if ticker:
            mapping[ticker] = cik

    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_PATH.write_text(json.dumps(mapping))
    _TICKER_CIK_CACHE = mapping
    return mapping


class SECEdgarIngester(BaseIngester):
    """Ingest Form 4 insider-trading disclosures from SEC EDGAR."""

    def __init__(self, symbols: list[str] | None = None, lookback_days: int = 7):
        self.symbols      = [s.upper() for s in (symbols or WATCHLIST)]
        self.lookback_days = lookback_days

    async def ingest(self) -> int:
        total = 0
        async with httpx.AsyncClient(timeout=30.0) as client:
            ticker_cik = await _load_ticker_cik_map(client)
            for symbol in self.symbols:
                cik = ticker_cik.get(symbol)
                if not cik:
                    logger.debug(f"No CIK for {symbol} (likely crypto) — skipping")
                    continue
                try:
                    count = await self._ingest_symbol(client, symbol, cik)
                    total += count
                    await asyncio.sleep(POLITE_DELAY)
                except Exception as e:
                    logger.error(f"EDGAR ingestion failed for {symbol}: {e}")
        logger.info(f"EDGAR Form 4: {total} insider transactions ingested")
        return total

    async def _ingest_symbol(self, client: httpx.AsyncClient, symbol: str, cik: str) -> int:
        url  = SUBMISSIONS_URL.format(cik=cik)
        resp = await client.get(url, headers=EDGAR_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        filings = data.get("filings", {}).get("recent", {})
        forms        = filings.get("form", [])
        dates        = filings.get("filingDate", [])
        accessions   = filings.get("accessionNumber", [])
        primary_docs = filings.get("primaryDocument", [])

        cutoff = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        count  = 0

        for form, date_str, acc_no, primary_doc in zip(forms, dates, accessions, primary_docs):
            if form != "4":
                continue
            try:
                filing_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if filing_date < cutoff:
                break   # sorted descending — no need to keep scanning

            await asyncio.sleep(POLITE_DELAY)
            count += await self._parse_filing(client, symbol, cik, acc_no, primary_doc, filing_date)

        return count

    async def _parse_filing(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        cik: str,
        acc_no: str,
        primary_doc: str,
        filing_date: datetime,
    ) -> int:
        acc_clean = acc_no.replace("-", "")
        url = f"{ARCHIVES_BASE}/{int(cik)}/{acc_clean}/{primary_doc}"

        try:
            resp = await client.get(url, headers=EDGAR_HEADERS, timeout=30)
            resp.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.warning(f"Could not fetch {url}: {e}")
            return 0

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            logger.warning(f"XML parse error for {acc_no}: {e}")
            return 0

        ns = {"": ""}  # Form 4 XML has no namespace

        def _val(node, path: str) -> str | None:
            el = node.find(f".//{path}/value") or node.find(f".//{path}")
            return el.text.strip() if el is not None and el.text else None

        # Issuer
        issuer_cik  = _val(root, "issuerCik")  or cik
        issuer_name = _val(root, "issuerName") or ""

        # Owner
        owner_cik   = _val(root, "rptOwnerCik")  or ""
        owner_name  = _val(root, "rptOwnerName") or "Unknown"
        owner_title = _val(root, "officerTitle")
        is_director = (_val(root, "isDirector") or "0") == "1"
        is_officer  = (_val(root, "isOfficer")  or "0") == "1"
        is_ten_pct  = (_val(root, "isTenPercentOwner") or "0") == "1"

        records = []
        seq = 0

        for txn in root.findall(".//nonDerivativeTransaction"):
            row = _parse_transaction(
                txn, seq, acc_no, filing_date, filing_url=url,
                ticker=symbol, issuer_name=issuer_name, issuer_cik=issuer_cik,
                owner_cik=owner_cik, owner_name=owner_name, owner_title=owner_title,
                is_director=is_director, is_officer=is_officer, is_ten_pct=is_ten_pct,
            )
            if row:
                records.append(row)
                seq += 1

        if not records:
            return 0

        async with AsyncSessionLocal() as session:
            stmt = insert(InsiderTrade).values(records)
            stmt = stmt.on_conflict_do_nothing(index_elements=["accession_number", "sequence_number"])
            await session.execute(stmt)
            await session.commit()

        logger.info(f"EDGAR {symbol} ({acc_no}): {len(records)} transactions")
        return len(records)


def _parse_transaction(
    txn: ET.Element,
    seq: int,
    acc_no: str,
    filing_date: datetime,
    filing_url: str,
    ticker: str,
    issuer_name: str,
    issuer_cik: str,
    owner_cik: str,
    owner_name: str,
    owner_title: str | None,
    is_director: bool,
    is_officer: bool,
    is_ten_pct: bool,
) -> dict | None:

    def _txt(path: str) -> str | None:
        el = txn.find(f".//{path}/value") or txn.find(f".//{path}")
        return el.text.strip() if el is not None and el.text else None

    code = _txt("transactionCode")
    if code not in SIGNAL_CODES:
        return None

    date_str = _txt("transactionDate")
    try:
        txn_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        txn_date = filing_date

    shares = _float(_txt("transactionShares"))
    price  = _float(_txt("transactionPricePerShare"))
    total  = shares * price if shares is not None and price is not None else None
    ad     = _txt("transactionAcquiredDisposedCode") or ("A" if code == "P" else "D")

    return {
        "accession_number":  acc_no,
        "sequence_number":   seq,
        "filing_date":       filing_date,
        "filing_url":        filing_url,
        "ticker":            ticker,
        "issuer_name":       issuer_name,
        "issuer_cik":        issuer_cik,
        "owner_name":        owner_name,
        "owner_cik":         owner_cik,
        "owner_title":       owner_title,
        "is_director":       is_director,
        "is_officer":        is_officer,
        "is_ten_pct":        is_ten_pct,
        "security_title":    _txt("securityTitle") or "Common Stock",
        "transaction_date":  txn_date,
        "transaction_code":  code,
        "shares":            shares,
        "price_per_share":   price,
        "total_value":       total,
        "acquired_disposed": ad,
        "shares_after":      _float(_txt("sharesOwnedFollowingTransaction")),
    }


def _float(val: str | None) -> float | None:
    if val is None:
        return None
    try:
        return float(re.sub(r"[^\d.\-]", "", val))
    except (ValueError, TypeError):
        return None
