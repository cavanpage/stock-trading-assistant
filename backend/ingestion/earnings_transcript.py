"""
Earnings call transcript scraper.

Strategy (tried in order, first hit wins):
  1. Motley Fool — free, well-structured HTML, transcripts up ~1 hour after call.
  2. SEC 8-K exhibit — companies often file the transcript as an Exhibit 99 to an 8-K.
     Always available, sometimes slower (1-2 days after call).

Trigger: the Finnhub earnings calendar (already ingested) tells us exactly when
each call happened. This ingester queries that calendar for calls in the past
`lookback_days` window that don't yet have a transcript stored.

FinBERT scores are computed across sliding 512-token chunks and averaged,
then separately on lines attributed to CEO/CFO if speaker labels are parseable.
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

from backend.db.session import AsyncSessionLocal
from backend.ingestion.base import BaseIngester
from backend.models.earnings_transcript import EarningsTranscript

logger = logging.getLogger(__name__)

POLITE_DELAY = 1.0   # Motley Fool — be polite, 1 req/s

WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META",
]

EXEC_TITLES_RE = re.compile(
    r"\b(chief executive|ceo|chief financial|cfo|president|chairman)\b",
    re.IGNORECASE,
)


class EarningsTranscriptIngester(BaseIngester):

    def __init__(self, symbols: list[str] | None = None, lookback_days: int = 14):
        self.symbols       = [s.upper() for s in (symbols or WATCHLIST)]
        self.lookback_days = lookback_days

    async def ingest(self) -> int:
        total = 0
        async with httpx.AsyncClient(
            timeout=30.0,
            headers={"User-Agent": "Mozilla/5.0 (compatible; stock-trading-assistant)"},
            follow_redirects=True,
        ) as client:
            for symbol in self.symbols:
                try:
                    count = await self._ingest_symbol(client, symbol)
                    total += count
                except Exception as e:
                    logger.error(f"Transcript ingestion failed for {symbol}: {e}")
        logger.info(f"Earnings transcripts: {total} new transcripts ingested")
        return total

    async def _ingest_symbol(self, client: httpx.AsyncClient, symbol: str) -> int:
        # Find recent earnings calls from the Finnhub data already in the DB.
        # Falls back to a simple quarterly schedule if not available.
        calls = await _recent_earnings_calls(symbol, self.lookback_days)
        if not calls:
            logger.debug(f"No recent earnings calls found for {symbol}")
            return 0

        count = 0
        for quarter, year, call_date in calls:
            if await _transcript_exists(symbol, quarter, year):
                continue

            await asyncio.sleep(POLITE_DELAY)
            record = await _fetch_transcript(client, symbol, quarter, year, call_date)
            if record:
                await _upsert(record)
                count += 1
                logger.info(f"Transcript ingested: {symbol} Q{quarter} {year} ({record['word_count']} words)")
        return count


# ── Fetch helpers ──────────────────────────────────────────────────────────────

async def _fetch_transcript(
    client: httpx.AsyncClient,
    symbol: str,
    quarter: int,
    year: int,
    call_date: datetime,
) -> dict | None:
    # 1. Try Motley Fool
    result = await _motley_fool(client, symbol, quarter, year, call_date)
    if result:
        return result

    # 2. Try SEC 8-K exhibit
    result = await _sec_8k(client, symbol, quarter, year, call_date)
    return result


async def _motley_fool(
    client: httpx.AsyncClient,
    symbol: str,
    quarter: int,
    year: int,
    call_date: datetime,
) -> dict | None:
    """
    Motley Fool transcript search.
    URL pattern: /earnings/call-transcripts/?company={symbol}
    Then find the link matching Q{N} {YEAR}.
    """
    search_url = f"https://www.fool.com/earnings/call-transcripts/?company={symbol.lower()}"
    try:
        resp = await client.get(search_url)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        logger.debug(f"Motley Fool search failed for {symbol}: {e}")
        return None

    # Find a link that contains Q{quarter} and the year
    target_pattern = re.compile(
        rf"Q{quarter}[^0-9]{{0,10}}{year}|{year}[^0-9]{{0,10}}Q{quarter}",
        re.IGNORECASE,
    )
    transcript_url = None
    for a in soup.find_all("a", href=True):
        title = a.get_text(" ", strip=True)
        if symbol.upper() in title.upper() and target_pattern.search(title):
            href = a["href"]
            transcript_url = href if href.startswith("http") else f"https://www.fool.com{href}"
            break

    if not transcript_url:
        logger.debug(f"No Motley Fool transcript found for {symbol} Q{quarter} {year}")
        return None

    await asyncio.sleep(POLITE_DELAY)
    try:
        page = await client.get(transcript_url)
        if page.status_code != 200:
            return None
        soup2 = BeautifulSoup(page.text, "html.parser")
    except Exception as e:
        logger.debug(f"Motley Fool transcript page failed: {e}")
        return None

    # The transcript body lives in the article content div
    body_div = (
        soup2.find("div", class_=re.compile(r"article-body|transcript|content"))
        or soup2.find("article")
    )
    if not body_div:
        return None

    full_text = body_div.get_text("\n", strip=True)
    if len(full_text) < 500:   # sanity check — real transcripts are 5k+ words
        return None

    company_name = _extract_company_name(soup2, symbol)

    return _build_record(
        ticker=symbol,
        company_name=company_name,
        quarter=quarter,
        year=year,
        call_date=call_date,
        full_text=full_text,
        source="motley_fool",
        source_url=transcript_url,
    )


async def _sec_8k(
    client: httpx.AsyncClient,
    symbol: str,
    quarter: int,
    year: int,
    call_date: datetime,
) -> dict | None:
    """
    Fallback: search EDGAR for an 8-K filed within 3 days of the call date.
    Many 8-Ks contain the transcript as Exhibit 99.2.
    """
    from backend.ingestion.sec_edgar import _load_ticker_cik_map, EDGAR_HEADERS, ARCHIVES_BASE

    try:
        ticker_cik = await _load_ticker_cik_map(client)
        cik = ticker_cik.get(symbol)
        if not cik:
            return None

        sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        resp = await client.get(sub_url, headers=EDGAR_HEADERS)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.debug(f"SEC 8-K lookup failed for {symbol}: {e}")
        return None

    filings      = data.get("filings", {}).get("recent", {})
    forms        = filings.get("form", [])
    dates        = filings.get("filingDate", [])
    accessions   = filings.get("accessionNumber", [])
    primary_docs = filings.get("primaryDocument", [])

    window_start = call_date - timedelta(days=1)
    window_end   = call_date + timedelta(days=3)

    for form, date_str, acc_no, primary_doc in zip(forms, dates, accessions, primary_docs):
        if form != "8-K":
            continue
        try:
            filing_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if not (window_start <= filing_date <= window_end):
            continue

        # Fetch the filing index to look for transcript exhibits
        acc_clean = acc_no.replace("-", "")
        index_url = f"{ARCHIVES_BASE}/{int(cik)}/{acc_clean}/{acc_no}-index.htm"
        try:
            await asyncio.sleep(0.15)
            idx_resp = await client.get(index_url, headers=EDGAR_HEADERS)
            idx_soup = BeautifulSoup(idx_resp.text, "html.parser")
        except Exception:
            continue

        # Look for exhibits with "transcript" in the description
        for row in idx_soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            desc = cells[1].get_text(strip=True).lower()
            if "transcript" not in desc:
                continue
            doc_link = cells[2].find("a", href=True)
            if not doc_link:
                continue

            doc_url = f"https://www.sec.gov{doc_link['href']}"
            try:
                await asyncio.sleep(0.15)
                doc_resp = await client.get(doc_url, headers=EDGAR_HEADERS)
                full_text = BeautifulSoup(doc_resp.text, "html.parser").get_text("\n", strip=True)
                if len(full_text) < 500:
                    continue

                return _build_record(
                    ticker=symbol,
                    company_name=symbol,
                    quarter=quarter,
                    year=year,
                    call_date=call_date,
                    full_text=full_text,
                    source="sec_8k",
                    source_url=doc_url,
                )
            except Exception:
                continue

    return None


# ── Utilities ──────────────────────────────────────────────────────────────────

def _build_record(
    ticker: str,
    company_name: str,
    quarter: int,
    year: int,
    call_date: datetime,
    full_text: str,
    source: str,
    source_url: str,
) -> dict:
    sentiment_score, exec_score = _score_transcript(full_text)
    return {
        "ticker":               ticker,
        "company_name":         company_name,
        "fiscal_quarter":       quarter,
        "fiscal_year":          year,
        "call_date":            call_date,
        "source":               source,
        "source_url":           source_url,
        "full_text":            full_text,
        "word_count":           len(full_text.split()),
        "sentiment_score":      sentiment_score,
        "exec_sentiment_score": exec_score,
    }


def _score_transcript(text: str) -> tuple[float | None, float | None]:
    """
    Run FinBERT on 512-token chunks of the transcript and average the scores.
    Returns (overall_score, exec_score).  Both are None if FinBERT unavailable.
    """
    try:
        from backend.ml.sentiment.finbert import score_texts

        # Chunk by sentences to respect the 512-token limit
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if len(s.strip()) > 20]

        # Combine into ~300-word chunks
        chunks, current, count = [], [], 0
        for s in sentences:
            words = len(s.split())
            if count + words > 300 and current:
                chunks.append(" ".join(current))
                current, count = [], 0
            current.append(s)
            count += words
        if current:
            chunks.append(" ".join(current))

        if not chunks:
            return None, None

        all_scores = score_texts(chunks)
        overall = sum(all_scores) / len(all_scores)

        # Try to extract executive speaking turns (lines after "Name -- Title")
        exec_lines = _extract_exec_lines(text)
        exec_score = None
        if exec_lines:
            exec_chunks = [" ".join(exec_lines[i:i+10]) for i in range(0, len(exec_lines), 10)]
            exec_scores = score_texts(exec_chunks)
            exec_score  = sum(exec_scores) / len(exec_scores) if exec_scores else None

        return round(overall, 4), round(exec_score, 4) if exec_score is not None else None

    except Exception as e:
        logger.warning(f"FinBERT scoring failed: {e}")
        return None, None


def _extract_exec_lines(text: str) -> list[str]:
    """
    Heuristic: Motley Fool transcripts label speakers as
    "First Last -- Chief Executive Officer" before their remarks.
    Collect sentences that follow an exec label.
    """
    lines   = text.split("\n")
    in_exec = False
    result  = []
    for line in lines:
        if "--" in line and EXEC_TITLES_RE.search(line):
            in_exec = True
            continue
        if in_exec:
            if re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+ --", line):
                in_exec = False  # new speaker, non-exec
            else:
                result.append(line.strip())
    return [l for l in result if len(l) > 20]


def _extract_company_name(soup: BeautifulSoup, fallback: str) -> str:
    title = soup.find("h1")
    if title:
        text = title.get_text(strip=True)
        m = re.match(r"^(.+?)\s*\(", text)
        if m:
            return m.group(1).strip()
    return fallback


async def _recent_earnings_calls(symbol: str, lookback_days: int) -> list[tuple[int, int, datetime]]:
    """
    Query the DB for recent earnings events for this symbol.
    Returns list of (quarter, year, call_datetime).
    Falls back to calendar estimation if no data.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    async with AsyncSessionLocal() as session:
        try:
            rows = await session.execute(
                text("""
                    SELECT period, date
                    FROM finnhub_earnings_calendar
                    WHERE symbol = :sym AND date >= :cutoff
                    ORDER BY date DESC
                """),
                {"sym": symbol, "cutoff": cutoff},
            )
            results = rows.fetchall()
        except Exception:
            results = []

    calls = []
    for row in results:
        period_str = row[0] or ""
        call_date  = row[1]
        if isinstance(call_date, str):
            try:
                call_date = datetime.strptime(call_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        m = re.match(r"(\d{4})Q(\d)", period_str)
        if m:
            year, quarter = int(m.group(1)), int(m.group(2))
            calls.append((quarter, year, call_date))

    # If no calendar data, estimate from current date
    if not calls:
        now = datetime.now(timezone.utc)
        quarter = (now.month - 1) // 3 + 1
        calls = [(quarter, now.year, now - timedelta(days=7))]

    return calls


async def _transcript_exists(symbol: str, quarter: int, year: int) -> bool:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(EarningsTranscript.id).where(
                EarningsTranscript.ticker        == symbol,
                EarningsTranscript.fiscal_quarter == quarter,
                EarningsTranscript.fiscal_year    == year,
            ).limit(1)
        )
        return result.scalar() is not None


async def _upsert(record: dict) -> None:
    async with AsyncSessionLocal() as session:
        stmt = insert(EarningsTranscript).values([record])
        stmt = stmt.on_conflict_do_update(
            index_elements=["ticker", "fiscal_quarter", "fiscal_year"],
            set_={
                "full_text":            stmt.excluded.full_text,
                "word_count":           stmt.excluded.word_count,
                "sentiment_score":      stmt.excluded.sentiment_score,
                "exec_sentiment_score": stmt.excluded.exec_sentiment_score,
                "source_url":           stmt.excluded.source_url,
            },
        )
        await session.execute(stmt)
        await session.commit()
