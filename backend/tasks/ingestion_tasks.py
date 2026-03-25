import asyncio
import logging

from backend.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_yahoo_finance", bind=True, max_retries=3)
def ingest_yahoo_finance(self):
    from backend.ingestion.yahoo_finance import YahooFinanceIngester
    try:
        count = _run(YahooFinanceIngester().ingest())
        logger.info(f"Yahoo Finance ingestion complete: {count} records")
        return count
    except Exception as exc:
        logger.error(f"Yahoo Finance ingestion failed: {exc}")
        raise self.retry(exc=exc, countdown=60)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_binance", bind=True, max_retries=3)
def ingest_binance(self):
    from backend.ingestion.binance import BinanceIngester
    try:
        count = _run(BinanceIngester().ingest())
        logger.info(f"Binance ingestion complete: {count} records")
        return count
    except Exception as exc:
        raise self.retry(exc=exc, countdown=30)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_reddit", bind=True, max_retries=2)
def ingest_reddit(self):
    from backend.ingestion.reddit import RedditIngester
    try:
        count = _run(RedditIngester().ingest())
        logger.info(f"Reddit ingestion complete: {count} records")
        return count
    except Exception as exc:
        raise self.retry(exc=exc, countdown=120)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_news", bind=True, max_retries=2)
def ingest_news(self):
    from backend.ingestion.newsapi import NewsAPIIngester
    try:
        count = _run(NewsAPIIngester().ingest())
        logger.info(f"NewsAPI ingestion complete: {count} records")
        return count
    except Exception as exc:
        raise self.retry(exc=exc, countdown=180)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_congress", bind=True, max_retries=2)
def ingest_congress(self):
    from backend.ingestion.congress import CongressIngester
    try:
        count = _run(CongressIngester().ingest())
        logger.info(f"Congress ingestion complete: {count} records")
        return count
    except Exception as exc:
        raise self.retry(exc=exc, countdown=300)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_finnhub", bind=True, max_retries=2)
def ingest_finnhub(self):
    from backend.ingestion.finnhub import FinnhubIngester
    try:
        count = _run(FinnhubIngester().ingest())
        logger.info(f"Finnhub ingestion complete: {count} records")
        return count
    except Exception as exc:
        raise self.retry(exc=exc, countdown=120)


@celery_app.task(name="backend.tasks.ingestion_tasks.ingest_newsdata", bind=True, max_retries=2)
def ingest_newsdata(self):
    from backend.ingestion.newsdata import NewsDataIngester
    try:
        count = _run(NewsDataIngester().ingest())
        logger.info(f"NewsData ingestion complete: {count} records")
        return count
    except Exception as exc:
        raise self.retry(exc=exc, countdown=180)
