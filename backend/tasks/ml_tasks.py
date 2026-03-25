import asyncio
import logging

from backend.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


@celery_app.task(name="backend.tasks.ml_tasks.run_daily_sentiment", bind=True)
def run_daily_sentiment(self):
    """Placeholder: Run FinBERT sentiment scoring on yesterday's posts and news."""
    logger.info("Daily sentiment scoring task triggered (not yet implemented)")
    # TODO: Query unscored posts from db, batch through finbert, update sentiment_score
    return 0
