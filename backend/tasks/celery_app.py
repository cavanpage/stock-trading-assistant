from celery import Celery
from celery.schedules import crontab

from backend.config import settings

celery_app = Celery(
    "trading-assistant",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "backend.tasks.ingestion_tasks",
        "backend.tasks.ml_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "backend.tasks.ml_tasks.*": {"queue": "ml"},
        "backend.tasks.ingestion_tasks.*": {"queue": "ingestion"},
    },
)

celery_app.conf.beat_schedule = {
    "ingest-yahoo-hourly": {
        "task": "backend.tasks.ingestion_tasks.ingest_yahoo_finance",
        "schedule": crontab(minute=0),  # every hour
    },
    "ingest-binance-every-5min": {
        "task": "backend.tasks.ingestion_tasks.ingest_binance",
        "schedule": crontab(minute="*/5"),
    },
    "ingest-reddit-every-15min": {
        "task": "backend.tasks.ingestion_tasks.ingest_reddit",
        "schedule": crontab(minute="*/15"),
    },
    "ingest-newsapi-every-30min": {
        "task": "backend.tasks.ingestion_tasks.ingest_news",
        "schedule": crontab(minute="*/30"),
    },
    "ingest-congress-every-6hr": {
        "task": "backend.tasks.ingestion_tasks.ingest_congress",
        "schedule": crontab(hour="*/6", minute=0),
    },
    "ingest-finnhub-every-15min": {
        "task": "backend.tasks.ingestion_tasks.ingest_finnhub",
        "schedule": crontab(minute="*/15"),
    },
    "ingest-newsdata-every-hour": {
        "task": "backend.tasks.ingestion_tasks.ingest_newsdata",
        "schedule": crontab(minute=30),  # offset from newsapi to spread load
    },
    "run-sentiment-daily": {
        "task": "backend.tasks.ml_tasks.run_daily_sentiment",
        "schedule": crontab(hour=9, minute=0),
    },
}
