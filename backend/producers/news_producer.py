"""
Finnhub WebSocket + polling → Kafka producer.

Finnhub streams real-time news via WebSocket. Each article is published to
the `news.raw` Kafka topic for Flink to pick up and score.

Run as a standalone process:
    python -m flink.producers.news_producer
"""
import asyncio
import json
import logging
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NEWS = "news.raw"
FINNHUB_WS_URL = "wss://ws.finnhub.io"

WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "GOOGL", "META", "SPY", "QQQ",
]


def _make_producer(retries: int = 10) -> KafkaProducer:
    for i in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable:
            wait = 2 ** i
            logger.warning(f"Kafka not ready, retrying in {wait}s ({i+1}/{retries})")
            time.sleep(wait)
    raise RuntimeError("Could not connect to Kafka after retries")


async def stream_news():
    """Connect to Finnhub WebSocket and forward news events to Kafka."""
    import websockets

    producer = _make_producer()
    finnhub_key = os.getenv("FINNHUB_API_KEY", "")

    while True:
        try:
            uri = f"{FINNHUB_WS_URL}?token={finnhub_key}"
            async with websockets.connect(uri) as ws:
                # Subscribe to news for each symbol
                for symbol in WATCHLIST:
                    await ws.send(json.dumps({"type": "subscribe-news", "symbol": symbol}))

                logger.info(f"Finnhub WS connected, subscribed to {len(WATCHLIST)} symbols")

                async for raw in ws:
                    event = json.loads(raw)
                    if event.get("type") != "news":
                        continue

                    for article in event.get("data", []):
                        msg = {
                            "type": "news",
                            "headline": article.get("headline", ""),
                            "summary": article.get("summary", ""),
                            "url": article.get("url", ""),
                            "source": article.get("source", "finnhub"),
                            "symbols": article.get("related", "").split(","),
                            "sentiment": article.get("sentiment"),  # pre-computed if available
                            "timestamp": article.get("datetime"),
                        }
                        # Key by first related symbol for Flink partitioning
                        key = msg["symbols"][0].strip().encode() if msg["symbols"] else b"general"
                        producer.send(TOPIC_NEWS, value=msg, key=key)
                        logger.debug(f"News published: {msg['headline'][:60]}")

        except Exception as e:
            logger.error(f"Finnhub WebSocket error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(stream_news())
