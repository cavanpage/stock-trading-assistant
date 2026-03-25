"""
Alpaca WebSocket → Kafka producer.

Subscribes to real-time stock quotes and trade updates from Alpaca,
serialises each event as JSON, and publishes to the `price.ticks` Kafka topic.

Run this as a standalone process (not via Flink — it's just a producer):
    python -m flink.producers.price_producer
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
TOPIC_PRICE = "price.ticks"
ALPACA_WS_URL = "wss://stream.data.alpaca.markets/v2/iex"

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


async def stream_prices():
    """Connect to Alpaca WebSocket and forward price events to Kafka."""
    import websockets

    producer = _make_producer()
    alpaca_key = os.getenv("ALPACA_API_KEY", "")
    alpaca_secret = os.getenv("ALPACA_API_SECRET", "")

    while True:
        try:
            async with websockets.connect(ALPACA_WS_URL) as ws:
                # Authenticate
                await ws.send(json.dumps({"action": "auth", "key": alpaca_key, "secret": alpaca_secret}))
                auth_resp = json.loads(await ws.recv())
                logger.info(f"Alpaca auth: {auth_resp}")

                # Subscribe to trades + quotes for watchlist
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "trades": WATCHLIST,
                    "quotes": WATCHLIST,
                    "bars": WATCHLIST,
                }))
                sub_resp = json.loads(await ws.recv())
                logger.info(f"Alpaca subscription: {sub_resp}")

                async for raw in ws:
                    events = json.loads(raw)
                    for event in events:
                        event_type = event.get("T")  # "t"=trade, "q"=quote, "b"=bar

                        if event_type == "b":  # 1-min bar — richest for inference
                            msg = {
                                "type": "bar",
                                "symbol": event["S"],
                                "open": event["o"],
                                "high": event["h"],
                                "low": event["l"],
                                "close": event["c"],
                                "volume": event["v"],
                                "vwap": event.get("vw"),
                                "timestamp": event["t"],
                                "source": "alpaca",
                            }
                            producer.send(TOPIC_PRICE, value=msg, key=event["S"].encode())
                            logger.debug(f"Bar published: {event['S']} @ {event['c']}")

                        elif event_type == "t":  # individual trade
                            msg = {
                                "type": "trade",
                                "symbol": event["S"],
                                "price": event["p"],
                                "size": event["s"],
                                "timestamp": event["t"],
                                "source": "alpaca",
                            }
                            producer.send(TOPIC_PRICE, value=msg, key=event["S"].encode())

        except Exception as e:
            logger.error(f"Alpaca WebSocket error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(stream_prices())
