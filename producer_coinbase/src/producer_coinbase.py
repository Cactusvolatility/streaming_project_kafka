import asyncio
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import websockets
from aiokafka import AIOKafkaProducer


WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_events")

PRODUCT_IDS = os.getenv("PRODUCT_IDS", "ETH-USD").split(",")
CHANNELS = os.getenv("CHANNELS", "ticker").split(",")  # e.g. "ticker,heartbeat,matches"

# Basic tuning
MAX_BACKOFF_S = float(os.getenv("MAX_BACKOFF_S", "30"))
PING_INTERVAL = None  # Coinbase docs often show ping_interval=None


@dataclass
class SeqTracker:
    """Track per-product sequence for gap detection (best-effort)."""
    last_seq: Dict[str, int]

    def __init__(self) -> None:
        self.last_seq = {}

    def observe(self, product_id: str, seq: Optional[int]) -> bool:
        """
        Returns True if a gap is detected (seq > last+1).
        If seq is None, returns False.
        """
        if seq is None:
            return False
        prev = self.last_seq.get(product_id)
        self.last_seq[product_id] = seq
        if prev is None:
            return False
        return seq > prev + 1


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_message(msg: Dict[str, Any], gap_detected: bool) -> Dict[str, Any]:
    """
    Normalize Coinbase messages into a stable envelope.
    Keep raw payload too (useful for replay/debugging).
    """
    product_id = msg.get("product_id")
    seq = msg.get("sequence")

    event_time = msg.get("time")  # ISO8601 when present
    msg_type = msg.get("type", "unknown")

    return {
        "schema_version": 1,
        "source": "coinbase_ws",
        "ingest_ts_ms": now_ms(),
        "event_time": event_time,          # may be None depending on message type
        "type": msg_type,
        "product_id": product_id,
        "sequence": seq,
        "gap_detected": gap_detected,
        "payload": msg,                    # raw message
    }


def build_subscribe_message() -> str:
    # Coinbase expects subscribe within 5 seconds
    subscribe = {
        "type": "subscribe",
        "product_ids": PRODUCT_IDS,
        "channels": CHANNELS,
    }
    return json.dumps(subscribe)


async def create_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        enable_idempotence=True,   # helps with duplicates on retries
        linger_ms=10,
        #retries=10,
    )
    await producer.start()
    return producer


async def ws_consume_and_publish(producer: AIOKafkaProducer) -> None:
    seq_tracker = SeqTracker()
    subscribe_message = build_subscribe_message()

    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=PING_INTERVAL,
                close_timeout=5,
                max_queue=1000,
            ) as ws:
                print(f"[ws] connected to {WS_URL}")
                await ws.send(subscribe_message)
                print(f"[ws] sent subscription: {subscribe_message}")
                
                # Check subscription response
                first_msg = await ws.recv()
                print(f"[ws] subscription response: {first_msg}")

                backoff = 1.0  # reset after successful connect
                msg_count = 0

                while True:
                    raw = await ws.recv()
                    msg = json.loads(raw)

                    msg_count += 1
                    if msg_count % 10 == 0:
                        print(f"[ws] received {msg_count} messages, latest type: {msg.get('type')}")

                    # Ignore non-JSON or unexpected types gracefully
                    product_id = msg.get("product_id")
                    seq = msg.get("sequence")

                    gap = False
                    if product_id is not None and isinstance(seq, int):
                        gap = seq_tracker.observe(product_id, seq)

                    event = normalize_message(msg, gap_detected=gap)

                    # Key by product_id for partitioning; fallback to type if missing
                    key = (event.get("product_id") or event.get("type") or "unknown").encode("utf-8")
                    value = json.dumps(event, separators=(",", ":")).encode("utf-8")

                    # Fire-and-forget is faster, but await gives backpressure safety
                    await producer.send_and_wait(KAFKA_TOPIC, key=key, value=value)

                    if msg_count == 1:
                        print(f"[kafka] first message sent successfully to {KAFKA_TOPIC}")

        except (websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                OSError) as e:
            # Transient network / server close -> reconnect with jittered backoff
            sleep_s = min(MAX_BACKOFF_S, backoff) + random.random()
            print(f"[ws] disconnected ({type(e).__name__}: {e}); retrying in {sleep_s:.1f}s")
            await asyncio.sleep(sleep_s)
            backoff = min(MAX_BACKOFF_S, backoff * 2)

        except Exception as e:
            # Unexpected: log and keep going (don’t crash the pipeline)
            sleep_s = min(MAX_BACKOFF_S, backoff) + random.random()
            print(f"[fatal] {type(e).__name__}: {e}; retrying in {sleep_s:.1f}s")
            await asyncio.sleep(sleep_s)
            backoff = min(MAX_BACKOFF_S, backoff * 2)


async def main() -> None:
    await asyncio.sleep(5)
    producer = await create_producer()
    try:
        print(f"[kafka] producing to {KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC}")
        print(f"[cfg] products={PRODUCT_IDS} channels={CHANNELS}")
        await ws_consume_and_publish(producer)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())