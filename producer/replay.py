"""Replay a StatsBomb match as a live Kafka event stream.

The timing logic is kept as pure functions (no Kafka, no sleeping) so it can be
unit-tested deterministically. The Kafka side is a thin wrapper around them.

Replay model
------------
StatsBomb `minute` is cumulative across periods (P2 starts at 45, extra time at
90/105), so absolute match-seconds = minute * 60 + second is monotonic. We sleep
the real-time gap between consecutive events, scaled by REPLAY_SPEED and capped
by MAX_SLEEP_SECONDS so half-time gaps don't stall the stream.
"""
import json
import logging
import time
from typing import Any, Iterator

from producer.config import (
    KAFKA_BROKER,
    MATCH_TOPIC,
    MAX_SLEEP_SECONDS,
    REPLAY_SPEED,
)

logger = logging.getLogger(__name__)


# ---- Pure logic (unit-testable, no I/O) --------------------------------

def match_seconds(event: dict[str, Any]) -> int:
    """Absolute match time of an event in seconds (monotonic across periods)."""
    return event.get("minute", 0) * 60 + event.get("second", 0)


def order_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return events in true chronological order.

    StatsBomb `index` is the authoritative within-match sequence; sorting by it
    is more robust than sorting by (minute, second), which can tie.
    """
    return sorted(events, key=lambda e: e.get("index", 0))


def replay_delays(
    events: list[dict[str, Any]],
    speed: float = REPLAY_SPEED,
    max_sleep: float = MAX_SLEEP_SECONDS,
) -> Iterator[float]:
    """Yield the real-time sleep (seconds) to wait *before* each event.

    The first event has zero delay; each subsequent delay is the match-time gap
    from the previous event divided by `speed`, clamped to [0, max_sleep].
    """
    prev = None
    for event in events:
        cur = match_seconds(event)
        if prev is None:
            yield 0.0
        else:
            gap = max(0, cur - prev) / speed
            yield min(gap, max_sleep)
        prev = cur


def to_payload(event: dict[str, Any]) -> dict[str, Any]:
    """Bronze payload — emit the raw event untouched (schema-on-read downstream)."""
    return event


# ---- Kafka side (thin wrapper) -----------------------------------------

def build_producer(broker: str = KAFKA_BROKER):
    """Create a confluent-kafka Producer. Imported lazily so tests need no broker."""
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": broker,
        "client.id": "pitchflow-replay",
        "linger.ms": 5,
    })


def stream_match(
    events: list[dict[str, Any]],
    producer=None,
    topic: str = MATCH_TOPIC,
    speed: float = REPLAY_SPEED,
    sleep_fn=time.sleep,
) -> int:
    """Replay events to Kafka in match order. Returns the number emitted.

    `producer` and `sleep_fn` are injectable so tests can run without a broker
    or real waiting.
    """
    if producer is None:
        producer = build_producer()

    ordered = order_events(events)
    delays = replay_delays(ordered, speed=speed)
    emitted = 0

    for event, delay in zip(ordered, delays):
        if delay > 0:
            sleep_fn(delay)

        key = str(event.get("possession_team", {}).get("name", "unknown"))
        producer.produce(
            topic,
            key=key,
            value=json.dumps(to_payload(event)),
        )
        producer.poll(0)
        emitted += 1

        if emitted % 200 == 0:
            minute = event.get("minute", 0)
            logger.info("Replayed %d events (match minute %d)", emitted, minute)

    producer.flush()
    logger.info("Replay complete — %d events emitted to '%s'", emitted, topic)
    return emitted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from producer.config import MATCH_ID
    from producer.download import download_match

    match_events = download_match(MATCH_ID)
    stream_match(match_events)
