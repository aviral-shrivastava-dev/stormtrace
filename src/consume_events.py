"""Consume StormTrace pipeline events from the Redpanda stream.

    python src/consume_events.py --from-beginning   # read the whole log
    python src/consume_events.py                     # tail live events

The consumer prints one formatted line per event and keeps a small summary
(step counts, failures). Two modes mirror Kafka's offset reset semantics:

- --from-beginning uses a fixed consumer group and earliest offset, so it
  replays the retained history; re-running it does not re-read messages the
  group has already committed.
- live mode uses a fresh group id and latest offset, printing only what
  arrives while it runs.

Press Ctrl+C to stop live mode.

Exits with code 2 when the broker is unreachable.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer
from confluent_kafka.error import KafkaException

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "stormtrace.pipeline.events"
POLL_TIMEOUT = 1.0
IDLE_LIMIT_SECONDS = 5


def format_event(event: dict[str, object]) -> str:
    started = str(event.get("started_at_utc", ""))[:19]
    step = str(event.get("step", "?"))
    status = str(event.get("status", "?"))
    duration = event.get("duration_seconds")
    detail = f" ({duration:.2f}s)" if isinstance(duration, (int, float)) else ""
    suffix = ""
    stderr = str(event.get("stderr") or "").strip()
    if status in ("failed", "skipped") and stderr:
        suffix = f" -- {stderr.splitlines()[0][:80]}"
    return f"{started}  {step:<28} {status:<8}{detail}{suffix}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="replay all retained events instead of tailing live ones",
    )
    args = parser.parse_args()

    if args.from_beginning:
        # A fresh group id with earliest offset reset makes every replay
        # deterministic: the full retained history is re-read each time,
        # regardless of what earlier replays consumed.
        group_id = f"stormtrace-replay-{uuid.uuid4().hex[:8]}"
        offset_reset = "earliest"
        auto_commit = False
    else:
        group_id = f"stormtrace-live-{uuid.uuid4().hex[:8]}"
        offset_reset = "latest"
        auto_commit = True

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
            "enable.auto.commit": auto_commit,
            "socket.keepalive.enable": True,
        }
    )
    try:
        consumer.subscribe([TOPIC])
    except KafkaException as error:
        print(
            f"Could not reach the Redpanda broker at {BOOTSTRAP}. "
            "Start it with: docker compose up -d",
            file=sys.stderr,
        )
        print(f"Detail: {error}", file=sys.stderr)
        return 2

    print(f"StormTrace event consumer ({'replay' if args.from_beginning else 'live'} mode)")
    print(f"Broker: {BOOTSTRAP}  topic: {TOPIC}")
    print("Ctrl+C to stop.")
    print()

    counts: Counter[str] = Counter()
    consumed = 0
    last_activity_at = datetime.now()
    # In replay mode the topic may legitimately be empty; wait longer
    # before giving up so a slow broker start is not mistaken for EOF.
    initial_wait = 20 if args.from_beginning else 5
    try:
        while True:
            message = consumer.poll(POLL_TIMEOUT)
            if message is None:
                idle_seconds = (datetime.now() - last_activity_at).total_seconds()
                if consumed > 0 and idle_seconds > IDLE_LIMIT_SECONDS:
                    break
                if consumed == 0 and idle_seconds > initial_wait:
                    break
                continue
            if message.error():
                continue

            event = json.loads(message.value().decode("utf-8"))
            print(format_event(event))
            counts[str(event.get("status", "?"))] += 1
            consumed += 1
            last_activity_at = datetime.now()
    except KeyboardInterrupt:
        print()
    finally:
        consumer.close()

    print()
    print(f"Events consumed: {consumed:,}")
    for status, count in sorted(counts.items()):
        print(f"  {status}: {count:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
