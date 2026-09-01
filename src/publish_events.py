"""Publish pipeline events from the JSONL log to the Redpanda stream.

The pipeline's source of truth for run history is the JSONL log file. This
publisher replays those events onto a Kafka-compatible topic so that other
systems (dashboards, alerting, downstream processors) can subscribe without
reading the laptop's filesystem.

    python src/publish_events.py            # publish the latest completed run
    python src/publish_events.py --all      # publish the entire log

Events are keyed by run_id, so every event of one run lands in the same
partition and consumers see each run in order. The message value is the
event's JSON, unchanged.

Exits with code 2 when the broker is unreachable, which the orchestrator
logs as a skip: streaming is optional infrastructure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.error import KafkaError, KafkaException

ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = ROOT / "data" / "logs" / "pipeline_runs.jsonl"

BOOTSTRAP = os.environ.get("STORMTRACE_KAFKA_BOOTSTRAP", "127.0.0.1:9092")
TOPIC = os.environ.get("STORMTRACE_KAFKA_TOPIC", "stormtrace.pipeline.events")
DELIVERY_TIMEOUT = 15



def load_events(all_events: bool) -> list[dict[str, object]]:
    if not LOG_PATH.exists():
        return []
    events = [
        json.loads(line)
        for line in LOG_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if all_events:
        return events

    # Publish the most recently COMPLETED run: the last event whose step is
    # 'pipeline' carries that run's final status. When this script runs as a
    # pipeline step, the current run has no such event yet, so the previous
    # complete run is published -- including its final summary event.
    completed_runs = [
        event["run_id"] for event in events if event.get("step") == "pipeline"
    ]
    if not completed_runs:
        return []
    last_run_id = completed_runs[-1]
    return [event for event in events if event.get("run_id") == last_run_id]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--all", action="store_true", help="publish the entire log history"
    )
    args = parser.parse_args()

    events = load_events(args.all)
    if not events:
        print("No pipeline events found. Run the pipeline first.")
        return 0

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "delivery.timeout.ms": DELIVERY_TIMEOUT * 1000,
            "socket.keepalive.enable": True,
        }
    )

    delivered: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    def on_delivery(err: KafkaError | None, msg: object) -> None:
        if err is not None:
            failed.append((str(msg.key() or b""), err.str()))
        else:
            delivered.append((str(msg.key() or b""), msg.topic()))

    try:
        for event in events:
            run_id = str(event.get("run_id", ""))
            step = str(event.get("step", ""))
            payload = json.dumps(event, ensure_ascii=True)
            producer.produce(
                topic=TOPIC,
                key=run_id.encode("utf-8"),
                value=payload.encode("utf-8"),
                headers={"event_step": step.encode("utf-8")},
                on_delivery=on_delivery,
            )
            producer.poll(0)
        remaining = producer.flush(DELIVERY_TIMEOUT)
    except KafkaException as error:
        print(
            f"Could not reach the Redpanda broker at {BOOTSTRAP}. "
            "Start it with: docker compose up -d",
            file=sys.stderr,
        )
        print(f"Detail: {error}", file=sys.stderr)
        return 2

    # flush() returns the number of messages still queued or in transit.
    # Undelivered messages mean the broker was unreachable (or too slow);
    # when nothing at all was delivered, report the tolerated skip code so
    # the pipeline logs this step as skipped rather than failed.
    if remaining > 0 or failed:
        delivered_count = len(delivered)
        if delivered_count == 0:
            print(
                f"Could not deliver events to {BOOTSTRAP} "
                f"({remaining + len(failed)} undelivered). "
                "Start the broker with: docker compose up -d",
                file=sys.stderr,
            )
            return 2
        print(
            f"Delivery incomplete: {len(failed) + remaining} of "
            f"{len(events)} events undelivered.",
            file=sys.stderr,
        )
        return 1

    print("StormTrace event publisher")
    print(f"Broker:  {BOOTSTRAP}")
    print(f"Topic:   {TOPIC}")
    print(f"Published: {len(delivered):,} events "
          f"({len({key for key, _ in delivered})} run(s))")
    print("Consume them with: python src/consume_events.py --from-beginning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
