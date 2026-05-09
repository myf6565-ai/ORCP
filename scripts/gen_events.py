#!/usr/bin/env python3
"""
ORCP load generator -- produces SourceEvent JSON records to a Kafka topic.

The schema matches com.orcp.common.dto.SourceEvent (Stage A) field-for-field:

    {
      "eventId":    "evt-0000000001",
      "bizType":    "ORDER",
      "bizKey":     "order-0000000001",
      "customerId": 37,
      "amount":     "128.3200",
      "eventTime":  "2026-05-09 10:00:00",
      "traceId":    "trace-abcdef1234567890"
    }

Typical usage:

    # Install deps once:
    pip install -r scripts/requirements.txt

    # Send 1000 events at ~500 msg/s to the external source topic:
    python scripts/gen_events.py \\
        --bootstrap-server node-1:9092,node-2:9092,node-3:9092 \\
        --topic orcp.src.demo \\
        --count 1000 --rate 500

    # Inject 5% duplicates to exercise orcp-ingest dedup:
    python scripts/gen_events.py --count 200 --duplicate-rate 0.05

    # Send continuously until Ctrl-C:
    python scripts/gen_events.py --count 0 --rate 100

Deterministic-by-default: a fixed --seed means two runs produce the same
event stream (modulo timestamps). Override with --seed 0 for entropy.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import string
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, NoBrokersAvailable
except ImportError:
    sys.stderr.write(
        "kafka-python is required. Install with:\n"
        "    pip install -r scripts/requirements.txt\n"
    )
    sys.exit(2)

LOG = logging.getLogger("gen_events")

BIZ_TYPES = ("ORDER", "REFUND")
# REFUNDs are rare; keeps the mix realistic.
BIZ_TYPE_WEIGHTS = (95, 5)


# -----------------------------------------------------------------------------
# Event model
# -----------------------------------------------------------------------------
@dataclass
class SourceEvent:
    eventId: str
    bizType: str
    bizKey: str
    customerId: int
    amount: str            # quoted -- Flink DECIMAL(18,4) prefers string input
    eventTime: str         # "yyyy-MM-dd HH:mm:ss" Asia/Shanghai
    traceId: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")


def build_event(seq: int, rng: random.Random) -> SourceEvent:
    biz_type = rng.choices(BIZ_TYPES, weights=BIZ_TYPE_WEIGHTS, k=1)[0]
    # Round to 4 dp to match DECIMAL(18,4) in both MySQL and OceanBase.
    amount_dec = Decimal(rng.uniform(1.0, 999.99)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )
    if biz_type == "REFUND":
        amount_dec = -amount_dec
    return SourceEvent(
        eventId="evt-%010d" % seq,
        bizType=biz_type,
        bizKey="order-%010d" % seq,
        customerId=rng.randint(1, 50),  # matches 50 seeded customers in mysql_detail_schema.sql
        amount=str(amount_dec),
        eventTime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        traceId="trace-" + "".join(rng.choices(string.hexdigits.lower(), k=16)),
    )


# -----------------------------------------------------------------------------
# Producer plumbing
# -----------------------------------------------------------------------------
def build_producer(bootstrap: str, acks: str, linger_ms: int) -> KafkaProducer:
    LOG.info("Connecting to %s", bootstrap)
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            acks=acks,
            linger_ms=linger_ms,
            batch_size=16384,
            retries=5,
            # Mirror the idempotence flag set by orcp-ingest in Stage D's
            # application.yml; safe to enable on the load generator too.
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v,  # already bytes
            client_id="orcp-loadgen",
        )
    except NoBrokersAvailable as e:
        LOG.error("Could not reach any broker at %s: %s", bootstrap, e)
        sys.exit(3)


class RateLimiter:
    """Minimal token-bucket pacing; no external deps."""

    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.0
        self._next = time.monotonic()

    def wait(self) -> None:
        if self._interval <= 0:
            return
        now = time.monotonic()
        if now < self._next:
            time.sleep(self._next - now)
        self._next += self._interval


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--bootstrap-server", "-b",
        default="node-1:9092,node-2:9092,node-3:9092",
        help="Kafka bootstrap servers (comma-separated).",
    )
    ap.add_argument(
        "--topic", "-t", default="orcp.src.demo",
        help="Target topic (default: orcp.src.demo, matches Stage C seed topic).",
    )
    ap.add_argument(
        "--count", "-n", type=int, default=1000,
        help="How many events to send. 0 means 'run until Ctrl-C'. Default 1000.",
    )
    ap.add_argument(
        "--rate", "-r", type=float, default=200.0,
        help="Target messages per second (0 = no limit). Default 200.",
    )
    ap.add_argument(
        "--duplicate-rate", type=float, default=0.0,
        help="Fraction in [0, 1] of events to emit TWICE, to exercise ingest dedup. Default 0.",
    )
    ap.add_argument(
        "--seed", type=int, default=42,
        help="RNG seed for reproducibility. Use 0 for entropy. Default 42.",
    )
    ap.add_argument(
        "--start-seq", type=int, default=1,
        help="First event sequence number (useful when continuing a prior run). Default 1.",
    )
    ap.add_argument(
        "--acks", choices=("0", "1", "all"), default="all",
        help="Kafka producer acks setting. Default all.",
    )
    ap.add_argument(
        "--linger-ms", type=int, default=5,
        help="Producer linger.ms for batching. Default 5.",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Print events to stdout instead of sending (does not open a Kafka connection).",
    )
    ap.add_argument(
        "--verbose", "-v", action="store_true",
        help="DEBUG-level logging.",
    )
    return ap.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
    )

    seed = args.seed if args.seed != 0 else None
    rng = random.Random(seed)

    if not (0.0 <= args.duplicate_rate <= 1.0):
        LOG.error("--duplicate-rate must be within [0, 1]")
        return 2

    producer: Optional[KafkaProducer] = None
    if not args.dry_run:
        producer = build_producer(args.bootstrap_server, args.acks, args.linger_ms)

    stopped = {"v": False}

    def _stop(_sig, _frame):
        stopped["v"] = True
        LOG.info("Stop signal received, flushing producer...")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    limiter = RateLimiter(args.rate)
    sent = 0
    dup_sent = 0
    failed = 0
    started = time.monotonic()
    seq = args.start_seq

    try:
        while not stopped["v"]:
            # 'sent' is the count of unique events; 'dup_sent' is extra duplicates
            # injected on top. We stop once we have emitted --count uniques.
            if args.count > 0 and sent >= args.count:
                break
            limiter.wait()

            evt = build_event(seq, rng)
            seq += 1
            payload = evt.to_json()
            # Decide duplication up front so dry-run and live runs behave the same.
            inject_dup = (
                args.duplicate_rate > 0 and rng.random() < args.duplicate_rate
            )

            if args.dry_run:
                print(payload.decode("utf-8"))
                sent += 1
                if inject_dup:
                    print(payload.decode("utf-8"))
                    dup_sent += 1
            else:
                assert producer is not None

                def _on_err(exc, _evt_id=evt.eventId):
                    # Closure over evt.eventId only; don't capture the full event.
                    nonlocal failed
                    failed += 1
                    LOG.error("send failed event_id=%s err=%s", _evt_id, exc)

                # Key on bizKey so the same order always lands on one partition.
                future = producer.send(
                    args.topic,
                    key=evt.bizKey,
                    value=payload,
                )
                future.add_errback(_on_err)
                sent += 1

                # Duplicate injection -- emit the same event again with the same key.
                if inject_dup:
                    producer.send(args.topic, key=evt.bizKey, value=payload) \
                        .add_errback(_on_err)
                    dup_sent += 1

            if sent % 1000 == 0:
                elapsed = time.monotonic() - started
                rate = sent / elapsed if elapsed > 0 else 0.0
                LOG.info(
                    "sent=%d dup=%d failed=%d elapsed=%.1fs rate=%.0f msg/s",
                    sent, dup_sent, failed, elapsed, rate,
                )
    finally:
        if producer is not None:
            LOG.info("Flushing producer...")
            producer.flush(timeout=30)
            producer.close(timeout=10)

    elapsed = time.monotonic() - started
    rate = sent / elapsed if elapsed > 0 else 0.0
    LOG.info(
        "done. total sent=%d (duplicates=%d), failed=%d, elapsed=%.2fs, rate=%.1f msg/s",
        sent, dup_sent, failed, elapsed, rate,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run(_parse_args()))
