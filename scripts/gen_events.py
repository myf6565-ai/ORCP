#!/usr/bin/env python3
"""
ORCP 发压生成器 —— 向 Kafka topic 生产 SourceEvent JSON 记录。

生成的消息 schema 与 com.orcp.common.dto.SourceEvent（阶段 A）字段完全对齐：

    {
      "eventId":    "evt-0000000001",
      "bizType":    "ORDER",
      "bizKey":     "order-0000000001",
      "customerId": 37,
      "amount":     "128.3200",
      "eventTime":  "2026-05-09 10:00:00",
      "traceId":    "trace-abcdef1234567890"
    }

典型用法：

    # 安装依赖（仅需一次）：
    pip install -r scripts/requirements.txt

    # 以 ~500 msg/s 向外部源 topic 发送 1000 条事件：
    python scripts/gen_events.py \\
        --bootstrap-server node-1:9092,node-2:9092,node-3:9092 \\
        --topic orcp.src.demo \\
        --count 1000 --rate 500

    # 注入 5% 重复消息以测试 orcp-ingest 去重逻辑：
    python scripts/gen_events.py --count 200 --duplicate-rate 0.05

    # 持续发送直到 Ctrl-C：
    python scripts/gen_events.py --count 0 --rate 100

默认确定性：固定 --seed 可使两次运行产生相同事件流（时间戳除外）。
使用 --seed 0 改为随机模式。
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
        "缺少 kafka-python，请先安装：\n"
        "    pip install -r scripts/requirements.txt\n"
    )
    sys.exit(2)

LOG = logging.getLogger("gen_events")

BIZ_TYPES = ("ORDER", "REFUND")
# 退款占少数，保持业务真实比例。
BIZ_TYPE_WEIGHTS = (95, 5)


# -----------------------------------------------------------------------------
# 事件模型
# -----------------------------------------------------------------------------
@dataclass
class SourceEvent:
    eventId: str
    bizType: str
    bizKey: str
    customerId: int
    amount: str            # 字符串形式 —— Flink DECIMAL(18,4) 解析字符串精度无损
    eventTime: str         # "yyyy-MM-dd HH:mm:ss" Asia/Shanghai
    traceId: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode("utf-8")


def build_event(seq: int, rng: random.Random) -> SourceEvent:
    biz_type = rng.choices(BIZ_TYPES, weights=BIZ_TYPE_WEIGHTS, k=1)[0]
    # 保留 4 位小数以匹配 MySQL 和 OceanBase 的 DECIMAL(18,4)。
    amount_dec = Decimal(rng.uniform(1.0, 999.99)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )
    if biz_type == "REFUND":
        amount_dec = -amount_dec
    return SourceEvent(
        eventId="evt-%010d" % seq,
        bizType=biz_type,
        bizKey="order-%010d" % seq,
        customerId=rng.randint(1, 50),  # 与 mysql_detail_schema.sql 中 50 条种子客户对应
        amount=str(amount_dec),
        eventTime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        traceId="trace-" + "".join(rng.choices(string.hexdigits.lower(), k=16)),
    )


# -----------------------------------------------------------------------------
# 生产者配置
# -----------------------------------------------------------------------------
def build_producer(bootstrap: str, acks: str, linger_ms: int) -> KafkaProducer:
    LOG.info("正在连接 %s", bootstrap)
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            acks=acks,
            linger_ms=linger_ms,
            batch_size=16384,
            retries=5,
            # 与阶段 D application.yml 中 orcp-ingest 的幂等配置保持一致；
            # 在发压脚本中同样安全。
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v,  # 已经是 bytes
            client_id="orcp-loadgen",
        )
    except NoBrokersAvailable as e:
        LOG.error("无法连接到 %s 的任意 broker：%s", bootstrap, e)
        sys.exit(3)


class RateLimiter:
    """最简令牌桶限速，无外部依赖。"""

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
# 主流程
# -----------------------------------------------------------------------------
def _parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--bootstrap-server", "-b",
        default="node-1:9092,node-2:9092,node-3:9092",
        help="Kafka bootstrap servers，逗号分隔。",
    )
    ap.add_argument(
        "--topic", "-t", default="orcp.src.demo",
        help="目标 topic（默认 orcp.src.demo，与阶段 C 种子 topic 一致）。",
    )
    ap.add_argument(
        "--count", "-n", type=int, default=1000,
        help="发送事件数。0 表示持续发送直到 Ctrl-C。默认 1000。",
    )
    ap.add_argument(
        "--rate", "-r", type=float, default=200.0,
        help="目标每秒消息数（0 = 不限速）。默认 200。",
    )
    ap.add_argument(
        "--duplicate-rate", type=float, default=0.0,
        help="重复注入比例（[0, 1]），每条事件额外发送一次，用于测试去重。默认 0。",
    )
    ap.add_argument(
        "--seed", type=int, default=42,
        help="随机数种子，用于可复现测试。0 表示使用随机熵。默认 42。",
    )
    ap.add_argument(
        "--start-seq", type=int, default=1,
        help="首条事件的序号（继续上次运行时使用）。默认 1。",
    )
    ap.add_argument(
        "--acks", choices=("0", "1", "all"), default="all",
        help="Kafka 生产者 acks 参数。默认 all。",
    )
    ap.add_argument(
        "--linger-ms", type=int, default=5,
        help="生产者批量等待时间（ms）。默认 5。",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="将事件打印到 stdout 而不发送（不打开 Kafka 连接）。",
    )
    ap.add_argument(
        "--verbose", "-v", action="store_true",
        help="DEBUG 级日志。",
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
        LOG.error("--duplicate-rate 必须在 [0, 1] 范围内")
        return 2

    producer: Optional[KafkaProducer] = None
    if not args.dry_run:
        producer = build_producer(args.bootstrap_server, args.acks, args.linger_ms)

    stopped = {"v": False}

    def _stop(_sig, _frame):
        stopped["v"] = True
        LOG.info("收到停止信号，正在 flush 生产者...")

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
            # sent 统计唯一事件数；dup_sent 统计额外注入的重复数。
            # 发送完 --count 个唯一事件后停止。
            if args.count > 0 and sent >= args.count:
                break
            limiter.wait()

            evt = build_event(seq, rng)
            seq += 1
            payload = evt.to_json()
            # 提前决定是否注入重复，使 dry-run 与实际发送行为一致。
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
                    # 仅捕获 evt.eventId，不持有完整事件对象引用。
                    nonlocal failed
                    failed += 1
                    LOG.error("发送失败 event_id=%s err=%s", _evt_id, exc)

                # 以 bizKey 为分区键，确保同一订单的事件落在同一分区（保序）。
                future = producer.send(
                    args.topic,
                    key=evt.bizKey,
                    value=payload,
                )
                future.add_errback(_on_err)
                sent += 1

                # 重复注入：以相同 key 再发一条，触发 orcp-ingest 去重逻辑。
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
            LOG.info("正在 flush 生产者...")
            producer.flush(timeout=30)
            producer.close(timeout=10)

    elapsed = time.monotonic() - started
    rate = sent / elapsed if elapsed > 0 else 0.0
    LOG.info(
        "完成。共发送=%d（重复=%d），失败=%d，耗时=%.2fs，速率=%.1f msg/s",
        sent, dup_sent, failed, elapsed, rate,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run(_parse_args()))
