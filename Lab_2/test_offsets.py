"""
test_offsets.py
===============
Offset Strategy Experiment — Step 6 of the ITI Kafka Lab.

Demonstrates the difference between two auto.offset.reset values:

  "latest"   → Consumer starts from the END of the log.
               It will only see messages produced AFTER it starts.
               Use-case: live monitoring — you don't care about history.

  "earliest" → Consumer starts from the BEGINNING of the log.
               It replays every message ever written to the partition.
               Use-case: reprocessing, auditing, backfill pipelines.

We run BOTH strategies in sequence, each consuming for a few seconds, so you
can directly compare what each one reads.

NOTE: Kafka only honours auto.offset.reset when the consumer group has NO
committed offset yet.  To force a fresh replay, each run below uses a
UNIQUE group.id so Kafka treats it as a brand-new consumer.
"""

import json
import time

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
SOURCE_TOPIC      = "topic_raw"
POLL_DURATION_SEC = 5          # how long to poll per strategy


def run_consumer(strategy: str, partition: int = 0) -> None:
    """
    Create a temporary consumer with the given offset strategy, poll for
    POLL_DURATION_SEC seconds, then close.
    """
    # Unique group so no committed offsets interfere
    group_id = f"offset-test-{strategy}-{int(time.time())}"

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": strategy,      # ← the key knob
            "enable.auto.commit": False,        # don't commit — purely exploratory
        }
    )

    # Pin to partition 0 (Cairo data) so we have a concrete, bounded dataset
    tp = TopicPartition(SOURCE_TOPIC, partition)
    consumer.assign([tp])

    print(f"\n{'='*60}")
    print(f"  Strategy : {strategy.upper()}")
    print(f"  Group ID : {group_id}")
    print(f"  Partition: {partition}")
    print(f"  Polling for {POLL_DURATION_SEC} seconds …")
    print(f"{'='*60}")

    deadline = time.time() + POLL_DURATION_SEC
    count = 0

    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            print("  … (no message)")
            continue

        if msg.error():
            print(f"  [ERROR] {msg.error()}")
            continue

        record = json.loads(msg.value().decode("utf-8"))
        count += 1
        print(
            f"  [MSG #{count}]  offset={msg.offset()}  "
            f"tx={record['transaction_id'][:8]}…  "
            f"amount={record['amount']:>10.2f}"
        )

    consumer.close()
    print(f"\n  → Total messages received with '{strategy}': {count}")


# ── Run both experiments back-to-back ─────────────────────────────────────────
if __name__ == "__main__":
    print("\n🔬 OFFSET STRATEGY COMPARISON TEST")
    print("Make sure producer_advanced.py has already sent some messages.\n")

    # 1. LATEST — should receive 0 historical messages
    run_consumer("latest")

    time.sleep(1)   # brief pause so timestamps don't collide in group IDs

    # 2. EARLIEST — should replay ALL historical messages from offset 0
    run_consumer("earliest")

    print("\n✅ Experiment complete.")
    print(
        "\nConclusion:\n"
        "  'latest'   → you only see NEW messages produced AFTER the consumer started.\n"
        "  'earliest' → you replay the ENTIRE partition history from offset 0."
    )
