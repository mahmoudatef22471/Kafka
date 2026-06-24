"""
consumer_partition.py
=====================
Partition-Isolated Consumer — Step 5 of the ITI Kafka Lab.

This script mimics an ML scoring engine that:
  1. Manually pins itself to Partition 0 (Cairo transactions only).
  2. Applies a simple fraud rule: amount > 100 000 → forward to topic_fraud.

Key Kafka concept illustrated here:
  Instead of consumer.subscribe() (which lets Kafka assign partitions via
  group rebalancing), we use consumer.assign() with an explicit TopicPartition
  object.  The consumer then ONLY receives messages from that one partition,
  regardless of how many partitions the topic has or how many consumers are
  running in the same group.
"""

import json

from confluent_kafka import Consumer, Producer, TopicPartition

# ── Kafka settings ────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
SOURCE_TOPIC = "topic_raw"
FRAUD_TOPIC   = "topic_fraud"
PINNED_PARTITION = 0          # Cairo partition only

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "fraud-detection-group",
        # earliest → start from the very first message if no committed offset
        "auto.offset.reset": "earliest",
    }
)

fraud_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


# ── Delivery callback for fraud alerts ───────────────────────────────────────
def fraud_delivery_report(err, msg):
    if err:
        print(f"[FRAUD-ERROR] Could not forward alert: {err}")
    else:
        print(f"[FRAUD-FORWARDED] offset={msg.offset()}  partition={msg.partition()}")


# ── Fraud rule ────────────────────────────────────────────────────────────────
FRAUD_THRESHOLD = 100_000.0


def is_fraud(record: dict) -> bool:
    return record.get("amount", 0) > FRAUD_THRESHOLD


# ── Manual partition assignment ───────────────────────────────────────────────
# TopicPartition(topic, partition)  — no offset means "start from committed /
# auto.offset.reset" which we set to "earliest" above.
tp = TopicPartition(SOURCE_TOPIC, PINNED_PARTITION)
consumer.assign([tp])

print(
    f"Consumer pinned to  topic='{SOURCE_TOPIC}'  partition={PINNED_PARTITION}\n"
    f"Fraud threshold: amount > {FRAUD_THRESHOLD:,.0f}\n"
    f"Listening … (Ctrl-C to stop)\n"
)

# ── Poll loop ─────────────────────────────────────────────────────────────────
try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            # No message arrived within the timeout — keep waiting
            continue

        if msg.error():
            print(f"[ERROR] {msg.error()}")
            continue

        # Deserialise JSON payload
        record = json.loads(msg.value().decode("utf-8"))

        print(
            f"[RECEIVED] tx={record['transaction_id'][:8]}…  "
            f"user={record['user_id']}  "
            f"amount={record['amount']:>10.2f}  "
            f"location={record['location']}"
        )

        # Apply fraud rule
        if is_fraud(record):
            print(f"  ⚠️  FRAUD ALERT — forwarding to '{FRAUD_TOPIC}'")
            fraud_producer.produce(
                topic=FRAUD_TOPIC,
                key=record["transaction_id"],
                value=json.dumps(record),
                callback=fraud_delivery_report,
            )
            fraud_producer.poll(0)

except KeyboardInterrupt:
    print("\nShutting down consumer …")

finally:
    consumer.close()
    fraud_producer.flush()
    print("Consumer closed.  All fraud alerts flushed.")
