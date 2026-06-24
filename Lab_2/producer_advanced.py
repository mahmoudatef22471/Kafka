"""
producer_advanced.py
====================
Partition-Targeted Producer — Step 4 of the ITI Kafka Lab.

Business Rule (Geo-Based Routing):
  - location == "Cairo"       → Partition 0
  - location == "Alexandria"  → Partition 1

Each message follows the topic_raw JSON schema:
  { transaction_id: str, user_id: int, amount: float, location: str }

The key insight: by manually specifying the partition number in producer.produce(),
we bypass Kafka's default hash-based partitioner and guarantee that every city's
transactions land on a dedicated, isolated partition.  This lets a downstream
consumer pin itself to one partition and process only one city's data.
"""

import json
import random
import time
import uuid

from confluent_kafka import Producer

# ── Kafka connection ──────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
TOPIC = "topic_raw"

# Partition routing table — business rule encoded as a plain dict
PARTITION_MAP = {
    "Cairo": 0,
    "Alexandria": 1,
}

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


# ── Delivery callback ─────────────────────────────────────────────────────────
def delivery_report(err, msg):
    """
    Called asynchronously by librdkafka once the broker acknowledges (or rejects)
    each message.  Printing the partition here confirms our routing logic worked.
    """
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(
            f"[OK] Delivered → topic={msg.topic()}  "
            f"partition={msg.partition()}  offset={msg.offset()}"
        )


# ── Message factory ───────────────────────────────────────────────────────────
def make_transaction(location: str) -> dict:
    """
    Generate a synthetic transaction record that conforms to the topic_raw schema.
    Amounts are intentionally skewed so that some Cairo transactions exceed the
    100 000 fraud threshold used by the downstream consumer.
    """
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        # ~30 % chance of a "suspicious" high-value Cairo transaction
        "amount": round(random.uniform(500, 200_000), 2),
        "location": location,
    }


# ── Main loop ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    locations = list(PARTITION_MAP.keys())   # ["Cairo", "Alexandria"]
    print(f"Starting producer → topic '{TOPIC}'.  Press Ctrl-C to stop.\n")

    try:
        for i in range(1, 21):               # send 20 messages then stop
            location = random.choice(locations)
            record = make_transaction(location)
            partition = PARTITION_MAP[location]

            producer.produce(
                topic=TOPIC,
                key=record["transaction_id"],   # key is stored for traceability
                value=json.dumps(record),       # JSON-serialised payload
                partition=partition,            # ← explicit partition assignment
                callback=delivery_report,
            )

            print(
                f"[SEND #{i:02d}] location={location:<12} "
                f"amount={record['amount']:>10.2f}  → partition {partition}"
            )

            # poll() lets the client process delivery callbacks without blocking
            producer.poll(0)
            time.sleep(0.3)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    finally:
        # flush() blocks until all queued messages are delivered or time out
        print("\nFlushing remaining messages …")
        producer.flush()
        print("Done.")
