"""
consumer_avro_local.py
======================
Local Avro Consumer — Step 10 of the ITI Kafka Lab.

Kafka brokers are schema-agnostic: they store and return raw bytes without
knowing or caring what encoding was used.  The consumer therefore must:
  1. Know the schema independently (local copy, same definition as the producer).
  2. Wrap the raw bytes in an io.BytesIO stream.
  3. Pass both to fastavro.schemaless_reader to decode back into a Python dict.

This "bring your own schema" approach is the lightweight alternative to
Confluent Schema Registry — both sides agree on the contract out-of-band
(e.g. via a shared library or version-controlled schema file).
"""

import io

import fastavro
from confluent_kafka import Consumer

# ── Kafka ─────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
SALES_TOPIC = "sales_topic"

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "avro-consumer-group",
        "auto.offset.reset": "earliest",   # replay all messages from the topic
    }
)
consumer.subscribe([SALES_TOPIC])

# ── Same Avro schema the producer used ───────────────────────────────────────
# Both sides MUST use an identical schema; any mismatch causes a decode error.
RAW_SCHEMA = {
    "type": "record",
    "name": "SaleOrder",
    "fields": [
        {"name": "order_id",   "type": "int"},
        {"name": "item_name",  "type": "string"},
        {"name": "price",      "type": "float"},
    ],
}
PARSED_SCHEMA = fastavro.parse_schema(RAW_SCHEMA)


# ── Deserialisation helper ────────────────────────────────────────────────────
def deserialise(raw_bytes: bytes) -> dict:
    """
    Decode schemaless Avro bytes back into a Python dictionary.
    schemaless_reader does NOT expect the 5-byte magic + schema-id prefix
    used by Confluent Schema Registry — matching how the producer wrote them.
    """
    buf = io.BytesIO(raw_bytes)
    return fastavro.schemaless_reader(buf, PARSED_SCHEMA)


# ── Poll loop ─────────────────────────────────────────────────────────────────
print(f"Avro consumer subscribed to '{SALES_TOPIC}' (Ctrl-C to stop) …\n")

try:
    while True:
        msg = consumer.poll(timeout=2.0)

        if msg is None:
            print("  … waiting for messages")
            continue

        if msg.error():
            print(f"[ERROR] {msg.error()}")
            continue

        raw_bytes = msg.value()

        try:
            record = deserialise(raw_bytes)
            print(
                f"[DECODED]  offset={msg.offset()}  "
                f"order_id={record['order_id']}  "
                f"item='{record['item_name']}'  "
                f"price={record['price']:.2f}"
            )
        except Exception as exc:
            # Could happen if the message was produced with a different schema
            # or is not Avro at all (e.g. Test Case B's blocked record never
            # reached Kafka, so this should not occur in normal operation).
            print(f"[DECODE ERROR]  offset={msg.offset()}  reason={exc}")
            print(f"  raw bytes (hex): {raw_bytes.hex()[:60]}…")

except KeyboardInterrupt:
    print("\nShutting down consumer …")

finally:
    consumer.close()
    print("Consumer closed.")
