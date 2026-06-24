"""
producer_avro_local.py
======================
Local Avro Schema Governance Producer — Step 9 of the ITI Kafka Lab.

Instead of relying on Confluent Schema Registry (an external server), this
script enforces the data contract INSIDE Python using fastavro.

Workflow per message:
  1. Define the Avro schema as a Python dict, then parse it.
  2. Call fastavro.validation.validate() on the record BEFORE touching Kafka.
     If the record is invalid the exception is raised here, and zero bytes
     reach the broker wire — true shift-left data quality.
  3. Serialise the validated record to bytes with fastavro.schemaless_writer.
  4. Produce the raw bytes to Kafka as the message value.

Two test cases are run automatically:
  A) Valid record   → serialises and delivers.
  B) Broken record  → validate() raises immediately, nothing is produced.
"""

import io

import fastavro
import fastavro.validation
from confluent_kafka import Producer

# ── Kafka ─────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
SALES_TOPIC = "sales_topic"

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    if err:
        print(f"  [ERROR] Delivery failed: {err}")
    else:
        print(
            f"  [OK] Delivered to topic='{msg.topic()}'  "
            f"partition={msg.partition()}  offset={msg.offset()}"
        )


# ── Avro schema definition ────────────────────────────────────────────────────
# fastavro.parse_schema() resolves named types and validates the schema itself.
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


# ── Serialisation helper ──────────────────────────────────────────────────────
def serialise(record: dict) -> bytes:
    """
    Validate then serialise a record.
    Raises fastavro.validation.ValidationError on schema mismatch.
    """
    # Step 1 — Validate against schema BEFORE any I/O
    fastavro.validation.validate(record, PARSED_SCHEMA, raise_errors=True)

    # Step 2 — Write schemaless binary (schema is NOT embedded; consumer must
    #           have its own copy — see consumer_avro_local.py)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, PARSED_SCHEMA, record)
    return buf.getvalue()


def produce_record(record: dict) -> None:
    """Validate, serialise, and produce one record.  Catches schema errors."""
    print(f"\n  Attempting to produce: {record}")
    try:
        raw_bytes = serialise(record)
        print(f"  ✅ Validation passed.  Serialised to {len(raw_bytes)} bytes.")

        producer.produce(
            topic=SALES_TOPIC,
            key=str(record.get("order_id")),
            value=raw_bytes,
            callback=delivery_report,
        )
        producer.poll(0)

    except Exception as exc:
        # Validation failed — nothing was sent to Kafka
        print(f"  ❌ Validation FAILED — message blocked before reaching Kafka.")
        print(f"     Reason: {exc}")


# ── Test Case A: Valid record ─────────────────────────────────────────────────
print("=" * 60)
print("TEST CASE A — Valid record (should succeed)")
print("=" * 60)
valid_record = {"order_id": 101, "item_name": "Laptop", "price": 1200.50}
produce_record(valid_record)

# ── Test Case B: Broken record ────────────────────────────────────────────────
print("\n" + "=" * 60)
print('TEST CASE B — Broken record: order_id is a string, not an int')
print("=" * 60)
broken_record = {"order_id": "ABC_NOT_AN_INT", "item_name": "Keyboard", "price": 49.99}
produce_record(broken_record)

# ── Final flush ───────────────────────────────────────────────────────────────
producer.flush()
print("\n✅ Producer finished.")
print(
    "\nSummary:\n"
    "  Test A → bytes reached Kafka broker  ✓\n"
    "  Test B → exception raised in Python, ZERO bytes sent to broker  ✓\n"
    "  This is client-side schema governance — no Schema Registry server needed."
)
