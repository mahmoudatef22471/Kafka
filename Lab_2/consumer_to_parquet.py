"""
consumer_to_parquet.py
======================
Parquet Lakehouse Sink — Step 7 of the ITI Kafka Lab.

This consumer:
  1. Subscribes to topic_fraud (fraud alerts forwarded by consumer_partition.py).
  2. Accumulates records in memory in a Python list.
  3. Every BATCH_SIZE records (or on shutdown), flushes the batch to a timestamped
     Parquet file inside the data_lake/ directory.

Why Parquet?
  - Columnar storage → massive speedup for analytical queries (DuckDB reads only
    the columns it needs).
  - Built-in compression (snappy by default in PyArrow).
  - Schema is embedded in the file — self-describing, no external catalogue needed.

Why batch writes instead of one-record-at-a-time?
  Writing a Parquet file has non-trivial overhead (header, footer, row-group
  metadata).  Batching amortises that overhead across many rows, producing larger,
  more efficient files that DuckDB can scan faster.
"""

import json
import os
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
FRAUD_TOPIC       = "topic_fraud"
OUTPUT_DIR        = "data_lake"        # directory where Parquet files land
BATCH_SIZE        = 5                  # flush every N records
POLL_TIMEOUT      = 2.0                # seconds to wait for a message

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "parquet-sink-group",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([FRAUD_TOPIC])

# ── PyArrow schema — mirrors the topic_raw / topic_fraud JSON schema ──────────
ARROW_SCHEMA = pa.schema(
    [
        pa.field("transaction_id", pa.string()),
        pa.field("user_id",        pa.int64()),
        pa.field("amount",         pa.float64()),
        pa.field("location",       pa.string()),
    ]
)


# ── Flush helper ──────────────────────────────────────────────────────────────
def flush_to_parquet(batch: list[dict]) -> None:
    """
    Convert a list of record dicts → Pandas DataFrame → PyArrow Table → Parquet.
    The filename encodes the current epoch so files are naturally time-ordered.
    """
    if not batch:
        return

    df = pd.DataFrame(batch)
    table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False)

    filename = os.path.join(OUTPUT_DIR, f"fraud_{int(time.time())}.parquet")
    pq.write_table(table, filename, compression="snappy")

    print(f"\n  💾 Flushed {len(batch)} records → {filename}")
    print(f"     Columns : {table.column_names}")
    print(f"     Rows    : {table.num_rows}\n")


# ── Main sink loop ─────────────────────────────────────────────────────────────
print(f"Parquet sink listening on '{FRAUD_TOPIC}' …")
print(f"Batch size: {BATCH_SIZE}  |  Output: {OUTPUT_DIR}/\n")

buffer: list[dict] = []

try:
    while True:
        msg = consumer.poll(timeout=POLL_TIMEOUT)

        if msg is None:
            # No new message — if buffer has something, flush it on timeout
            if buffer:
                print(f"  (timeout with {len(buffer)} buffered records — flushing)")
                flush_to_parquet(buffer)
                buffer.clear()
            continue

        if msg.error():
            print(f"[ERROR] {msg.error()}")
            continue

        record = json.loads(msg.value().decode("utf-8"))
        buffer.append(record)

        print(
            f"[BUFFERED {len(buffer)}/{BATCH_SIZE}]  "
            f"tx={record['transaction_id'][:8]}…  "
            f"amount={record['amount']:>10.2f}"
        )

        # Flush once batch is full
        if len(buffer) >= BATCH_SIZE:
            flush_to_parquet(buffer)
            buffer.clear()

except KeyboardInterrupt:
    print("\nShutting down — flushing remaining buffer …")
    flush_to_parquet(buffer)

finally:
    consumer.close()
    print("Consumer closed.  Check data_lake/ for Parquet files.")
