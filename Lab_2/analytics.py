"""
analytics.py
============
Serverless DuckDB Analytics — Step 8 of the ITI Kafka Lab.

DuckDB reads Parquet files DIRECTLY from disk using a glob pattern — no data
loading, no import step, no database server.  This is the "lakehouse" pattern:
  raw files on storage  +  in-process SQL engine  =  instant analytics.

Why DuckDB over Spark / Pandas here?
  - Zero infrastructure — single pip install, runs in-process.
  - Columnar pushdown — only the requested columns are read from Parquet.
  - Predicate pushdown — WHERE filters are applied at file-scan time.
  - Result: sub-second queries on millions of rows on a laptop.
"""

import os

import duckdb

PARQUET_GLOB = "data_lake/*.parquet"   # matches all sink files written by consumer_to_parquet.py

# ── Connect (in-memory, ephemeral) ────────────────────────────────────────────
con = duckdb.connect()

# ── Helper to pretty-print query results ─────────────────────────────────────
def run(title: str, sql: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")
    result = con.execute(sql).fetchdf()
    if result.empty:
        print("  (no rows)")
    else:
        print(result.to_string(index=False))


# ── Sanity check: make sure files exist ──────────────────────────────────────
import glob
files = glob.glob(PARQUET_GLOB)
if not files:
    print(
        f"⚠️  No Parquet files found in data_lake/\n"
        f"   Run consumer_to_parquet.py first, then producer_advanced.py + "
        f"consumer_partition.py to generate fraud alerts."
    )
    raise SystemExit(1)

print(f"✅ Found {len(files)} Parquet file(s): {[os.path.basename(f) for f in files]}")

# ── Query 1: Preview all fraud records ───────────────────────────────────────
run(
    "Q1 — All fraud records",
    f"SELECT * FROM read_parquet('{PARQUET_GLOB}') ORDER BY amount DESC",
)

# ── Query 2: Total & average fraudulent amount ────────────────────────────────
run(
    "Q2 — Aggregate stats",
    f"""
    SELECT
        COUNT(*)                          AS total_fraud_txns,
        ROUND(SUM(amount), 2)             AS total_amount,
        ROUND(AVG(amount), 2)             AS avg_amount,
        ROUND(MAX(amount), 2)             AS max_amount
    FROM read_parquet('{PARQUET_GLOB}')
    """,
)

# ── Query 3: Fraud count per location ────────────────────────────────────────
run(
    "Q3 — Fraud count by location",
    f"""
    SELECT
        location,
        COUNT(*)          AS fraud_count,
        ROUND(SUM(amount), 2) AS total_amount
    FROM read_parquet('{PARQUET_GLOB}')
    GROUP BY location
    ORDER BY fraud_count DESC
    """,
)

# ── Query 4: Top 5 highest-value fraud transactions ──────────────────────────
run(
    "Q4 — Top 5 highest-value fraud transactions",
    f"""
    SELECT transaction_id, user_id, amount, location
    FROM read_parquet('{PARQUET_GLOB}')
    ORDER BY amount DESC
    LIMIT 5
    """,
)

# ── Query 5: Users with more than 1 fraud alert ───────────────────────────────
run(
    "Q5 — Repeat offenders (users with 2+ fraud alerts)",
    f"""
    SELECT user_id, COUNT(*) AS alert_count, ROUND(SUM(amount), 2) AS total
    FROM read_parquet('{PARQUET_GLOB}')
    GROUP BY user_id
    HAVING COUNT(*) > 1
    ORDER BY alert_count DESC
    """,
)

con.close()
print("\n✅ Analytics complete.")
