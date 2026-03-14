#!/usr/bin/env python3
"""Export PostgreSQL distillation_pairs to Parquet files.

Reads all rows from distillation_pairs, converts to Parquet using pyarrow,
and writes sharded files in HuggingFace datasets compatible structure.
File pattern: train-00000-of-NNNNN.parquet
"""

from __future__ import annotations

import argparse
import math
import os
import sys

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq


TABLE_NAME = "distillation_pairs"

DEFAULT_OUTPUT_DIR = "/mnt/nfs/distillation"
DEFAULT_SHARD_SIZE = 50000

# Column order matches the table definition.
# capability_tags, success_criteria, plan, self_check, quality_notes,
# references_, artifacts are PostgreSQL TEXT[] -> Parquet list<string>.
COLUMNS = [
    "task_id",
    "domain",
    "difficulty",
    "task_shape",
    "capability_tags",
    "user_request",
    "context",
    "success_criteria",
    "plan",
    "reasoning_summary",
    "final_answer",
    "self_check",
    "quality_notes",
    "references_",
    "artifacts",
    "created_at",
]

# Parquet schema: list columns mapped to list<string>, timestamps to timestamp.
PARQUET_SCHEMA = pa.schema([
    pa.field("task_id", pa.string(), nullable=False),
    pa.field("domain", pa.string(), nullable=False),
    pa.field("difficulty", pa.string(), nullable=False),
    pa.field("task_shape", pa.string(), nullable=False),
    pa.field("capability_tags", pa.list_(pa.string()), nullable=False),
    pa.field("user_request", pa.string(), nullable=False),
    pa.field("context", pa.string(), nullable=False),
    pa.field("success_criteria", pa.list_(pa.string()), nullable=False),
    pa.field("plan", pa.list_(pa.string()), nullable=False),
    pa.field("reasoning_summary", pa.string(), nullable=False),
    pa.field("final_answer", pa.string(), nullable=False),
    pa.field("self_check", pa.list_(pa.string()), nullable=False),
    pa.field("quality_notes", pa.list_(pa.string()), nullable=False),
    pa.field("references", pa.list_(pa.string()), nullable=True),
    pa.field("artifacts", pa.list_(pa.string()), nullable=True),
    pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=True),
])

SELECT_SQL = f"""
SELECT {', '.join(COLUMNS)}
FROM {TABLE_NAME}
ORDER BY id
"""


def get_db_connection() -> psycopg2.extensions.connection:
    """Create a PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "distillation"),
        user=os.environ.get("POSTGRES_USER", "distillation"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def fetch_all_rows(conn: psycopg2.extensions.connection) -> list[tuple]:
    """Fetch all rows from distillation_pairs ordered by id."""
    with conn.cursor() as cur:
        cur.execute(SELECT_SQL)
        return cur.fetchall()


def rows_to_column_dict(rows: list[tuple]) -> dict[str, list]:
    """Transpose row-major data to column-major dict for pyarrow."""
    col_data: dict[str, list] = {col: [] for col in COLUMNS}

    for row in rows:
        for i, col in enumerate(COLUMNS):
            value = row[i]
            # PostgreSQL TEXT[] comes as Python list via psycopg2, no conversion needed.
            # None values stay as None for nullable columns.
            col_data[col].append(value)

    return col_data


def build_table(col_data: dict[str, list]) -> pa.Table:
    """Build a pyarrow Table from column data with explicit schema."""
    # Map references_ column name to 'references' in Parquet (references is reserved in Python)
    arrays = []
    for field in PARQUET_SCHEMA:
        col_name = field.name
        source_col = "references_" if col_name == "references" else col_name
        data = col_data[source_col]
        arrays.append(pa.array(data, type=field.type))

    return pa.Table.from_arrays(arrays, schema=PARQUET_SCHEMA)


def write_shards(
    table: pa.Table,
    output_dir: str,
    shard_size: int,
) -> int:
    """Write table as sharded Parquet files. Returns number of shards written."""
    total_rows = table.num_rows
    if total_rows == 0:
        print("[export_parquet] No rows to export", file=sys.stderr)
        return 0

    num_shards = max(1, math.ceil(total_rows / shard_size))

    os.makedirs(output_dir, exist_ok=True)

    for shard_idx in range(num_shards):
        start = shard_idx * shard_size
        end = min(start + shard_size, total_rows)
        shard_table = table.slice(start, end - start)

        filename = f"train-{shard_idx:05d}-of-{num_shards:05d}.parquet"
        filepath = os.path.join(output_dir, filename)

        pq.write_table(shard_table, filepath)
        print(
            f"[export_parquet] Written {filepath} ({end - start} rows)",
            file=sys.stderr,
        )

    return num_shards


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL distillation_pairs to Parquet files",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for Parquet files (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--shard-size",
        type=int,
        default=DEFAULT_SHARD_SIZE,
        help=f"Maximum rows per shard (default: {DEFAULT_SHARD_SIZE})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    try:
        conn = get_db_connection()
    except psycopg2.OperationalError as exc:
        print(f"[export_parquet] DB connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = fetch_all_rows(conn)
    finally:
        conn.close()

    print(
        f"[export_parquet] Fetched {len(rows)} rows from {TABLE_NAME}",
        file=sys.stderr,
    )

    col_data = rows_to_column_dict(rows)
    table = build_table(col_data)

    num_shards = write_shards(table, args.output_dir, args.shard_size)

    print(
        f"[export_parquet] Export complete: {len(rows)} rows in {num_shards} shard(s)"
        f" -> {args.output_dir}",
    )


if __name__ == "__main__":
    main()
