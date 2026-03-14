#!/usr/bin/env python3
"""Dump 4-axis coverage distribution from PostgreSQL to coverage.json.

Runs as init container. Queries distillation_pairs table for counts
grouped by domain, difficulty, task_shape, and capability_tags (unnested).
Outputs empty coverage when the table does not exist yet.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2


TABLE_NAME = "distillation_pairs"

DEFAULT_OUTPUT = "/workspace/coverage.json"


def get_db_connection() -> psycopg2.extensions.connection:
    """Create a PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "distillation"),
        user=os.environ.get("POSTGRES_USER", "distillation"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )


def table_exists(cur: psycopg2.extensions.cursor) -> bool:
    """Check whether distillation_pairs table exists."""
    cur.execute(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.tables"
        "  WHERE table_name = %s"
        ")",
        (TABLE_NAME,),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def query_axis_counts(
    cur: psycopg2.extensions.cursor,
    column: str,
) -> dict[str, int]:
    """Count rows grouped by a single text column."""
    cur.execute(
        f"SELECT {column}, COUNT(*) FROM {TABLE_NAME} GROUP BY {column}",  # noqa: S608
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def query_capability_counts(cur: psycopg2.extensions.cursor) -> dict[str, int]:
    """Count by unnested capability_tags (PostgreSQL array)."""
    cur.execute(
        f"SELECT tag, COUNT(*) FROM {TABLE_NAME}, unnest(capability_tags) AS tag"  # noqa: S608
        " GROUP BY tag",
    )
    return {row[0]: row[1] for row in cur.fetchall()}


def query_cross_counts(
    cur: psycopg2.extensions.cursor,
    col_a: str,
    col_b: str,
) -> dict[str, int]:
    """Count rows grouped by two columns combined as 'a:b' key."""
    cur.execute(
        f"SELECT {col_a}, {col_b}, COUNT(*)"  # noqa: S608
        f" FROM {TABLE_NAME}"
        f" GROUP BY {col_a}, {col_b}",
    )
    return {f"{row[0]}:{row[1]}": row[2] for row in cur.fetchall()}


def query_total(cur: psycopg2.extensions.cursor) -> int:
    """Return total row count."""
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")  # noqa: S608
    row = cur.fetchone()
    return row[0] if row else 0


def build_empty_coverage() -> dict:
    """Return empty coverage structure for initial state."""
    return {
        "total_count": 0,
        "domain": {},
        "difficulty": {},
        "task_shape": {},
        "capability": {},
        "domain_x_difficulty": {},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_coverage(cur: psycopg2.extensions.cursor) -> dict:
    """Query all axes and assemble coverage dict."""
    return {
        "total_count": query_total(cur),
        "domain": query_axis_counts(cur, "domain"),
        "difficulty": query_axis_counts(cur, "difficulty"),
        "task_shape": query_axis_counts(cur, "task_shape"),
        "capability": query_capability_counts(cur),
        "domain_x_difficulty": query_cross_counts(cur, "domain", "difficulty"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dump 4-axis coverage from PostgreSQL to coverage.json",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output path for coverage.json (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    try:
        conn = get_db_connection()
    except psycopg2.OperationalError as exc:
        print(f"[dump_coverage] DB connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            if not table_exists(cur):
                print(
                    f"[dump_coverage] Table '{TABLE_NAME}' not found, writing empty coverage",
                    file=sys.stderr,
                )
                coverage = build_empty_coverage()
            else:
                coverage = build_coverage(cur)
    finally:
        conn.close()

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(coverage, f, indent=2, ensure_ascii=False)

    print(
        f"[dump_coverage] Written {coverage['total_count']} total records"
        f" -> {args.output}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
