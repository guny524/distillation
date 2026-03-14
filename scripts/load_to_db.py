#!/usr/bin/env python3
"""Load JSONL files into PostgreSQL distillation_pairs table.

Reads one or more JSONL files, optionally validates each line against
the distillation schema, and INSERTs into PostgreSQL.
Duplicate task_id rows are silently skipped (ON CONFLICT DO NOTHING).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2


TABLE_NAME = "distillation_pairs"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id              SERIAL PRIMARY KEY,
    task_id         TEXT UNIQUE NOT NULL,
    domain          TEXT NOT NULL,
    difficulty      TEXT NOT NULL,
    task_shape      TEXT NOT NULL,
    capability_tags TEXT[] NOT NULL,
    user_request    TEXT NOT NULL,
    context         TEXT NOT NULL,
    success_criteria TEXT[] NOT NULL,
    plan            TEXT[] NOT NULL,
    reasoning_summary TEXT NOT NULL,
    final_answer    TEXT NOT NULL,
    self_check      TEXT[] NOT NULL,
    quality_notes   TEXT[] NOT NULL,
    references_     TEXT[],
    artifacts       TEXT[],
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    task_id, domain, difficulty, task_shape, capability_tags,
    user_request, context, success_criteria, plan,
    reasoning_summary, final_answer, self_check, quality_notes,
    references_, artifacts
) VALUES (
    %(task_id)s, %(domain)s, %(difficulty)s, %(task_shape)s, %(capability_tags)s,
    %(user_request)s, %(context)s, %(success_criteria)s, %(plan)s,
    %(reasoning_summary)s, %(final_answer)s, %(self_check)s, %(quality_notes)s,
    %(references_)s, %(artifacts)s
)
ON CONFLICT (task_id) DO NOTHING
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


def load_schema(schema_path: str) -> dict | None:
    """Load JSON schema for validation. Returns None on failure."""
    try:
        import jsonschema  # noqa: F401 - check availability

        with open(schema_path, encoding="utf-8") as f:
            return json.load(f)
    except ImportError:
        print(
            "[load_to_db] jsonschema package not available, skipping validation",
            file=sys.stderr,
        )
        return None
    except (OSError, json.JSONDecodeError) as exc:
        print(
            f"[load_to_db] Failed to load schema {schema_path}: {exc}",
            file=sys.stderr,
        )
        return None


def validate_record(record: dict, schema: dict) -> bool:
    """Validate a record against JSON schema. Returns True if valid."""
    try:
        import jsonschema

        jsonschema.validate(instance=record, schema=schema)
    except jsonschema.ValidationError as exc:
        print(
            f"[load_to_db] Validation failed for task_id={record.get('task_id', '?')}: "
            f"{exc.message}",
            file=sys.stderr,
        )
        return False
    return True


def record_to_params(record: dict) -> dict:
    """Convert a JSON record to INSERT parameter dict."""
    return {
        "task_id": record["task_id"],
        "domain": record["domain"],
        "difficulty": record["difficulty"],
        "task_shape": record["task_shape"],
        "capability_tags": record["capability_tags"],
        "user_request": record["user_request"],
        "context": record["context"],
        "success_criteria": record["success_criteria"],
        "plan": record["plan"],
        "reasoning_summary": record["reasoning_summary"],
        "final_answer": record["final_answer"],
        "self_check": record["self_check"],
        "quality_notes": record["quality_notes"],
        "references_": record.get("references"),
        "artifacts": record.get("artifacts"),
    }


def process_file(
    file_path: Path,
    cur: psycopg2.extensions.cursor,
    schema: dict | None,
) -> tuple[int, int, int]:
    """Process one JSONL file. Returns (inserted, skipped, failed) counts."""
    inserted = 0
    skipped = 0
    failed = 0

    with open(file_path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[load_to_db] {file_path}:{line_no} JSON parse error: {exc}",
                    file=sys.stderr,
                )
                failed += 1
                continue

            if schema is not None and not validate_record(record, schema):
                failed += 1
                continue

            try:
                params = record_to_params(record)
            except KeyError as exc:
                print(
                    f"[load_to_db] {file_path}:{line_no} Missing required field: {exc}",
                    file=sys.stderr,
                )
                failed += 1
                continue

            try:
                cur.execute(INSERT_SQL, params)
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            except psycopg2.Error as exc:
                print(
                    f"[load_to_db] {file_path}:{line_no} INSERT error: {exc}",
                    file=sys.stderr,
                )
                cur.connection.rollback()
                failed += 1

    return inserted, skipped, failed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load JSONL files into PostgreSQL distillation_pairs table",
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="JSONL file path(s) to load",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="JSON schema file path for validation (optional, skip if not provided)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    schema = None
    if args.schema:
        schema = load_schema(args.schema)

    try:
        conn = get_db_connection()
    except psycopg2.OperationalError as exc:
        print(f"[load_to_db] DB connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

            total_inserted = 0
            total_skipped = 0
            total_failed = 0

            for file_path in args.files:
                if not file_path.exists():
                    print(
                        f"[load_to_db] File not found: {file_path}",
                        file=sys.stderr,
                    )
                    continue

                inserted, skipped, failed = process_file(file_path, cur, schema)
                conn.commit()

                total_inserted += inserted
                total_skipped += skipped
                total_failed += failed

                print(
                    f"[load_to_db] {file_path}: "
                    f"inserted={inserted} skipped={skipped} failed={failed}",
                    file=sys.stderr,
                )

        print(
            f"[load_to_db] Total: "
            f"inserted={total_inserted} skipped={total_skipped} failed={total_failed}",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
