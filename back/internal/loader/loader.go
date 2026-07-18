// Package loader provides JSONL-to-PostgreSQL loading for the distillation pipeline.
// This is the Go equivalent of Python's load_to_db.py.
package loader

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/db/sqlcgen"
	"github.com/guny524/distillation/internal/model"
)

// tableName is the target PostgreSQL table. The DDL lives in internal/db
// (internal/db/schema.sql, applied by db.MigratePairs and
// read by sqlc); the count/max/delete statements are sqlc-generated (sqlcgen).
// tableName is kept for the insert SQL and error messages.
const tableName = "distillation_pairs"

// maxLineSize is the maximum JSONL line size in bytes (10MB).
// GPT-5.4 single response output is ~200KB; with all fields combined a JSONL
// record is typically under 500KB. 10MB provides a generous safety margin
// while still catching truly malformed input that would exhaust memory.
const maxLineSize = 10 << 20 // 10MB

// insertSQL is NOT sqlc-generated (unlike count/max/delete): InsertRecord is fed
// an untyped map[string]any (RecordToParams output) through a signature that two
// FROZEN callers depend on (internal/runner and internal/worker's
// ProjectAndComplete), so sqlc's typed params struct is a poor fit; and
// internal/runner/runner_test.go asserts strings.HasPrefix(TrimSpace(sql),
// "INSERT") on the executed statement, which sqlc's mandatory leading
// "-- name: ... :execrows" comment on the generated query string would break. It
// stays a positional INSERT here (columns still validated against the same
// schema, internal/db/schema.sql, by the sqlc
// block's presence). ON CONFLICT (task_id) DO NOTHING
// silently skips duplicate task_id rows. Base params ($1..$15), M3 params
// ($16..$23), reasoning params ($24..$26); created_at is DB-defaulted.
const insertSQL = `
INSERT INTO ` + tableName + ` (
    task_id, domain, difficulty, task_shape, capability_tags,
    user_request, context, success_criteria, plan,
    reasoning_summary, final_answer, self_check, quality_notes,
    references_, artifacts,
    source_lane, pair_id, artifact_refs, student_filter_verdict,
    verification, difficulty_mutations, teacher_model, teacher_provider,
    cot, cot_raw, has_raw_cot
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13,
    $14, $15,
    $16, $17, $18, $19,
    $20, $21, $22, $23,
    $24, $25, $26
)
ON CONFLICT (task_id) DO NOTHING`

// insertParamOrder defines the key order for extracting positional parameters
// from the params map. Must match the $1..$26 order in insertSQL.
var insertParamOrder = []string{
	"task_id", "domain", "difficulty", "task_shape", "capability_tags",
	"user_request", "context", "success_criteria", "plan",
	"reasoning_summary", "final_answer", "self_check", "quality_notes",
	"references_", "artifacts",
	"source_lane", "pair_id", "artifact_refs", "student_filter_verdict",
	"verification", "difficulty_mutations", "teacher_model", "teacher_provider",
	"cot", "cot_raw", "has_raw_cot",
}

// CountRows returns the number of rows currently buffered in distillation_pairs.
// The flush cycle triggers only when this reaches the configured threshold.
func CountRows(ctx context.Context, q db.Querier) (int, error) {
	n, err := sqlcgen.New(q).CountPairs(ctx)
	if err != nil {
		return 0, fmt.Errorf("count %s: %w", tableName, err)
	}
	return int(n), nil
}

// MaxID returns the highest id in distillation_pairs, or 0 when the table is
// empty. The flush cycle snapshots this value as the high-water mark: only rows
// with id <= MaxID are exported and deleted, so rows inserted concurrently
// (which get higher ids) are preserved for the next flush.
func MaxID(ctx context.Context, q db.Querier) (int64, error) {
	id, err := sqlcgen.New(q).MaxPairID(ctx)
	if err != nil {
		return 0, fmt.Errorf("max id %s: %w", tableName, err)
	}
	return id, nil
}

// DeleteByTaskIDs removes exactly the rows whose task_id is in taskIDs and
// returns the number deleted. flush calls this with the task_ids it actually
// exported so the delete set == the export set (never more). It must be called
// only after the corresponding Parquet batch has been durably written to the
// export volume and verified (flush atomicity: write and verify first, delete second — a
// failed write never reaches this call).
func DeleteByTaskIDs(ctx context.Context, q db.Querier, taskIDs []string) (int64, error) {
	if len(taskIDs) == 0 {
		return 0, nil
	}
	deleted, err := sqlcgen.New(q).DeletePairsByTaskIDs(ctx, taskIDs)
	if err != nil {
		return 0, fmt.Errorf("delete %d flushed rows by task_id: %w", len(taskIDs), err)
	}
	return deleted, nil
}

// CreateTable runs the idempotent distillation_pairs DDL (CREATE TABLE IF NOT
// EXISTS + ALTER TABLE ADD COLUMN IF NOT EXISTS for pre-M3 tables). Safe to call
// on every run. Thin delegate to db.MigratePairs (the schema now lives in
// internal/db); the "create table <tableName>" error context is preserved for
// existing callers (monolithic run, load, coverage, worker openDB, loader_test).
func CreateTable(ctx context.Context, q db.Querier) error {
	if err := db.MigratePairs(ctx, q); err != nil {
		return fmt.Errorf("create table %s: %w", tableName, err)
	}
	return nil
}

// InsertRecord inserts a single record into the distillation_pairs table.
// Returns (true, nil) if the row was inserted, (false, nil) if skipped
// due to ON CONFLICT (duplicate task_id), or (false, err) on error.
func InsertRecord(ctx context.Context, q db.Querier, params map[string]any) (inserted bool, err error) {
	args := make([]any, len(insertParamOrder))
	for i, key := range insertParamOrder {
		args[i] = params[key]
	}

	tag, err := q.Exec(ctx, insertSQL, args...)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// ProcessFile reads a JSONL file line by line, parses each line to a record,
// converts it to DB parameters via model.RecordToParams, and inserts it.
// Empty lines are skipped. Failures at any stage (JSON parse, missing field,
// INSERT error) increment the failed count and processing continues.
// Returns (inserted, skipped, failed, error). The error return is only for
// file-level errors (e.g., file not found); per-record errors are counted in failed.
func ProcessFile(ctx context.Context, q db.Querier, filePath string) (inserted, skipped, failed int, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open %s: %w", filePath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	lineNo := 0

	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Step 1: JSON parse
		var record map[string]any
		if jsonErr := json.Unmarshal([]byte(line), &record); jsonErr != nil {
			fmt.Fprintf(os.Stderr, "[loader] %s:%d JSON parse error: %v\n", filePath, lineNo, jsonErr)
			failed++
			continue
		}

		// Step 2: Convert to DB params (validates required fields)
		params, paramErr := model.RecordToParams(record)
		if paramErr != nil {
			fmt.Fprintf(os.Stderr, "[loader] %s:%d missing/invalid field: %v\n", filePath, lineNo, paramErr)
			failed++
			continue
		}

		// Step 2.5: Validate enum values (domain, difficulty, task_shape, capability_tags)
		if enumErr := model.ValidateEnums(params); enumErr != nil {
			fmt.Fprintf(os.Stderr, "[loader] %s:%d enum validation: %v\n", filePath, lineNo, enumErr)
			failed++
			continue
		}

		// Step 3: INSERT
		ok, insertErr := InsertRecord(ctx, q, params)
		if insertErr != nil {
			fmt.Fprintf(os.Stderr, "[loader] %s:%d INSERT error: %v\n", filePath, lineNo, insertErr)
			failed++
			continue
		}
		if ok {
			inserted++
		} else {
			skipped++
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return inserted, skipped, failed, fmt.Errorf("scan %s: %w", filePath, scanErr)
	}

	return inserted, skipped, failed, nil
}
