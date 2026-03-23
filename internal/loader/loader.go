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
	"github.com/guny524/distillation/internal/model"
)

// tableName is the target PostgreSQL table.
const tableName = "distillation_pairs"

// maxLineSize is the maximum JSONL line size in bytes (10MB).
// GPT-5.4 single response output is ~200KB; with all fields combined a JSONL
// record is typically under 500KB. 10MB provides a generous safety margin
// while still catching truly malformed input that would exhaust memory.
const maxLineSize = 10 << 20 // 10MB

// createTableSQL defines the DDL for the distillation_pairs table.
// 15 columns matching Python's CREATE_TABLE_SQL exactly.
const createTableSQL = `
CREATE TABLE IF NOT EXISTS ` + tableName + ` (
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
)`

// insertSQL defines the INSERT statement with positional parameters and
// ON CONFLICT (task_id) DO NOTHING. Duplicate task_id rows are silently skipped.
// Parameter order: task_id, domain, difficulty, task_shape, capability_tags,
// user_request, context, success_criteria, plan, reasoning_summary,
// final_answer, self_check, quality_notes, references_, artifacts.
const insertSQL = `
INSERT INTO ` + tableName + ` (
    task_id, domain, difficulty, task_shape, capability_tags,
    user_request, context, success_criteria, plan,
    reasoning_summary, final_answer, self_check, quality_notes,
    references_, artifacts
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13,
    $14, $15
)
ON CONFLICT (task_id) DO NOTHING`

// insertParamOrder defines the key order for extracting positional parameters
// from the params map. Must match the $1..$15 order in insertSQL.
var insertParamOrder = []string{
	"task_id", "domain", "difficulty", "task_shape", "capability_tags",
	"user_request", "context", "success_criteria", "plan",
	"reasoning_summary", "final_answer", "self_check", "quality_notes",
	"references_", "artifacts",
}

// CreateTable executes the CREATE TABLE IF NOT EXISTS DDL.
func CreateTable(ctx context.Context, q db.Querier) error {
	_, err := q.Exec(ctx, createTableSQL)
	if err != nil {
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
