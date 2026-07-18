package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
)

// --- tests ---

func TestCreateTable_ExecutesDDL(t *testing.T) {
	var capturedSQL string
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
			capturedSQL = sql
			return pgconn.NewCommandTag("CREATE TABLE"), nil
		},
	}

	err := CreateTable(context.Background(), q)

	require.NoError(t, err)
	assert.Contains(t, capturedSQL, "CREATE TABLE IF NOT EXISTS")
	assert.Contains(t, capturedSQL, "distillation_pairs")
	assert.Contains(t, capturedSQL, "task_id")
	assert.Contains(t, capturedSQL, "references_")
	// M3 columns are folded into the single-file CREATE (no ALTER history).
	assert.Contains(t, capturedSQL, "artifact_refs")
	assert.Contains(t, capturedSQL, "source_lane")
	assert.Contains(t, capturedSQL, "JSONB")
}

func TestInsertRecord_M3Columns_InSQL(t *testing.T) {
	var capturedSQL string
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			capturedSQL = sql
			// 26 positional params ($1..$26): 15 base + 8 M3 + 3 reasoning.
			assert.Len(t, args, 26)
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	inserted, err := InsertRecord(context.Background(), q, makeValidParams())

	require.NoError(t, err)
	assert.True(t, inserted)
	assert.Contains(t, capturedSQL, "artifact_refs")
	assert.Contains(t, capturedSQL, "cot")
	assert.Contains(t, capturedSQL, "$26")
}

func TestProcessFile_M3Record(t *testing.T) {
	// A full artifact-based JSONL record (all M3 fields) must load successfully.
	var capturedArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
			capturedArgs = args
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	filePath := writeTestJSONL(t, m3JSONLLine())

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 0, failed)
	// source_lane is $16 (index 15); artifact_refs $18 (index 17).
	require.Len(t, capturedArgs, 26)
	assert.Equal(t, "ko", capturedArgs[15])
	assert.NotNil(t, capturedArgs[17], "artifact_refs must be passed to INSERT")
}

func TestProcessFile_ReasoningFields(t *testing.T) {
	// A record carrying cot/cot_raw/has_raw_cot must land those values at the
	// $24/$25/$26 positions (indices 23/24/25) of the INSERT.
	var capturedArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
			capturedArgs = args
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	line := `{"task_id":"cot-1","domain":"mathematics","difficulty":"medium","task_shape":"short-text",` +
		`"capability_tags":["reasoning"],"user_request":"q","context":"c","success_criteria":["ok"],` +
		`"plan":["s1"],"reasoning_summary":"sum","final_answer":"42","self_check":["ok"],"quality_notes":["ok"],` +
		`"cot":"in-band reasoning","cot_raw":"raw reasoning_content","has_raw_cot":true}`
	filePath := writeTestJSONL(t, line)

	inserted, _, failed, err := ProcessFile(context.Background(), q, filePath)
	require.NoError(t, err)
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 0, failed)
	require.Len(t, capturedArgs, 26)
	assert.Equal(t, "in-band reasoning", capturedArgs[23])
	assert.Equal(t, "raw reasoning_content", capturedArgs[24])
	assert.Equal(t, true, capturedArgs[25])
}

func TestProcessFile_InvalidArtifactSourceType_Failed(t *testing.T) {
	// A record whose artifact_refs[].source_type is not a valid enum must fail.
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	line := `{"task_id":"bad-src","domain":"software-engineering","difficulty":"hard","task_shape":"code","capability_tags":["reasoning"],"user_request":"req","context":"ctx","success_criteria":["ok"],"plan":["s1"],"reasoning_summary":"sum","final_answer":"ans","self_check":["ok"],"quality_notes":["ok"],"source_lane":"en","pair_id":"p1","artifact_refs":[{"source_type":"wikipedia","doc_id":"d1","license":"CC","why_relevant":"r"}],"student_filter_verdict":{"verdict":"fail","method":"both","reason":"hard"}}`
	filePath := writeTestJSONL(t, line)

	inserted, _, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 1, failed, "invalid artifact source_type should be counted as failed")
}

func TestInsertRecord_Inserted(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			assert.Contains(t, sql, "INSERT INTO")
			assert.Contains(t, sql, "ON CONFLICT")
			// rowsAffected=1 means inserted
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	params := makeValidParams()
	inserted, err := InsertRecord(context.Background(), q, params)

	require.NoError(t, err)
	assert.True(t, inserted, "rowsAffected=1 should mean inserted=true")
}

func TestInsertRecord_Skipped(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			// rowsAffected=0 means ON CONFLICT skipped
			return pgconn.NewCommandTag("INSERT 0 0"), nil
		},
	}

	params := makeValidParams()
	inserted, err := InsertRecord(context.Background(), q, params)

	require.NoError(t, err)
	assert.False(t, inserted, "rowsAffected=0 should mean inserted=false (skipped)")
}

func TestInsertRecord_Error(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), fmt.Errorf("unique constraint violation")
		},
	}

	params := makeValidParams()
	_, err := InsertRecord(context.Background(), q, params)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unique constraint violation")
}

func TestProcessFile_ValidRecord(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	filePath := writeTestJSONL(t, validJSONLLine())

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 0, failed)
}

func TestProcessFile_InvalidJSON_CountedAsFailed(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	filePath := writeTestJSONL(t, "{not valid json")

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 1, failed)
}

func TestProcessFile_MissingField_CountedAsFailed(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	// JSON with missing required field "domain"
	filePath := writeTestJSONL(t, `{"task_id":"t1"}`)

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 1, failed)
}

func TestProcessFile_EmptyLinesSkipped(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	content := "\n" + validJSONLLine() + "\n\n  \n"
	filePath := writeTestJSONL(t, content)

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 0, failed)
}

func TestProcessFile_MultipleRecords(t *testing.T) {
	insertCount := 0
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			insertCount++
			if insertCount == 2 {
				// Second record: ON CONFLICT skip
				return pgconn.NewCommandTag("INSERT 0 0"), nil
			}
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	line1 := validJSONLLineWithTaskID("task-001")
	line2 := validJSONLLineWithTaskID("task-002")
	line3 := validJSONLLineWithTaskID("task-003")
	content := line1 + "\n" + line2 + "\n" + line3
	filePath := writeTestJSONL(t, content)

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 2, inserted, "records 1 and 3 should be inserted")
	assert.Equal(t, 1, skipped, "record 2 should be skipped (ON CONFLICT)")
	assert.Equal(t, 0, failed)
}

func TestCreateTable_ExecError(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), fmt.Errorf("exec failed: permission denied")
		},
	}

	err := CreateTable(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create table")
	assert.Contains(t, err.Error(), "distillation_pairs")
}

func TestProcessFile_NotExist(t *testing.T) {
	q := &dbtest.MockQuerier{}

	_, _, _, err := ProcessFile(context.Background(), q, "/nonexistent/path/to/file.jsonl")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "open")
}

func TestProcessFile_InsertError_CountedAsFailed(t *testing.T) {
	// INSERT errors are counted as failed (per-record, not file-level).
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), fmt.Errorf("insert constraint error")
		},
	}

	filePath := writeTestJSONL(t, validJSONLLine())

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	// File-level error must be nil (INSERT errors are counted, not propagated).
	require.NoError(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 1, failed, "INSERT error should be counted as failed, not propagated")
}

func TestProcessFile_InvalidEnum_CountedAsFailed(t *testing.T) {
	// A record with valid JSON and all required fields but invalid enum value
	// (e.g., domain="invalid-domain") must be counted as failed via ValidateEnums.
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	// Valid JSON with invalid domain enum value.
	line := `{"task_id":"enum-fail","domain":"invalid-domain","difficulty":"medium","task_shape":"short-text","capability_tags":["reasoning"],"user_request":"req","context":"ctx","success_criteria":["ok"],"plan":["s1"],"reasoning_summary":"sum","final_answer":"ans","self_check":["ok"],"quality_notes":["ok"]}`
	filePath := writeTestJSONL(t, line)

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 1, failed, "invalid enum should be counted as failed")
}

// --- test helpers ---

// makeValidParams returns a parameter map that matches the INSERT SQL.
func makeValidParams() map[string]any {
	return map[string]any{
		"task_id":           "test-id-1",
		"domain":            "mathematics",
		"difficulty":        "medium",
		"task_shape":        "short-text",
		"capability_tags":   []string{"reasoning"},
		"user_request":      "Solve this",
		"context":           "Given...",
		"success_criteria":  []string{"correct"},
		"plan":              []string{"step1"},
		"reasoning_summary": "Thought process",
		"final_answer":      "42",
		"self_check":        []string{"verified"},
		"quality_notes":     []string{"good"},
		"references_":       nil,
		"artifacts":         nil,
		// M3 params (all optional; nil is a valid INSERT value / SQL NULL).
		"source_lane":            nil,
		"pair_id":                nil,
		"artifact_refs":          nil,
		"student_filter_verdict": nil,
		"verification":           nil,
		"difficulty_mutations":   nil,
		"teacher_model":          nil,
		"teacher_provider":       nil,
		// Reasoning params (all optional; nil is SQL NULL).
		"cot":         nil,
		"cot_raw":     nil,
		"has_raw_cot": nil,
	}
}

// m3JSONLLine returns a full artifact-based JSONL record with every M3 field.
func m3JSONLLine() string {
	return `{"task_id":"m3-001","domain":"software-engineering","difficulty":"hard",` +
		`"task_shape":"code","capability_tags":["reasoning","problem-solving"],` +
		`"user_request":"Fix the deadlock","context":"Given a KEP excerpt","success_criteria":["no deadlock"],` +
		`"plan":["reproduce","patch"],"reasoning_summary":"race on lock ordering","final_answer":"reorder locks",` +
		`"self_check":["tested"],"quality_notes":["ok"],"source_lane":"ko","pair_id":"pair-42",` +
		`"artifact_refs":[{"source_type":"k8s","doc_id":"KEP-1234","url":"https://kep","license":"CC-BY-4.0","locator":"sec 3","why_relevant":"describes lock order"}],` +
		`"student_filter_verdict":{"verdict":"uncertain","method":"both","judge_score":0.4,"self_consistency_disagreement":0.6,"reason":"disputed"},` +
		`"verification":{"method":"test","result":"pass","detail":"race test green"},` +
		`"difficulty_mutations":["fault_injection","conflicting_constraint"],` +
		`"teacher_model":"gpt-5.4","teacher_provider":"openai"}`
}

// validJSONLLine returns a single valid JSONL record string.
func validJSONLLine() string {
	return validJSONLLineWithTaskID("test-task-001")
}

// validJSONLLineWithTaskID returns a valid JSONL record with the given task_id.
func validJSONLLineWithTaskID(taskID string) string {
	return fmt.Sprintf(`{"task_id":"%s","domain":"mathematics","difficulty":"medium","task_shape":"short-text","capability_tags":["reasoning"],"user_request":"Solve this","context":"Given...","success_criteria":["correct"],"plan":["step1"],"reasoning_summary":"Thought process","final_answer":"42","self_check":["verified"],"quality_notes":["good"]}`, taskID)
}

func TestProcessFile_LargeRecord_WithinBuffer(t *testing.T) {
	// bufio.Scanner buffer is 10MB (maxLineSize). A valid JSONL record slightly
	// over 1MB (the old limit) must be processed successfully with the new buffer.
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag("INSERT 0 1"), nil
		},
	}

	// Build a valid JSONL record with a final_answer field > 1MB (old limit).
	largeAnswer := make([]byte, 1<<20+1) // 1MB+1 bytes
	for i := range largeAnswer {
		largeAnswer[i] = 'A'
	}
	line := fmt.Sprintf(
		`{"task_id":"big","domain":"mathematics","difficulty":"medium","task_shape":"short-text","capability_tags":["reasoning"],"user_request":"Solve","context":"Ctx","success_criteria":["ok"],"plan":["s1"],"reasoning_summary":"Sum","final_answer":"%s","self_check":["v"],"quality_notes":["g"]}`,
		string(largeAnswer),
	)
	filePath := writeTestJSONL(t, line)

	inserted, skipped, failed, err := ProcessFile(context.Background(), q, filePath)

	require.NoError(t, err)
	assert.Equal(t, 1, inserted, "1MB+ record should be inserted with 10MB buffer")
	assert.Equal(t, 0, skipped)
	assert.Equal(t, 0, failed)
}

func TestProcessFile_ScannerError(t *testing.T) {
	// bufio.Scanner returns an error when a single line exceeds the 10MB buffer limit.
	// This exercises the scanner.Err() != nil branch in ProcessFile.
	q := &dbtest.MockQuerier{}

	// Build a line longer than 10MB (scanner buffer max = 10<<20 = 10485760 bytes).
	oversized := make([]byte, 10<<20+1)
	for i := range oversized {
		oversized[i] = 'x'
	}
	filePath := writeTestJSONL(t, string(oversized))

	_, _, _, err := ProcessFile(context.Background(), q, filePath)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "scan")
}

// writeTestJSONL creates a temporary JSONL file with the given content.
func writeTestJSONL(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.jsonl")
	err := os.WriteFile(path, []byte(content), 0o644)
	require.NoError(t, err)
	return path
}

// --- flush buffer helpers (CountRows / MaxID / DeleteByTaskIDs) ---

func TestCountRows(t *testing.T) {
	var gotSQL string
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			gotSQL = sql
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				// sqlcgen.CountPairs scans COUNT(*) into an int64.
				*(dest[0].(*int64)) = 12345
				return nil
			}}
		},
	}

	n, err := CountRows(context.Background(), q)

	require.NoError(t, err)
	assert.Equal(t, 12345, n)
	assert.Contains(t, gotSQL, "COUNT(*)")
	assert.Contains(t, gotSQL, "distillation_pairs")
}

func TestCountRows_ScanError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(...any) error { return fmt.Errorf("boom") }}
		},
	}

	_, err := CountRows(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "count distillation_pairs")
}

func TestMaxID(t *testing.T) {
	var gotSQL string
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			gotSQL = sql
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int64)) = 987
				return nil
			}}
		},
	}

	id, err := MaxID(context.Background(), q)

	require.NoError(t, err)
	assert.Equal(t, int64(987), id)
	assert.Contains(t, gotSQL, "MAX(id)")
	assert.Contains(t, gotSQL, "COALESCE")
}

func TestMaxID_ScanError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(...any) error { return fmt.Errorf("boom") }}
		},
	}

	_, err := MaxID(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "max id distillation_pairs")
}

func TestDeleteByTaskIDs(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			gotSQL = sql
			gotArgs = args
			return pgconn.NewCommandTag("DELETE 2"), nil
		},
	}

	deleted, err := DeleteByTaskIDs(context.Background(), q, []string{"task-a", "task-b"})

	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	assert.Contains(t, gotSQL, "DELETE FROM distillation_pairs")
	assert.Contains(t, gotSQL, "WHERE task_id = ANY($1::text[])")
	require.Len(t, gotArgs, 1)
	assert.Equal(t, []string{"task-a", "task-b"}, gotArgs[0])
}

// Empty input is a no-op: no Exec, no delete (nothing was exported).
func TestDeleteByTaskIDs_Empty_NoOp(t *testing.T) {
	called := false
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			called = true
			return pgconn.NewCommandTag(""), nil
		},
	}

	deleted, err := DeleteByTaskIDs(context.Background(), q, nil)

	require.NoError(t, err)
	assert.Zero(t, deleted)
	assert.False(t, called, "empty task_id set must not issue a DELETE")
}

func TestDeleteByTaskIDs_ExecError(t *testing.T) {
	q := &dbtest.MockQuerier{
		ExecFn: func(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
			return pgconn.NewCommandTag(""), fmt.Errorf("deadlock")
		},
	}

	_, err := DeleteByTaskIDs(context.Background(), q, []string{"task-a"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete 1 flushed rows by task_id")
}
