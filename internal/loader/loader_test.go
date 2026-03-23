package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
	}
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
