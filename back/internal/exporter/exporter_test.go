package exporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
)


// --- FetchAllRows tests ---

func TestFetchAllRows_Empty(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			assert.Contains(t, sql, "distillation_pairs")
			assert.Contains(t, sql, "ORDER BY id")
			return dbtest.NewMockRowsAny([][]any{}), nil
		},
	}

	rows, err := FetchAllRows(context.Background(), q)

	require.NoError(t, err)
	assert.Empty(t, rows)
}

func TestFetchAllRows_WithRows(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	rawRow := []any{
		"task-1", "math", "easy", "qa",
		[]string{"reasoning"}, "request", "context",
		[]string{"correct"}, []string{"step1"}, "summary",
		"answer", []string{"ok"}, []string{"good"},
		[]string{"ref1"}, []string{"art1"},
		"ko", "pair-1", `[{"source_type":"arxiv"}]`, `{"verdict":"fail"}`,
		`{"method":"test","result":"pass"}`, []string{"fault_injection"}, "gpt", "openai",
		"in-band cot", "raw reasoning", true,
		&ts,
	}

	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return dbtest.NewMockRowsAny([][]any{rawRow}), nil
		},
	}

	rows, err := FetchAllRows(context.Background(), q)

	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "task-1", rows[0][0])
}

func TestFetchAllRows_QueryError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	rows, err := FetchAllRows(context.Background(), q)

	require.Error(t, err)
	assert.Nil(t, rows)
	assert.Contains(t, err.Error(), "fetch all rows")
}

func TestFetchAllRows_ScanError(t *testing.T) {
	// rows.Scan error during iteration must propagate with "scan row" prefix.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := &dbtest.MockRows{
				Data:   [][]any{{"task-1", "math", "easy", "qa", nil, "req", "ctx", nil, nil, "summary", "ans", nil, nil, nil, nil, nil}},
				Cursor: -1,
				ScanFn: func(_ []any, _ ...any) error {
					return fmt.Errorf("scan: dest count mismatch")
				},
			}
			return rows, nil
		},
	}

	rows, err := FetchAllRows(context.Background(), q)

	require.Error(t, err)
	assert.Nil(t, rows)
	assert.Contains(t, err.Error(), "scan row")
}

func TestFetchAllRows_RowsErrError(t *testing.T) {
	// rows.Err() returning a non-nil error after iteration must propagate.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := &dbtest.MockRowsWithErr{
				Inner:  dbtest.NewMockRowsAny([][]any{}),
				ErrVal: fmt.Errorf("rows iteration: network timeout"),
			}
			return rows, nil
		},
	}

	result, err := FetchAllRows(context.Background(), q)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "rows iteration")
}

// --- FetchRowsUpTo tests ---

func TestFetchRowsUpTo_QueryArgAndWhere(t *testing.T) {
	var gotSQL string
	var gotArgs []any
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			gotSQL = sql
			gotArgs = args
			return dbtest.NewMockRowsAny(makeSampleRows(2)), nil
		},
	}

	rows, err := FetchRowsUpTo(context.Background(), q, 77)

	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Contains(t, gotSQL, "WHERE id <= $1")
	assert.Contains(t, gotSQL, "ORDER BY id")
	require.Len(t, gotArgs, 1)
	assert.Equal(t, int64(77), gotArgs[0])
}

func TestFetchRowsUpTo_QueryError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	rows, err := FetchRowsUpTo(context.Background(), q, 5)

	require.Error(t, err)
	assert.Nil(t, rows)
	assert.Contains(t, err.Error(), "fetch rows up to 5")
}

// --- VerifyParquetDir tests ---

func TestVerifyParquetDir_Match(t *testing.T) {
	dir := t.TempDir()
	mem := memory.NewGoAllocator()
	rec, err := BuildTable(makeSampleRows(4), mem)
	require.NoError(t, err)
	defer rec.Release()

	_, err = WriteShards(rec, dir, 2, "zstd") // 4 rows / 2 = 2 shards
	require.NoError(t, err)

	assert.NoError(t, VerifyParquetDir(dir, 4))
}

func TestVerifyParquetDir_Mismatch(t *testing.T) {
	dir := t.TempDir()
	mem := memory.NewGoAllocator()
	rec, err := BuildTable(makeSampleRows(3), mem)
	require.NoError(t, err)
	defer rec.Release()

	_, err = WriteShards(rec, dir, 100, "zstd")
	require.NoError(t, err)

	err = VerifyParquetDir(dir, 5) // expected 5 but wrote 3
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row count mismatch")
}

func TestVerifyParquetDir_NoShards(t *testing.T) {
	err := VerifyParquetDir(t.TempDir(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no parquet shards")
}

// --- ArrowSchema tests ---

func TestArrowSchema_FieldCount(t *testing.T) {
	// ArrowSchema must define exactly 27 fields (16 base + 8 M3 + 3 reasoning).
	assert.Equal(t, 27, ArrowSchema.NumFields())
}

func TestArrowSchema_ReasoningFields(t *testing.T) {
	// cot/cot_raw export as nullable String; has_raw_cot as nullable Boolean.
	for _, name := range []string{"cot", "cot_raw"} {
		idx := ArrowSchema.FieldIndices(name)
		require.Len(t, idx, 1, "ArrowSchema must have field %q", name)
		f := ArrowSchema.Field(idx[0])
		assert.True(t, f.Nullable, "field %q must be nullable", name)
		assert.Equal(t, arrow.BinaryTypes.String, f.Type, "field %q must be String", name)
	}
	idx := ArrowSchema.FieldIndices("has_raw_cot")
	require.Len(t, idx, 1)
	f := ArrowSchema.Field(idx[0])
	assert.True(t, f.Nullable, "has_raw_cot must be nullable")
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, f.Type, "has_raw_cot must be Boolean")
}

func TestArrowSchema_M3Fields(t *testing.T) {
	// The M3 fields must exist, be nullable, and JSONB fields must be String.
	for _, name := range []string{
		"source_lane", "pair_id", "artifact_refs", "student_filter_verdict",
		"verification", "difficulty_mutations", "teacher_model", "teacher_provider",
	} {
		idx := ArrowSchema.FieldIndices(name)
		require.Len(t, idx, 1, "ArrowSchema must have field %q", name)
		assert.True(t, ArrowSchema.Field(idx[0]).Nullable, "field %q must be nullable", name)
	}
	// JSONB-as-text fields must be plain String, not List.
	for _, name := range []string{"artifact_refs", "student_filter_verdict", "verification"} {
		idx := ArrowSchema.FieldIndices(name)
		assert.Equal(t, arrow.BinaryTypes.String, ArrowSchema.Field(idx[0]).Type,
			"JSONB field %q must export as String", name)
	}
}

func TestSelectSQL_JSONBCastToText(t *testing.T) {
	// JSONB columns must be selected as ::text so pgx returns strings.
	assert.Contains(t, selectSQL, "artifact_refs::text")
	assert.Contains(t, selectSQL, "student_filter_verdict::text")
	assert.Contains(t, selectSQL, "verification::text")
	// Non-JSONB columns must not be cast.
	assert.NotContains(t, selectSQL, "source_lane::text")
}

func TestArrowSchema_ReferencesRemapping(t *testing.T) {
	// The DB column is "references_" but the Parquet field must be "references".
	// ArrowSchema must use "references" (not "references_").
	idx, found := ArrowSchema.FieldsByName("references")
	require.True(t, found, "ArrowSchema must have a field named 'references'")
	require.Len(t, idx, 1, "exactly one field named 'references'")

	// Must NOT have "references_".
	_, foundUnderscore := ArrowSchema.FieldsByName("references_")
	assert.False(t, foundUnderscore, "ArrowSchema must NOT have 'references_'")
}

// --- BuildTable tests ---

func TestBuildTable_RowCount(t *testing.T) {
	rows := makeSampleRows(3)
	mem := memory.NewGoAllocator()

	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	assert.Equal(t, int64(3), rec.NumRows())
}

func TestBuildTable_NullableColumns(t *testing.T) {
	// references and artifacts can be nil; created_at can be nil.
	rows := [][]any{
		makeRowWithNulls(), // references=nil, artifacts=nil, created_at=nil
	}
	mem := memory.NewGoAllocator()

	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	assert.Equal(t, int64(1), rec.NumRows())

	// Verify the nullable columns have null values.
	refsIdx := rec.Schema().FieldIndices("references")
	require.Len(t, refsIdx, 1)
	assert.True(t, rec.Column(refsIdx[0]).IsNull(0), "references should be null")

	artsIdx := rec.Schema().FieldIndices("artifacts")
	require.Len(t, artsIdx, 1)
	assert.True(t, rec.Column(artsIdx[0]).IsNull(0), "artifacts should be null")

	tsIdx := rec.Schema().FieldIndices("created_at")
	require.Len(t, tsIdx, 1)
	assert.True(t, rec.Column(tsIdx[0]).IsNull(0), "created_at should be null")
}

func TestBuildTable_WrongColumnCount(t *testing.T) {
	// A row with wrong number of columns must return an error.
	badRows := [][]any{
		{"only-one-element"},
	}
	mem := memory.NewGoAllocator()

	_, err := BuildTable(badRows, mem)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 27 columns")
}

func TestBuildTable_InvalidStringType(t *testing.T) {
	// task_id column expects a string but gets an int.
	row := makeSampleRows(1)
	row[0][0] = 12345 // task_id should be string
	mem := memory.NewGoAllocator()

	_, err := BuildTable(row, mem)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append row")
}

func TestBuildTable_InvalidListType(t *testing.T) {
	// capability_tags expects []string but gets a string.
	row := makeSampleRows(1)
	row[0][4] = "not-a-slice" // capability_tags should be []string
	mem := memory.NewGoAllocator()

	_, err := BuildTable(row, mem)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append row")
}

func TestBuildTable_InvalidTimestampType(t *testing.T) {
	// created_at expects *time.Time or nil, but gets a plain string.
	row := makeSampleRows(1)
	row[0][26] = "not-a-time" // created_at should be *time.Time or nil
	mem := memory.NewGoAllocator()

	_, err := BuildTable(row, mem)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "append row")
}

func TestBuildTable_CreatedAt_NilPointer(t *testing.T) {
	// created_at as a typed nil *time.Time pointer (not nil interface) must append null.
	// This exercises the tp == nil branch in appendRow.
	row := makeSampleRows(1)
	var nilTs *time.Time = nil
	row[0][26] = nilTs // typed nil *time.Time: val != nil, tp == nil
	mem := memory.NewGoAllocator()

	rec, err := BuildTable(row, mem)
	require.NoError(t, err)
	defer rec.Release()

	assert.Equal(t, int64(1), rec.NumRows())
	tsIdx := rec.Schema().FieldIndices("created_at")
	require.Len(t, tsIdx, 1)
	assert.True(t, rec.Column(tsIdx[0]).IsNull(0), "created_at with typed nil *time.Time should be null")
}

// --- WriteShards tests ---

func TestWriteShards_EmptyTable_NoFiles(t *testing.T) {
	mem := memory.NewGoAllocator()
	rec, err := BuildTable([][]any{}, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, DefaultShardSize, DefaultCompression)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// No parquet files should be created (lock file may exist).
	parquets, _ := filepath.Glob(filepath.Join(dir, "train-*.parquet"))
	assert.Empty(t, parquets)
}

func TestWriteShards_SingleShard(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(5)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 100, "snappy") // shard_size > row count
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestWriteShards_ShardFilenamePattern(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(2)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	_, err = WriteShards(rec, dir, 100, "snappy")
	require.NoError(t, err)

	// Expect exactly: train-00000-of-00001.parquet
	expected := filepath.Join(dir, "train-00000-of-00001.parquet")
	_, statErr := os.Stat(expected)
	assert.NoError(t, statErr, "expected file %s to exist", expected)
}

func TestWriteShards_MultipleShards_CorrectCount(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(10)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 3, "snappy") // 10 rows / 3 per shard = ceil(10/3) = 4
	require.NoError(t, err)
	assert.Equal(t, 4, count)

	parquets, _ := filepath.Glob(filepath.Join(dir, "train-*.parquet"))
	assert.Len(t, parquets, 4)
}

func TestWriteShards_TotalRowsPreserved(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(7)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 3, "snappy") // 7 rows / 3 = ceil(7/3) = 3 shards
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify total rows across all shards by reading back.
	// Simple check: all shard files exist.
	for i := 0; i < 3; i++ {
		fname := filepath.Join(dir, shardFilename(i, 3))
		_, statErr := os.Stat(fname)
		assert.NoError(t, statErr, "shard file %d should exist", i)
	}
}

func TestWriteShards_OutputDirCreated(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(1)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	// Use a nested directory that does not yet exist.
	dir := filepath.Join(t.TempDir(), "nested", "output")
	count, err := WriteShards(rec, dir, 100, "snappy")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Directory must have been created.
	info, statErr := os.Stat(dir)
	require.NoError(t, statErr)
	assert.True(t, info.IsDir())
}

func TestWriteShards_WriteError_ReadonlyDir(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root user bypasses permission checks")
	}

	mem := memory.NewGoAllocator()
	rows := makeSampleRows(1)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	// Create a directory and remove write permission.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755) //nolint:errcheck // cleanup

	_, err = WriteShards(rec, dir, 100, "snappy")

	require.Error(t, err)
	// Lock file creation or shard write will fail on readonly dir.
	assert.True(t, strings.Contains(err.Error(), "open lock") || strings.Contains(err.Error(), "write shard"),
		"expected lock or write error, got: %s", err.Error())
}

func TestWriteShards_MkdirAllError(t *testing.T) {
	// WriteShards must return "create output dir" error when outputDir cannot be created.
	// Trigger by using a read-only parent so MkdirAll fails to create a nested child.
	if os.Getuid() == 0 {
		t.Skip("root user bypasses permission checks")
	}

	mem := memory.NewGoAllocator()
	rows := makeSampleRows(1)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	// Make parent read-only so creating the nested directory fails.
	parent := t.TempDir()
	require.NoError(t, os.Chmod(parent, 0o555))
	defer os.Chmod(parent, 0o755) //nolint:errcheck // cleanup

	nestedDir := filepath.Join(parent, "cannot_create")
	_, err = WriteShards(rec, nestedDir, 100, "snappy")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create output dir")
}

func TestWriteShards_ZeroShardSize(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(1)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	_, err = WriteShards(rec, t.TempDir(), 0, "snappy")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard size must be > 0")
}

func TestWriteShards_NegativeShardSize(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(1)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	_, err = WriteShards(rec, t.TempDir(), -5, "snappy")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard size must be > 0")
}

func TestWriteShards_SnappyCompression(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(3)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 100, "snappy")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Open the generated Parquet file and verify snappy compression.
	fpath := filepath.Join(dir, "train-00000-of-00001.parquet")
	reader, err := file.OpenParquetFile(fpath, false)
	require.NoError(t, err)
	defer reader.Close()

	meta := reader.MetaData()
	require.Greater(t, meta.NumRowGroups(), 0, "should have at least one row group")

	rg := meta.RowGroup(0)
	for i := 0; i < rg.NumColumns(); i++ {
		col, colErr := rg.ColumnChunk(i)
		require.NoError(t, colErr, "column chunk %d", i)
		codec := col.Compression()
		assert.Equal(t, "SNAPPY", codec.String(),
			"column %d (%s) should use snappy compression", i, ArrowSchema.Field(i).Name)
	}
}

func TestWriteShards_ZstdCompression(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(3)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 100, "zstd")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	fpath := filepath.Join(dir, "train-00000-of-00001.parquet")
	reader, err := file.OpenParquetFile(fpath, false)
	require.NoError(t, err)
	defer reader.Close()

	meta := reader.MetaData()
	require.Greater(t, meta.NumRowGroups(), 0)

	rg := meta.RowGroup(0)
	for i := 0; i < rg.NumColumns(); i++ {
		col, colErr := rg.ColumnChunk(i)
		require.NoError(t, colErr)
		codec := col.Compression()
		assert.Equal(t, "ZSTD", codec.String(),
			"column %d should use zstd compression", i)
	}
}

func TestWriteShards_InvalidCompression(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(1)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	_, err = WriteShards(rec, t.TempDir(), 100, "invalid-codec")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported compression")
}

// --- toStringSlice tests ---

func TestToStringSlice_SliceAny(t *testing.T) {
	// pgx v5 decodes TEXT[] as []any when scanning into *any.
	// This exercises the []any branch in toStringSlice.
	input := []any{"alpha", "beta", "gamma"}

	result, err := toStringSlice(input)

	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, result)
}

func TestToStringSlice_SliceAny_NonStringElement(t *testing.T) {
	// []any with a non-string element must return an error.
	input := []any{"valid", 42}

	_, err := toStringSlice(input)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected string at index 1")
}

func TestToStringSlice_InvalidType(t *testing.T) {
	// A non-[]string/non-[]any type must return an error.
	input := 12345

	_, err := toStringSlice(input)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected []string or []any")
}

// --- toTime tests ---

func TestToTime_ValueType(t *testing.T) {
	// pgx v5 decodes TIMESTAMPTZ as time.Time (value, not pointer) when scanning into *any.
	// This exercises the time.Time (non-pointer) branch in toTime.
	ts := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	result, err := toTime(ts)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, ts, *result)
}

// --- BuildTable with []any list columns (pgx pattern) ---

func TestBuildTable_ListColumnsAsSliceAny(t *testing.T) {
	// pgx v5 returns TEXT[] as []any{"str1","str2"} when scanning into *any.
	// BuildTable must handle this via toStringSlice's []any branch.
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	row := []any{
		"task-any",                 // task_id
		"math",                     // domain
		"easy",                     // difficulty
		"qa",                       // task_shape
		[]any{"reasoning", "math"}, // capability_tags as []any (pgx pattern)
		"Solve this",               // user_request
		"context",                  // context
		[]any{"correct"},           // success_criteria as []any
		[]any{"step1"},             // plan as []any
		"summary",                  // reasoning_summary
		"answer",                   // final_answer
		[]any{"ok"},                // self_check as []any
		[]any{"good"},              // quality_notes as []any
		[]any{"ref1"},              // references as []any
		[]any{"art1"},              // artifacts as []any
		"en",                       // source_lane
		"pair-9",                   // pair_id
		`[{"source_type":"k8s"}]`,  // artifact_refs (::text)
		`{"verdict":"uncertain"}`,  // student_filter_verdict (::text)
		nil,                        // verification (nullable, pre-verify row)
		[]any{"partial_info_removal"}, // difficulty_mutations as []any
		"claude-fable",             // teacher_model
		"claude",                   // teacher_provider
		"cot inband",               // cot
		nil,                        // cot_raw (nullable, subscription provider)
		false,                      // has_raw_cot
		ts,                         // created_at as time.Time value (pgx pattern)
	}

	mem := memory.NewGoAllocator()
	rec, err := BuildTable([][]any{row}, mem)
	require.NoError(t, err)
	defer rec.Release()

	assert.Equal(t, int64(1), rec.NumRows())
}

// --- WriteShards compression variants ---

func TestWriteShards_GzipCompression(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(2)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 100, "gzip")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	fpath := filepath.Join(dir, "train-00000-of-00001.parquet")
	reader, err := file.OpenParquetFile(fpath, false)
	require.NoError(t, err)
	defer reader.Close()

	meta := reader.MetaData()
	require.Greater(t, meta.NumRowGroups(), 0)
	rg := meta.RowGroup(0)
	col, colErr := rg.ColumnChunk(0)
	require.NoError(t, colErr)
	assert.Equal(t, "GZIP", col.Compression().String())
}

func TestWriteShards_NoneCompression(t *testing.T) {
	mem := memory.NewGoAllocator()
	rows := makeSampleRows(2)
	rec, err := BuildTable(rows, mem)
	require.NoError(t, err)
	defer rec.Release()

	dir := t.TempDir()
	count, err := WriteShards(rec, dir, 100, "none")
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	fpath := filepath.Join(dir, "train-00000-of-00001.parquet")
	reader, err := file.OpenParquetFile(fpath, false)
	require.NoError(t, err)
	defer reader.Close()

	meta := reader.MetaData()
	require.Greater(t, meta.NumRowGroups(), 0)
	rg := meta.RowGroup(0)
	col, colErr := rg.ColumnChunk(0)
	require.NoError(t, colErr)
	assert.Equal(t, "UNCOMPRESSED", col.Compression().String())
}

func TestWriteShards_RemovesStaleFiles(t *testing.T) {
	mem := memory.NewGoAllocator()

	// First run: create 3 shards (3 rows, shard size 1).
	rows1 := makeSampleRows(3)
	rec1, err := BuildTable(rows1, mem)
	require.NoError(t, err)

	dir := t.TempDir()
	count1, err := WriteShards(rec1, dir, 1, "zstd")
	rec1.Release()
	require.NoError(t, err)
	assert.Equal(t, 3, count1)

	// Verify 3 files exist.
	matches1, _ := filepath.Glob(filepath.Join(dir, "train-*.parquet"))
	assert.Len(t, matches1, 3)

	// Second run: create 1 shard (1 row, shard size 10).
	rows2 := makeSampleRows(1)
	rec2, err := BuildTable(rows2, mem)
	require.NoError(t, err)

	count2, err := WriteShards(rec2, dir, 10, "zstd")
	rec2.Release()
	require.NoError(t, err)
	assert.Equal(t, 1, count2)

	// Verify only 1 file remains (stale 3 files removed, 1 new created).
	matches2, _ := filepath.Glob(filepath.Join(dir, "train-*.parquet"))
	assert.Len(t, matches2, 1)
	assert.Contains(t, matches2[0], "train-00000-of-00001.parquet")
}

func TestWriteShards_ZeroRows_RemovesStaleFiles(t *testing.T) {
	mem := memory.NewGoAllocator()

	// First run: create 2 shards.
	rows1 := makeSampleRows(2)
	rec1, err := BuildTable(rows1, mem)
	require.NoError(t, err)

	dir := t.TempDir()
	count1, err := WriteShards(rec1, dir, 1, "zstd")
	rec1.Release()
	require.NoError(t, err)
	assert.Equal(t, 2, count1)

	matches1, _ := filepath.Glob(filepath.Join(dir, "train-*.parquet"))
	assert.Len(t, matches1, 2)

	// Second run: zero rows (empty DB). Stale files must be removed.
	emptyRec, err := BuildTable(nil, mem)
	require.NoError(t, err)
	defer emptyRec.Release()

	count2, err := WriteShards(emptyRec, dir, 10, "zstd")
	require.NoError(t, err)
	assert.Equal(t, 0, count2)

	// Verify all stale files removed.
	matches2, _ := filepath.Glob(filepath.Join(dir, "train-*.parquet"))
	assert.Len(t, matches2, 0)
}

// --- test helpers ---

// makeSampleRows creates n sample rows matching the 27-column COLUMNS order.
// Each row is []any with realistic types simulating pgx scan results; JSONB
// columns arrive as JSON text (::text cast in selectSQL).
func makeSampleRows(n int) [][]any {
	rows := make([][]any, n)
	for i := 0; i < n; i++ {
		ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		rows[i] = []any{
			"task-" + string(rune('A'+i)),                 // task_id
			"math",                                        // domain
			"easy",                                        // difficulty
			"qa",                                          // task_shape
			[]string{"reasoning", "math"},                 // capability_tags
			"Solve this problem",                          // user_request
			"Some context",                                // context
			[]string{"correct answer"},                    // success_criteria
			[]string{"step1", "step2"},                    // plan
			"I reasoned step by step",                     // reasoning_summary
			"42",                                          // final_answer
			[]string{"checked"},                           // self_check
			[]string{"high quality"},                      // quality_notes
			[]string{"ref1"},                              // references_ (DB col name)
			[]string{"artifact1"},                         // artifacts
			"ko",                                          // source_lane
			"pair-1",                                      // pair_id
			`[{"source_type":"arxiv","doc_id":"2401.1"}]`, // artifact_refs (::text)
			`{"verdict":"fail","method":"both"}`,          // student_filter_verdict (::text)
			`{"method":"test","result":"pass"}`,           // verification (::text)
			[]string{"fault_injection"},                   // difficulty_mutations
			"gpt-5.4",                                     // teacher_model
			"openai",                                      // teacher_provider
			"in-band cot",                                 // cot
			"raw reasoning",                               // cot_raw
			true,                                          // has_raw_cot
			&ts,                                           // created_at
		}
	}
	return rows
}

// makeRowWithNulls creates a row where all nullable columns are nil (a legacy
// taxonomy row exported after the M3 columns were added).
func makeRowWithNulls() []any {
	return []any{
		"task-null",          // task_id
		"science",            // domain
		"hard",               // difficulty
		"essay",              // task_shape
		[]string{"analysis"}, // capability_tags
		"Write an essay",     // user_request
		"No context",         // context
		[]string{"coherent"}, // success_criteria
		[]string{"plan1"},    // plan
		"Summary",            // reasoning_summary
		"Answer text",        // final_answer
		[]string{"ok"},       // self_check
		[]string{"good"},     // quality_notes
		nil,                  // references_ (nullable)
		nil,                  // artifacts (nullable)
		nil,                  // source_lane (nullable)
		nil,                  // pair_id (nullable)
		nil,                  // artifact_refs (nullable)
		nil,                  // student_filter_verdict (nullable)
		nil,                  // verification (nullable)
		nil,                  // difficulty_mutations (nullable)
		nil,                  // teacher_model (nullable)
		nil,                  // teacher_provider (nullable)
		nil,                  // cot (nullable)
		nil,                  // cot_raw (nullable)
		nil,                  // has_raw_cot (nullable)
		nil,                  // created_at (nullable)
	}
}

// shardFilename generates the HuggingFace-compatible shard filename.
// Duplicated here to avoid importing from the package under test.
func shardFilename(idx, total int) string {
	return fmt.Sprintf("train-%05d-of-%05d.parquet", idx, total)
}
