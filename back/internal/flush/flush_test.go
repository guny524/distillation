package flush

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
)

// bufferState is a minimal in-memory stand-in for distillation_pairs used to
// drive the flush cycle through the db.Querier mock: COUNT(*), MAX(id),
// SELECT ... WHERE id <= $1, and DELETE ... WHERE id <= $1.
type bufferState struct {
	count       int
	maxID       int64
	rowsUpTo    [][]any    // returned by FetchRowsUpTo regardless of the arg
	deleteCalls [][]string // task_id slices passed to DeleteByTaskIDs
	fetchArgs   []int64    // maxID values passed to FetchRowsUpTo
}

func newQuerier(b *bufferState) *dbtest.MockQuerier {
	return &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			switch {
			case strings.Contains(sql, "COUNT(*)"):
				return &dbtest.MockRow{ScanFn: func(dest ...any) error {
					// sqlcgen.CountPairs scans COUNT(*) into an int64.
					*(dest[0].(*int64)) = int64(b.count)
					return nil
				}}
			case strings.Contains(sql, "MAX(id)"):
				return &dbtest.MockRow{ScanFn: func(dest ...any) error {
					*(dest[0].(*int64)) = b.maxID
					return nil
				}}
			default:
				return &dbtest.MockRow{ScanFn: func(...any) error {
					return fmt.Errorf("unexpected QueryRow: %s", sql)
				}}
			}
		},
		QueryFn: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			if !strings.Contains(sql, "WHERE id <= $1") {
				return nil, fmt.Errorf("unexpected Query: %s", sql)
			}
			b.fetchArgs = append(b.fetchArgs, args[0].(int64))
			return dbtest.NewMockRowsAny(b.rowsUpTo), nil
		},
		ExecFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			if !strings.Contains(sql, "DELETE") {
				return pgconn.NewCommandTag(""), fmt.Errorf("unexpected Exec: %s", sql)
			}
			ids := args[0].([]string)
			b.deleteCalls = append(b.deleteCalls, ids)
			return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", len(ids))), nil
		},
	}
}

// sampleRow builds one valid 27-column distillation_pairs row (pgx *any scan
// shape) so exporter.BuildTable accepts it.
func sampleRow(i int) []any {
	ts := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	return []any{
		fmt.Sprintf("task-%d", i), "math", "easy", "qa",
		[]string{"reasoning"}, "request", "context",
		[]string{"correct"}, []string{"step1"}, "summary",
		"answer", []string{"ok"}, []string{"good"},
		[]string{"ref1"}, []string{"art1"},
		"ko", "pair-1", `[{"source_type":"arxiv"}]`, `{"verdict":"pass"}`,
		`{"result":"pass"}`, []string{"fault_injection"}, "gpt-5.4", "openai",
		"in-band cot", "raw reasoning", true,
		&ts,
	}
}

func sampleRows(n int) [][]any {
	rows := make([][]any, n)
	for i := 0; i < n; i++ {
		rows[i] = sampleRow(i)
	}
	return rows
}

func testFlusher(t *testing.T, cfg Config, q *dbtest.MockQuerier) *Flusher {
	t.Helper()
	f := New(cfg, q)
	f.Log = &strings.Builder{}
	f.Now = func() time.Time { return time.Date(2026, 7, 13, 9, 30, 0, 0, time.UTC) }
	return f
}

func enabledConfig(dir string, threshold int) Config {
	return Config{
		Enabled:       true,
		ExportDir:        dir,
		ThresholdRows: threshold,
		ShardSize:     50000,
		Compression:   "zstd",
	}
}

// --- MaybeFlush threshold gating ---

func TestMaybeFlush_Disabled_NoOp(t *testing.T) {
	b := &bufferState{count: 1_000_000, maxID: 10, rowsUpTo: sampleRows(3)}
	f := testFlusher(t, Config{Enabled: false}, newQuerier(b))

	flushed, n, err := f.MaybeFlush(context.Background())

	require.NoError(t, err)
	assert.False(t, flushed)
	assert.Zero(t, n)
	assert.Empty(t, b.deleteCalls, "disabled flush must never delete")
}

func TestMaybeFlush_BelowThreshold_NoFlushNoDelete(t *testing.T) {
	b := &bufferState{count: 5, maxID: 5, rowsUpTo: sampleRows(5)}
	f := testFlusher(t, enabledConfig(t.TempDir(), 50000), newQuerier(b))

	flushed, n, err := f.MaybeFlush(context.Background())

	require.NoError(t, err)
	assert.False(t, flushed)
	assert.Zero(t, n)
	assert.Empty(t, b.deleteCalls, "below threshold must not delete")
	assert.Empty(t, b.fetchArgs, "below threshold must not fetch/export")
}

func TestMaybeFlush_AtThreshold_FlushesAndClears(t *testing.T) {
	dir := t.TempDir()
	rows := sampleRows(3)
	b := &bufferState{count: 3, maxID: 42, rowsUpTo: rows}
	f := testFlusher(t, enabledConfig(dir, 3), newQuerier(b))

	flushed, n, err := f.MaybeFlush(context.Background())

	require.NoError(t, err)
	assert.True(t, flushed)
	assert.Equal(t, 3, n)

	// The batch dir exists with a Parquet shard.
	batches, _ := filepath.Glob(filepath.Join(dir, "batch-*"))
	require.Len(t, batches, 1, "exactly one batch dir")
	shards, _ := filepath.Glob(filepath.Join(batches[0], "train-*.parquet"))
	assert.NotEmpty(t, shards, "batch must contain a parquet shard")

	// No temp dir left behind.
	tmp, _ := filepath.Glob(filepath.Join(dir, ".batch-*.tmp"))
	assert.Empty(t, tmp, "temp dir must be renamed away")

	// Fetch was bounded by the snapshot; delete removed exactly the exported
	// task_ids (not id <= maxID).
	require.Equal(t, []int64{42}, b.fetchArgs)
	require.Len(t, b.deleteCalls, 1)
	assert.Equal(t, []string{"task-0", "task-1", "task-2"}, b.deleteCalls[0])

	// The delete committed, so the pending-delete manifest was retired: nothing
	// for a later recovery scan to revisit.
	manifests, _ := filepath.Glob(filepath.Join(dir, "batch-*", manifestName))
	assert.Empty(t, manifests, "manifest retired after successful delete")
}

// --- exactly-once: a crash between rename and delete is recovered, not
// re-exported ---

// TestFlush_CrashBeforeDelete_RecoveredNotReexported: first flush promotes the
// batch but its buffer delete fails (the crash window). The next flush must
// COMPLETE that delete from the batch's pending-delete manifest and must not
// export the same rows into a second batch.
func TestFlush_CrashBeforeDelete_RecoveredNotReexported(t *testing.T) {
	dir := t.TempDir()
	rows := sampleRows(3)

	// Run 1: delete fails after the batch dir is promoted.
	b1 := &bufferState{count: 3, maxID: 42, rowsUpTo: rows}
	q1 := newQuerier(b1)
	q1.ExecFn = func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag(""), fmt.Errorf("connection reset before DELETE")
	}
	f1 := testFlusher(t, enabledConfig(dir, 3), q1)
	_, err := f1.Flush(context.Background())
	require.Error(t, err, "the failed delete is surfaced")

	batches, _ := filepath.Glob(filepath.Join(dir, "batch-*"))
	require.Len(t, batches, 1, "the batch was promoted before the crash")
	manifests, _ := filepath.Glob(filepath.Join(dir, "batch-*", manifestName))
	require.Len(t, manifests, 1, "the pending-delete manifest survives the crash")

	// Run 2: recovery completes the delete; the (already exported) rows are gone
	// from the buffer, so nothing is exported again.
	b2 := &bufferState{count: 0, maxID: 0}
	f2 := testFlusher(t, enabledConfig(dir, 3), newQuerier(b2))
	n, err := f2.Flush(context.Background())
	require.NoError(t, err)
	assert.Zero(t, n, "nothing new to export")
	require.Len(t, b2.deleteCalls, 1, "recovery completed the pending delete")
	assert.Equal(t, []string{"task-0", "task-1", "task-2"}, b2.deleteCalls[0])

	batches, _ = filepath.Glob(filepath.Join(dir, "batch-*"))
	assert.Len(t, batches, 1, "the rows exist in exactly ONE batch (no duplicate export)")
	manifests, _ = filepath.Glob(filepath.Join(dir, "batch-*", manifestName))
	assert.Empty(t, manifests, "recovered manifest retired")
}

// TestFlush_RecoveryRunsBeforeExport: a leftover manifest is completed before the
// new snapshot/export, and the new flush then proceeds normally.
func TestFlush_RecoveryRunsBeforeExport(t *testing.T) {
	dir := t.TempDir()
	// A previously promoted batch whose delete never ran.
	stale := filepath.Join(dir, "batch-20260101-000000-id7")
	require.NoError(t, os.MkdirAll(stale, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stale, manifestName), []byte(`["old-1","old-2"]`), 0o644))

	rows := sampleRows(2)
	b := &bufferState{count: 2, maxID: 9, rowsUpTo: rows}
	f := testFlusher(t, enabledConfig(dir, 2), newQuerier(b))

	n, err := f.Flush(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	require.Len(t, b.deleteCalls, 2)
	assert.Equal(t, []string{"old-1", "old-2"}, b.deleteCalls[0], "recovery delete runs FIRST")
	assert.Equal(t, []string{"task-0", "task-1"}, b.deleteCalls[1], "then the new flush's own delete")
	assert.NoFileExists(t, filepath.Join(stale, manifestName), "stale manifest retired")
}

// --- atomicity: snapshot bounds the delete (concurrent insert survives) ---

func TestFlush_DeletesOnlyUpToSnapshot(t *testing.T) {
	// maxID snapshot is 100; rows inserted after (id > 100) are represented by
	// the buffer count being larger than the exported set. The flush fetches
	// with id <= 100 and deletes exactly the exported task_ids (never a higher
	// bound and never an un-exported row), so newer rows remain for next flush.
	dir := t.TempDir()
	b := &bufferState{count: 130, maxID: 100, rowsUpTo: sampleRows(100)}
	f := testFlusher(t, enabledConfig(dir, 50), newQuerier(b))

	flushed, n, err := f.MaybeFlush(context.Background())

	require.NoError(t, err)
	assert.True(t, flushed)
	assert.Equal(t, 100, n)
	require.Equal(t, []int64{100}, b.fetchArgs, "export bounded by snapshot")
	// Delete set == exported set (100 task_ids), not id <= maxID.
	require.Len(t, b.deleteCalls, 1)
	assert.Len(t, b.deleteCalls[0], 100)
	assert.Equal(t, "task-0", b.deleteCalls[0][0])
	assert.Equal(t, "task-99", b.deleteCalls[0][99])
}

// --- write failure must not delete (no data loss) ---

func TestFlush_WriteFailure_NoDelete(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	// A read-only export dir makes the lock-file/shard write fail; the buffer must
	// be left untouched (no DELETE) so nothing is lost.
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o555))
	defer os.Chmod(dir, 0o755) //nolint:errcheck // cleanup

	b := &bufferState{count: 3, maxID: 42, rowsUpTo: sampleRows(3)}
	f := testFlusher(t, enabledConfig(dir, 3), newQuerier(b))

	flushed, n, err := f.MaybeFlush(context.Background())

	require.Error(t, err)
	assert.False(t, flushed)
	assert.Zero(t, n)
	assert.Empty(t, b.deleteCalls, "write failure must not delete any row")
}

func TestFlush_EmptyBuffer_NoDelete(t *testing.T) {
	// maxID == 0 (empty buffer) even though count somehow met threshold: nothing
	// to export or delete.
	dir := t.TempDir()
	b := &bufferState{count: 3, maxID: 0, rowsUpTo: nil}
	f := testFlusher(t, enabledConfig(dir, 1), newQuerier(b))

	flushed, n, err := f.MaybeFlush(context.Background())

	require.NoError(t, err)
	assert.False(t, flushed)
	assert.Zero(t, n)
	assert.Empty(t, b.deleteCalls)
}

// --- error propagation ---

func TestMaybeFlush_CountError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(...any) error { return fmt.Errorf("conn refused") }}
		},
	}
	f := testFlusher(t, enabledConfig(t.TempDir(), 1), q)

	_, _, err := f.MaybeFlush(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "count rows")
}

// --- Config.Validate ---

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"disabled is always valid", Config{Enabled: false}, ""},
		{"default is valid", DefaultConfig(), ""},
		{"empty export dir", Config{Enabled: true, ExportDir: "", ThresholdRows: 1, ShardSize: 1, Compression: "zstd"}, "export_dir"},
		{"zero threshold", Config{Enabled: true, ExportDir: "/x", ThresholdRows: 0, ShardSize: 1, Compression: "zstd"}, "threshold_rows"},
		{"zero shard size", Config{Enabled: true, ExportDir: "/x", ThresholdRows: 1, ShardSize: 0, Compression: "zstd"}, "shard_size"},
		{"bad compression", Config{Enabled: true, ExportDir: "/x", ThresholdRows: 1, ShardSize: 1, Compression: "brotli"}, "compression"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	assert.True(t, c.Enabled)
	assert.Equal(t, DefaultThresholdRows, c.ThresholdRows)
	assert.NotEmpty(t, c.ExportDir)
	assert.NoError(t, c.Validate())
}
