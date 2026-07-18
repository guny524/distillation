// Package flush implements the DB-buffer -> export-volume flush -> DB-clear cycle.
//
// distillation_pairs is treated as a bounded buffer, not the final store. Rows
// accumulate there until the buffer reaches a threshold; a flush then merge-
// exports the whole buffer to a single compressed Parquet batch on the export volume and
// deletes the flushed rows from the DB. Writing large merged shards (instead of
// many tiny files straight to the export volume) is what lets compression and downstream
// training I/O be efficient.
//
// Atomicity (exactly-once, no loss): each flush snapshots MAX(id) under an
// exclusive export-volume lock, exports rows with id <= that snapshot into a
// hidden temp dir together with a pending-delete manifest of their task_ids,
// fsyncs and verifies the Parquet row count, atomically renames the temp dir to
// the final batch dir, and only THEN deletes those rows (retiring the manifest
// on success). A crash before the rename leaves the buffer intact and the temp
// dir is discarded; a crash between rename and delete leaves the manifest in the
// promoted batch, and the next flush COMPLETES that delete (idempotent) before
// exporting -- so the rows appear in exactly one batch, never twice and never
// lost.
package flush

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/exporter"
	"github.com/guny524/distillation/internal/loader"
)

// DefaultThresholdRows is the buffer size that triggers a flush. It equals the
// default shard size so a flush produces at least one full-size shard.
const DefaultThresholdRows = 50000

const (
	// batchPrefix names each flush's output directory (batch-<ts>-id<maxid>).
	batchPrefix = "batch-"
	// flushLockName serializes flushers across pods on the shared export volume.
	flushLockName = ".flush.lock"
	// manifestName is the per-batch task_id manifest. It exists exactly while the
	// batch's buffer delete is PENDING: written with the shards, removed right
	// after the delete commits. A manifest left behind marks a flush that crashed
	// between the batch rename and the buffer delete; the next flush completes
	// that delete (idempotent) instead of re-exporting the same rows into a second
	// batch -- this is what upgrades the cycle from at-least-once to exactly-once.
	manifestName = "pending-delete.json"
)

// Config parameterizes the flush cycle. Loaded from the run config's `flush:`
// section (see runner.Config); zero value is a disabled no-op.
type Config struct {
	// Enabled turns the auto-flush cycle on. When false, MaybeFlush is a no-op
	// and distillation_pairs is never cleared.
	Enabled bool `yaml:"enabled"`
	// ExportDir is the PVC mount path where batch directories are written. Batches
	// live in per-flush subdirectories so they never collide with each other or
	// with a manual `distill export` (which writes train-*.parquet at the root).
	ExportDir string `yaml:"export_dir"`
	// ThresholdRows is the buffer row count at/above which a flush runs.
	ThresholdRows int `yaml:"threshold_rows"`
	// ShardSize is the max rows per Parquet shard within a batch.
	ShardSize int `yaml:"shard_size"`
	// Compression is the Parquet codec (zstd, snappy, gzip, none).
	Compression string `yaml:"compression"`
}

// DefaultConfig returns the recommended flush settings: enabled, writing to the
// same export root as the manual exporter, flushing at DefaultThresholdRows.
func DefaultConfig() Config {
	return Config{
		Enabled:       true,
		ExportDir:        exporter.DefaultOutputDir,
		ThresholdRows: DefaultThresholdRows,
		ShardSize:     exporter.DefaultShardSize,
		Compression:   exporter.DefaultCompression,
	}
}

// Validate checks the config invariants the flush cycle depends on. A disabled
// config is always valid (the fields are ignored).
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.ExportDir == "" {
		return fmt.Errorf("export_dir must not be empty when flush is enabled")
	}
	if c.ThresholdRows <= 0 {
		return fmt.Errorf("threshold_rows must be > 0, got %d", c.ThresholdRows)
	}
	if c.ShardSize <= 0 {
		return fmt.Errorf("shard_size must be > 0, got %d", c.ShardSize)
	}
	if _, ok := exporter.ValidCompressions[c.Compression]; !ok {
		return fmt.Errorf("unsupported compression %q (valid: zstd, snappy, gzip, none)", c.Compression)
	}
	return nil
}

// Flusher runs the buffer -> export volume -> clear cycle against one DB connection.
type Flusher struct {
	cfg Config
	db  db.Querier
	mem memory.Allocator
	// Now supplies the batch timestamp; tests override it.
	Now func() time.Time
	// Log receives progress lines (stderr in production).
	Log io.Writer
}

// New wires a Flusher with production defaults (real clock, stderr log).
func New(cfg Config, q db.Querier) *Flusher {
	return &Flusher{
		cfg: cfg,
		db:  q,
		mem: memory.DefaultAllocator,
		Now: time.Now,
		Log: os.Stderr,
	}
}

func (f *Flusher) logf(format string, args ...any) {
	fmt.Fprintf(f.Log, format+"\n", args...)
}

// MaybeFlush flushes the buffer only when it has reached ThresholdRows. It
// returns (flushed, rowsFlushed, err). A disabled config or a below-threshold
// buffer is a clean no-op: (false, 0, nil).
func (f *Flusher) MaybeFlush(ctx context.Context) (bool, int, error) {
	if !f.cfg.Enabled {
		return false, 0, nil
	}
	count, err := loader.CountRows(ctx, f.db)
	if err != nil {
		return false, 0, fmt.Errorf("count rows: %w", err)
	}
	if count < f.cfg.ThresholdRows {
		f.logf("[flush] buffer has %d row(s) < threshold %d, skipping", count, f.cfg.ThresholdRows)
		return false, 0, nil
	}
	n, err := f.Flush(ctx)
	if err != nil {
		return false, 0, err
	}
	// n == 0 means the buffer was emptied between the count and the snapshot
	// (or held only ids already gone): nothing was written or cleared.
	return n > 0, n, nil
}

// Flush exports the current buffer (rows up to a snapshotted high-water mark) to
// a compressed Parquet batch on the export volume, then deletes exactly those rows. It is
// safe to call unconditionally; MaybeFlush is the threshold-gated entry point.
func (f *Flusher) Flush(ctx context.Context) (int, error) {
	if err := os.MkdirAll(f.cfg.ExportDir, 0o755); err != nil {
		return 0, fmt.Errorf("create export dir %s: %w", f.cfg.ExportDir, err)
	}

	// Serialize flushers across pods on the shared export volume (fcntl, NFS-safe). A
	// second flusher fails fast here rather than double-exporting.
	lock, err := exporter.AcquireLock(f.cfg.ExportDir, flushLockName)
	if err != nil {
		return 0, fmt.Errorf("acquire flush lock: %w", err)
	}
	defer exporter.ReleaseLock(lock)

	// Complete any delete a previous flush crashed out of (batch promoted, rows
	// not yet cleared) BEFORE snapshotting, so those rows are removed from the
	// buffer instead of being exported a second time.
	if err := f.recoverPendingDeletes(ctx); err != nil {
		return 0, err
	}

	// Snapshot the high-water mark INSIDE the lock: this is the atomicity
	// anchor. Only rows with id <= maxID are exported and deleted; rows inserted
	// after this point (higher ids) survive for the next flush.
	maxID, err := loader.MaxID(ctx, f.db)
	if err != nil {
		return 0, fmt.Errorf("snapshot max id: %w", err)
	}
	if maxID == 0 {
		return 0, nil // empty buffer
	}

	rows, err := exporter.FetchRowsUpTo(ctx, f.db, maxID)
	if err != nil {
		return 0, fmt.Errorf("fetch rows: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	// Capture the exact task_ids exported (column 0) so the post-write delete
	// removes only these rows, not "id <= maxID". This closes a TOCTOU window:
	// a row committing with id <= maxID after this fetch is not in the export
	// and must survive to the next flush rather than be deleted un-exported.
	taskIDs, err := exportedTaskIDs(rows)
	if err != nil {
		return 0, err
	}

	rec, err := exporter.BuildTable(rows, f.mem)
	if err != nil {
		return 0, fmt.Errorf("build table: %w", err)
	}
	defer rec.Release()

	// Write to a hidden temp dir, fsync+verify, then atomically rename to the
	// final batch dir. The batch becomes visible (and the rows get deleted) only
	// after the bytes are durable and the row count checks out.
	stamp := f.Now().UTC().Format("20060102-150405")
	batchName := fmt.Sprintf("%s%s-id%d", batchPrefix, stamp, maxID)
	finalDir := filepath.Join(f.cfg.ExportDir, batchName)
	tmpDir := filepath.Join(f.cfg.ExportDir, "."+batchName+".tmp")

	// A leftover temp dir from a crashed flush with the same stamp+id holds no
	// authoritative data (it was never renamed); clear it before reusing.
	_ = os.RemoveAll(tmpDir)

	numShards, err := exporter.WriteShards(rec, tmpDir, f.cfg.ShardSize, f.cfg.Compression)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return 0, fmt.Errorf("write shards: %w", err)
	}
	// The pending-delete manifest rides in the batch dir and is fsynced with the
	// shards, so after the rename it is durable exactly as long as the delete is
	// still owed (see manifestName).
	if err := writeManifest(tmpDir, taskIDs); err != nil {
		_ = os.RemoveAll(tmpDir)
		return 0, fmt.Errorf("write manifest: %w", err)
	}
	if err := syncDir(tmpDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return 0, fmt.Errorf("sync tmp dir: %w", err)
	}
	if err := exporter.VerifyParquetDir(tmpDir, len(rows)); err != nil {
		_ = os.RemoveAll(tmpDir)
		return 0, fmt.Errorf("verify parquet: %w", err)
	}
	if err := os.Rename(tmpDir, finalDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return 0, fmt.Errorf("promote batch %s: %w", batchName, err)
	}
	// fsync the parent so the rename survives a crash before the DB delete.
	if err := syncPath(f.cfg.ExportDir); err != nil {
		return 0, fmt.Errorf("sync export dir: %w", err)
	}

	// The batch is durable and verified. Only now clear the flushed rows,
	// deleting exactly the exported task_ids (never id <= maxID, which could
	// include a concurrently-committed row that was not in the export).
	deleted, err := loader.DeleteByTaskIDs(ctx, f.db, taskIDs)
	if err != nil {
		// Data is safe on the export volume and the batch's pending-delete
		// manifest survives, so the next flush completes this delete instead of
		// re-exporting the rows. Surface the failure.
		return 0, fmt.Errorf("clear buffer up to id %d (batch %s written, delete pending recovery): %w",
			maxID, batchName, err)
	}
	// Delete committed: retire the manifest so recovery no longer revisits this
	// batch. A failure here is harmless -- the next recovery re-runs the delete
	// (a no-op) and retries the removal.
	if err := os.Remove(filepath.Join(finalDir, manifestName)); err != nil {
		f.logf("[flush] warning: could not retire manifest for %s (recovery will no-op it): %v", batchName, err)
	} else if err := syncPath(finalDir); err != nil {
		f.logf("[flush] warning: sync %s after manifest retire: %v", finalDir, err)
	}

	f.logf("[flush] wrote %d row(s) in %d shard(s) to %s, cleared %d buffer row(s)",
		len(rows), numShards, finalDir, deleted)
	return len(rows), nil
}

// writeManifest records the batch's exported task_ids as pending-delete state.
func writeManifest(dir string, taskIDs []string) error {
	b, err := json.Marshal(taskIDs)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, manifestName), b, 0o644)
}

// recoverPendingDeletes finishes the buffer delete of every promoted batch whose
// pending-delete manifest is still present (a flush crashed between the batch
// rename and the delete). DeleteByTaskIDs is idempotent -- rows already gone
// delete zero -- so re-running after a crash mid-recovery is safe. Runs under
// the flush lock.
func (f *Flusher) recoverPendingDeletes(ctx context.Context) error {
	pattern := filepath.Join(f.cfg.ExportDir, batchPrefix+"*", manifestName)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("scan pending manifests: %w", err)
	}
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			return fmt.Errorf("read pending manifest %s: %w", m, err)
		}
		var taskIDs []string
		if err := json.Unmarshal(b, &taskIDs); err != nil {
			return fmt.Errorf("parse pending manifest %s: %w", m, err)
		}
		var deleted int64
		if len(taskIDs) > 0 {
			if deleted, err = loader.DeleteByTaskIDs(ctx, f.db, taskIDs); err != nil {
				return fmt.Errorf("recover pending delete %s: %w", m, err)
			}
		}
		if err := os.Remove(m); err != nil {
			return fmt.Errorf("retire recovered manifest %s: %w", m, err)
		}
		if err := syncPath(filepath.Dir(m)); err != nil {
			return fmt.Errorf("sync %s after recovery: %w", filepath.Dir(m), err)
		}
		f.logf("[flush] recovered %s: completed pending delete of %d row(s) (batch was already exported)",
			filepath.Base(filepath.Dir(m)), deleted)
	}
	return nil
}

// exportedTaskIDs pulls the task_id (SELECT column 0, a TEXT NOT NULL key) from
// each exported row. pgx may scan TEXT as string or []byte, so both are handled.
func exportedTaskIDs(rows [][]any) ([]string, error) {
	ids := make([]string, 0, len(rows))
	for i, r := range rows {
		if len(r) == 0 {
			return nil, fmt.Errorf("exported row %d has no columns", i)
		}
		switch v := r[0].(type) {
		case string:
			if v == "" {
				return nil, fmt.Errorf("exported row %d has empty task_id", i)
			}
			ids = append(ids, v)
		case []byte:
			if len(v) == 0 {
				return nil, fmt.Errorf("exported row %d has empty task_id", i)
			}
			ids = append(ids, string(v))
		default:
			return nil, fmt.Errorf("exported row %d task_id has unexpected type %T", i, r[0])
		}
	}
	return ids, nil
}

// syncDir fsyncs every regular file in dir and then the directory entry itself,
// forcing the batch's bytes and its directory listing to the export volume before the
// buffer rows are deleted.
func syncDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		if err := syncPath(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return syncPath(dir)
}

// syncPath fsyncs a single path (file or directory). fsync on a directory fd
// persists its entries (needed after writes and after the rename).
func syncPath(p string) error {
	fh, err := os.Open(p)
	if err != nil {
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	return fh.Close()
}
