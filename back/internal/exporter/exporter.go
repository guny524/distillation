// Package exporter converts PostgreSQL distillation_pairs rows to sharded
// Parquet files in a format compatible with HuggingFace datasets.
//
// This is the Go equivalent of Python scripts/export_parquet.py.
// The 3-stage name mapping (DB "references_" -> Parquet "references") is
// handled by ArrowSchema and BuildTable.
package exporter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/jackc/pgx/v5"

	"github.com/guny524/distillation/internal/db"
)

// DefaultOutputDir is the default directory for Parquet shard files.
const DefaultOutputDir = "/mnt/distillation"

// DefaultShardSize is the maximum number of rows per Parquet shard file.
const DefaultShardSize = 50000

// columns lists the DB column names in SELECT order.
// "references_" is the DB column name; it maps to "references" in ArrowSchema.
// The three JSONB columns are exported as their JSON text via ::text (see
// selectExpr) so BuildTable can treat them as plain string fields.
var columns = []string{
	"task_id", "domain", "difficulty", "task_shape",
	"capability_tags", "user_request", "context",
	"success_criteria", "plan", "reasoning_summary",
	"final_answer", "self_check", "quality_notes",
	"references_", "artifacts",
	"source_lane", "pair_id", "artifact_refs", "student_filter_verdict",
	"verification", "difficulty_mutations", "teacher_model", "teacher_provider",
	"cot", "cot_raw", "has_raw_cot",
	"created_at",
}

// jsonbColumns are stored as JSONB in the DB and exported as JSON text.
var jsonbColumns = map[string]bool{
	"artifact_refs": true, "student_filter_verdict": true, "verification": true,
}

// selectExpr returns the SELECT expression for a DB column: JSONB columns are
// cast to text so pgx returns a deterministic string (or NULL); others select
// as-is.
func selectExpr(col string) string {
	if jsonbColumns[col] {
		return col + "::text"
	}
	return col
}

// ArrowSchema defines the Parquet output schema (27 fields).
//   - string fields: arrow.BinaryTypes.String
//   - list<string> fields: arrow.ListOf(arrow.BinaryTypes.String)
//   - JSONB fields (artifact_refs, student_filter_verdict, verification) are
//     exported as JSON text (String)
//   - boolean field: has_raw_cot
//   - timestamp field: arrow.Microsecond with UTC timezone
//   - "references_" DB column is mapped to "references" Parquet field name
//   - nullable=true for references, artifacts, all M3 fields, the reasoning
//     fields (cot, cot_raw, has_raw_cot), and created_at
var ArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "task_id", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "domain", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "difficulty", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "task_shape", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "capability_tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
	{Name: "user_request", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "context", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "success_criteria", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
	{Name: "plan", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
	{Name: "reasoning_summary", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "final_answer", Type: arrow.BinaryTypes.String, Nullable: false},
	{Name: "self_check", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
	{Name: "quality_notes", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: false},
	{Name: "references", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	{Name: "artifacts", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	{Name: "source_lane", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "pair_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "artifact_refs", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "student_filter_verdict", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "verification", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "difficulty_mutations", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	{Name: "teacher_model", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "teacher_provider", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "cot", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "cot_raw", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "has_raw_cot", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
}, nil)

// selectColumnsExpr joins every column's SELECT expression (JSONB columns cast
// to text via selectExpr) into the column list shared by all fetch queries.
func selectColumnsExpr() string {
	exprs := make([]string, len(columns))
	for i, c := range columns {
		exprs[i] = selectExpr(c)
	}
	return strings.Join(exprs, ", ")
}

// selectSQL fetches every row from distillation_pairs (manual full export).
var selectSQL = fmt.Sprintf("SELECT %s FROM distillation_pairs ORDER BY id", selectColumnsExpr())

// selectUpToSQL fetches rows with id <= $1 (the flush high-water mark), so a
// concurrent INSERT (higher id) is excluded from the batch being flushed.
var selectUpToSQL = fmt.Sprintf("SELECT %s FROM distillation_pairs WHERE id <= $1 ORDER BY id", selectColumnsExpr())

// FetchAllRows fetches all rows from distillation_pairs ordered by id.
// Each row is a []any slice matching the columns order.
func FetchAllRows(ctx context.Context, q db.Querier) ([][]any, error) {
	rows, err := q.Query(ctx, selectSQL)
	if err != nil {
		return nil, fmt.Errorf("fetch all rows: %w", err)
	}
	return scanRows(rows)
}

// FetchRowsUpTo fetches rows with id <= maxID ordered by id. The flush cycle
// snapshots maxID first (under the export-volume lock) and exports only up to it, leaving
// rows inserted afterward for the next flush.
func FetchRowsUpTo(ctx context.Context, q db.Querier, maxID int64) ([][]any, error) {
	rows, err := q.Query(ctx, selectUpToSQL, maxID)
	if err != nil {
		return nil, fmt.Errorf("fetch rows up to %d: %w", maxID, err)
	}
	return scanRows(rows)
}

// scanRows drains a pgx.Rows into row-major []any slices (one per column),
// closing the cursor. Shared by FetchAllRows and FetchRowsUpTo.
func scanRows(rows pgx.Rows) ([][]any, error) {
	defer rows.Close()

	var result [][]any
	for rows.Next() {
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		result = append(result, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}
	return result, nil
}

// BuildTable converts row-major data (from FetchAllRows or test fixtures) into
// an Arrow Record matching ArrowSchema. The "references_" DB column (index 13)
// is mapped to the "references" Parquet field.
//
// Each row must be a []any of length 16 with types:
//   - string for TEXT columns
//   - []string for TEXT[] columns (nil for nullable)
//   - *time.Time for TIMESTAMPTZ (nil for nullable)
func BuildTable(rows [][]any, mem memory.Allocator) (arrow.RecordBatch, error) {
	bldr := array.NewRecordBuilder(mem, ArrowSchema)
	defer bldr.Release()

	for _, row := range rows {
		if len(row) != len(columns) {
			return nil, fmt.Errorf("expected %d columns, got %d", len(columns), len(row))
		}
		if err := appendRow(bldr, row); err != nil {
			return nil, fmt.Errorf("append row: %w", err)
		}
	}

	rec := bldr.NewRecordBatch()
	return rec, nil
}

// appendRow appends one row of data to the RecordBuilder.
// Column order matches the columns slice and ArrowSchema field order.
func appendRow(bldr *array.RecordBuilder, row []any) error {
	for i, val := range row {
		fieldName := ArrowSchema.Field(i).Name
		fb := bldr.Field(i)

		switch {
		case isStringField(fieldName):
			sb := fb.(*array.StringBuilder)
			if val == nil {
				sb.AppendNull() // nullable M3 string columns (source_lane, pair_id, JSONB-as-text, teacher_*)
				continue
			}
			s, ok := val.(string)
			if !ok {
				return fmt.Errorf("field %q: expected string, got %T", fieldName, val)
			}
			sb.Append(s)

		case isListStringField(fieldName):
			lb := fb.(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.StringBuilder)
			if val == nil {
				lb.AppendNull()
			} else {
				strs, err := toStringSlice(val)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				lb.Append(true)
				for _, s := range strs {
					vb.Append(s)
				}
			}

		case isBoolField(fieldName):
			bb := fb.(*array.BooleanBuilder)
			if val == nil {
				bb.AppendNull() // nullable has_raw_cot
				continue
			}
			b, ok := val.(bool)
			if !ok {
				return fmt.Errorf("field %q: expected bool, got %T", fieldName, val)
			}
			bb.Append(b)

		case fieldName == "created_at":
			tb := fb.(*array.TimestampBuilder)
			if val == nil {
				tb.AppendNull()
			} else {
				t, err := toTime(val)
				if err != nil {
					return fmt.Errorf("field %q: %w", fieldName, err)
				}
				if t == nil {
					tb.AppendNull()
				} else {
					us := t.UnixMicro()
					tb.Append(arrow.Timestamp(us))
				}
			}

		default:
			return fmt.Errorf("unknown field %q at index %d", fieldName, i)
		}
	}
	return nil
}

// isStringField returns true if the field is a plain string (not list, not
// timestamp). Includes the M3 scalar fields and the JSONB-as-text fields.
func isStringField(name string) bool {
	switch name {
	case "task_id", "domain", "difficulty", "task_shape",
		"user_request", "context", "reasoning_summary", "final_answer",
		"source_lane", "pair_id", "artifact_refs", "student_filter_verdict",
		"verification", "teacher_model", "teacher_provider", "cot", "cot_raw":
		return true
	}
	return false
}

// isBoolField returns true if the field is a boolean (has_raw_cot).
func isBoolField(name string) bool {
	return name == "has_raw_cot"
}

// isListStringField returns true if the field is a list<string>.
func isListStringField(name string) bool {
	switch name {
	case "capability_tags", "success_criteria", "plan",
		"self_check", "quality_notes", "references", "artifacts",
		"difficulty_mutations":
		return true
	}
	return false
}

// toStringSlice converts a value to []string.
// pgx v5 decodes TEXT[] as []any when scanning into *any, so we handle both
// []string (test mocks) and []any (real pgx) patterns.
func toStringSlice(val any) ([]string, error) {
	switch v := val.(type) {
	case []string:
		return v, nil
	case []any:
		result := make([]string, len(v))
		for i, elem := range v {
			s, ok := elem.(string)
			if !ok {
				return nil, fmt.Errorf("expected string at index %d, got %T", i, elem)
			}
			result[i] = s
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected []string or []any, got %T", val)
	}
}

// toTime converts a value to *time.Time.
// pgx v5 decodes TIMESTAMPTZ as time.Time (not *time.Time) when scanning into *any.
func toTime(val any) (*time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		return &v, nil
	case *time.Time:
		return v, nil
	default:
		return nil, fmt.Errorf("expected time.Time or *time.Time, got %T", val)
	}
}

// DefaultCompression is the default Parquet compression codec.
// zstd: 30-39% smaller than snappy, similar read speed, industry trend (Apache Iceberg default).
const DefaultCompression = "zstd"

// ValidCompressions lists the allowed compression codec names for CLI validation.
var ValidCompressions = map[string]compress.Compression{
	"zstd":   compress.Codecs.Zstd,
	"snappy": compress.Codecs.Snappy,
	"gzip":   compress.Codecs.Gzip,
	"none":   compress.Codecs.Uncompressed,
}

// WriteShards writes an Arrow Record as sharded Parquet files.
// Returns the number of shards written (0 if the record has no rows).
// Creates outputDir if it does not exist.
// File pattern: train-00000-of-NNNNN.parquet (HuggingFace compatible).
func WriteShards(rec arrow.RecordBatch, outputDir string, shardSize int, compression string) (int, error) {
	if shardSize <= 0 {
		return 0, fmt.Errorf("shard size must be > 0, got %d", shardSize)
	}

	codec, ok := ValidCompressions[compression]
	if !ok {
		return 0, fmt.Errorf("unsupported compression %q (valid: zstd, snappy, gzip, none)", compression)
	}

	totalRows := int(rec.NumRows())

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return 0, fmt.Errorf("create output dir %s: %w", outputDir, err)
	}

	// Advisory file lock prevents concurrent WriteShards from interleaving
	// delete and write operations on the same outputDir.
	lock, err := AcquireLock(outputDir, ".write-shards.lock")
	if err != nil {
		return 0, err
	}
	defer ReleaseLock(lock)

	// Remove stale shard files from previous runs to prevent HuggingFace
	// datasets from loading outdated data (e.g., 5 shards -> 3 shards
	// would leave train-00003-of-00005.parquet and train-00004-of-00005.parquet).
	// Also cleans up when totalRows == 0 (all DB rows deleted).
	if err := removeStaleShards(outputDir); err != nil {
		return 0, fmt.Errorf("remove stale shards in %s: %w", outputDir, err)
	}

	if totalRows == 0 {
		return 0, nil
	}

	numShards := int(math.Ceil(float64(totalRows) / float64(shardSize)))
	if numShards < 1 {
		numShards = 1
	}

	for shardIdx := 0; shardIdx < numShards; shardIdx++ {
		start := int64(shardIdx * shardSize)
		length := int64(shardSize)
		if start+length > int64(totalRows) {
			length = int64(totalRows) - start
		}

		shard := rec.NewSlice(start, start+length)

		filename := fmt.Sprintf("train-%05d-of-%05d.parquet", shardIdx, numShards)
		fpath := filepath.Join(outputDir, filename)

		err := writeParquetFile(fpath, shard, codec)
		shard.Release()
		if err != nil {
			return 0, fmt.Errorf("write shard %s: %w", filename, err)
		}
	}

	return numShards, nil
}

// writeParquetFile writes a single Arrow Record to a Parquet file with the specified compression.
func writeParquetFile(path string, rec arrow.RecordBatch, codec compress.Compression) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file %s: %w", path, err)
	}
	defer f.Close()

	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(codec))

	fw, err := pqarrow.NewFileWriter(rec.Schema(), f, writerProps, arrProps)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}

	if err := fw.Write(rec); err != nil {
		fw.Close()
		return fmt.Errorf("write record: %w", err)
	}

	return fw.Close()
}

// AcquireLock takes an exclusive POSIX lock (fcntl F_SETLK) on lockName inside
// dir. Uses fcntl instead of flock for NFS cross-host locking compatibility
// (flock is local-only on NFS since Linux 2.6.12).
// LOCK_NB equivalent: F_SETLK returns EAGAIN immediately if the lock is held,
// so a second holder fails fast rather than blocking. Callers pair it with
// ReleaseLock (typically via defer). The flush cycle reuses it to serialize
// flushers across pods on the shared export volume.
func AcquireLock(dir, lockName string) (*os.File, error) {
	lockPath := filepath.Join(dir, lockName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", lockPath, err)
	}
	lk := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lk); err != nil {
		f.Close()
		return nil, fmt.Errorf("lock %s (another holder running?): %w", lockPath, err)
	}
	return f, nil
}

// ReleaseLock releases the POSIX lock and closes the file.
// The lock file is NOT deleted to prevent a TOCTOU race:
// if deleted, a new process could create a new inode and lock it
// while an old process still holds a lock on the deleted inode.
func ReleaseLock(f *os.File) {
	lk := syscall.Flock_t{
		Type:   syscall.F_UNLCK,
		Whence: 0,
		Start:  0,
		Len:    0,
	}
	syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lk)
	f.Close()
}

// removeStaleShards deletes all train-*.parquet files in dir.
// Uses os.ReadDir instead of filepath.Glob for NFS compatibility
// (filepath.Glob silently ignores I/O errors from readdir/stat).
// Best-effort: continues removing remaining files on individual errors.
func removeStaleShards(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dir, err)
	}
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() {
			continue
		}
		if strings.HasPrefix(name, "train-") && strings.HasSuffix(name, ".parquet") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to remove %d stale shard(s): %w", len(errs), errors.Join(errs...))
	}
	return nil
}

// VerifyParquetDir opens every train-*.parquet shard in dir, sums their row
// counts from the Parquet footers, and returns an error unless the total equals
// expected (and at least one shard exists). The flush cycle calls this after
// writing a batch and before deleting the source rows: a truncated or missing
// shard fails verification, so the DB rows are never deleted (no data loss).
func VerifyParquetDir(dir string, expected int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dir, err)
	}
	var total int64
	found := 0
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(name, "train-") || !strings.HasSuffix(name, ".parquet") {
			continue
		}
		n, err := parquetFileRows(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		total += n
		found++
	}
	if found == 0 {
		return fmt.Errorf("no parquet shards found in %s", dir)
	}
	if total != int64(expected) {
		return fmt.Errorf("row count mismatch in %s: parquet has %d, expected %d", dir, total, expected)
	}
	return nil
}

// parquetFileRows returns the number of rows recorded in a Parquet file footer.
func parquetFileRows(path string) (int64, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return 0, fmt.Errorf("open parquet %s: %w", path, err)
	}
	defer rdr.Close()
	return rdr.NumRows(), nil
}
