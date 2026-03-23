// Package dbtest provides shared mock implementations of db.Querier
// for use in tests across internal packages.
package dbtest

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/guny524/distillation/internal/db"
)

// MockRow implements pgx.Row for QueryRow results.
type MockRow struct {
	ScanFn func(dest ...any) error
}

func (r *MockRow) Scan(dest ...any) error {
	return r.ScanFn(dest...)
}

// MockRows implements pgx.Rows for Query results.
// ScanFn allows each package to define its own scan behavior
// (e.g., typed scan for coverage/loader, *any scan for exporter).
type MockRows struct {
	Data    [][]any
	Cursor  int
	IsClosed bool
	Columns []string
	ScanFn  func(row []any, dest ...any) error // custom scan logic per package
}

// NewMockRows creates a MockRows with typed scan (coverage/loader pattern).
// Supports *string, *int, *int64, *bool destinations.
func NewMockRows(columns []string, data [][]any) *MockRows {
	return &MockRows{
		Data:    data,
		Cursor:  -1,
		Columns: columns,
		ScanFn:  TypedScan,
	}
}

// NewMockRowsAny creates a MockRows with *any scan (exporter pattern).
// Destinations must be *any (pointer to interface{}).
func NewMockRowsAny(data [][]any) *MockRows {
	return &MockRows{
		Data:   data,
		Cursor: -1,
		ScanFn: AnyScan,
	}
}

func (r *MockRows) Next() bool {
	if r.IsClosed {
		return false
	}
	r.Cursor++
	return r.Cursor < len(r.Data)
}

func (r *MockRows) Scan(dest ...any) error {
	if r.Cursor < 0 || r.Cursor >= len(r.Data) {
		return fmt.Errorf("scan: no current row")
	}
	row := r.Data[r.Cursor]
	if len(dest) != len(row) {
		return fmt.Errorf("scan: expected %d dest, got %d", len(row), len(dest))
	}
	return r.ScanFn(row, dest...)
}

func (r *MockRows) Close()                                        { r.IsClosed = true }
func (r *MockRows) Err() error                                    { return nil }
func (r *MockRows) CommandTag() pgconn.CommandTag                 { return pgconn.NewCommandTag("SELECT 0") }
func (r *MockRows) FieldDescriptions() []pgconn.FieldDescription  { return nil }
func (r *MockRows) RawValues() [][]byte                           { return nil }
func (r *MockRows) Values() ([]any, error)                        { return nil, nil }
func (r *MockRows) Conn() *pgx.Conn                               { return nil }

// TypedScan scans row values into typed destinations (*string, *int, *int64, *bool).
// Used by coverage and loader tests.
func TypedScan(row []any, dest ...any) error {
	for i, val := range row {
		switch d := dest[i].(type) {
		case *string:
			*d = val.(string)
		case *int:
			*d = val.(int)
		case *int64:
			*d = val.(int64)
		case *bool:
			*d = val.(bool)
		default:
			return fmt.Errorf("scan: unsupported dest type %T at index %d", dest[i], i)
		}
	}
	return nil
}

// AnyScan scans row values into *any destinations.
// Used by exporter tests (FetchAllRows pattern: ptrs[i] = &vals[i]).
func AnyScan(row []any, dest ...any) error {
	for i, val := range row {
		d, ok := dest[i].(*any)
		if !ok {
			return fmt.Errorf("scan: expected *any dest at %d, got %T", i, dest[i])
		}
		*d = val
	}
	return nil
}

// MockQuerier implements db.Querier for testing.
// Set QueryRowFn, QueryFn, ExecFn to control behavior per test.
type MockQuerier struct {
	QueryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	QueryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	ExecFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (m *MockQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if m.QueryRowFn != nil {
		return m.QueryRowFn(ctx, sql, args...)
	}
	return &MockRow{ScanFn: func(dest ...any) error { return fmt.Errorf("not implemented") }}
}

func (m *MockQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if m.QueryFn != nil {
		return m.QueryFn(ctx, sql, args...)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *MockQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if m.ExecFn != nil {
		return m.ExecFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag(""), fmt.Errorf("not implemented")
}

// MockRowsWithErr wraps a MockRows but overrides Err() to return a specific error.
// Used to test rows.Err() error paths after successful iteration.
type MockRowsWithErr struct {
	Inner  *MockRows
	ErrVal error
}

func (r *MockRowsWithErr) Next() bool                                       { return r.Inner.Next() }
func (r *MockRowsWithErr) Scan(dest ...any) error                           { return r.Inner.Scan(dest...) }
func (r *MockRowsWithErr) Close()                                           { r.Inner.Close() }
func (r *MockRowsWithErr) Err() error                                       { return r.ErrVal }
func (r *MockRowsWithErr) CommandTag() pgconn.CommandTag                    { return r.Inner.CommandTag() }
func (r *MockRowsWithErr) FieldDescriptions() []pgconn.FieldDescription     { return r.Inner.FieldDescriptions() }
func (r *MockRowsWithErr) RawValues() [][]byte                              { return r.Inner.RawValues() }
func (r *MockRowsWithErr) Values() ([]any, error)                           { return r.Inner.Values() }
func (r *MockRowsWithErr) Conn() *pgx.Conn                                  { return r.Inner.Conn() }

// Compile-time check: MockQuerier satisfies db.Querier.
var _ db.Querier = (*MockQuerier)(nil)
