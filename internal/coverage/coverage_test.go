package coverage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/guny524/distillation/internal/db/dbtest"
)

// --- tests ---

func TestTableExists_True(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, args ...any) pgx.Row {
			assert.Contains(t, sql, "information_schema.tables")
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = true
				return nil
			}}
		},
	}

	exists, err := TableExists(context.Background(), q)

	require.NoError(t, err)
	assert.True(t, exists)
}

func TestTableExists_False(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*bool)) = false
				return nil
			}}
		},
	}

	exists, err := TableExists(context.Background(), q)

	require.NoError(t, err)
	assert.False(t, exists)
}

func TestQueryTotal(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			assert.Contains(t, sql, "COUNT(*)")
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 42
				return nil
			}}
		},
	}

	total, err := QueryTotal(context.Background(), q)

	require.NoError(t, err)
	assert.Equal(t, 42, total)
}

func TestQueryAxisCounts(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			assert.Contains(t, sql, "domain")
			assert.Contains(t, sql, "GROUP BY")
			rows := dbtest.NewMockRows([]string{"domain", "count"}, [][]any{
				{"math", 10},
				{"science", 5},
				{"coding", 20},
			})
			return rows, nil
		},
	}

	counts, err := QueryAxisCounts(context.Background(), q, "domain")

	require.NoError(t, err)
	assert.Equal(t, map[string]int{"math": 10, "science": 5, "coding": 20}, counts)
}

func TestQueryAxisCounts_Empty(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := dbtest.NewMockRows([]string{"domain", "count"}, [][]any{})
			return rows, nil
		},
	}

	counts, err := QueryAxisCounts(context.Background(), q, "domain")

	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestQueryCapabilityCounts(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			assert.Contains(t, sql, "unnest(capability_tags)")
			rows := dbtest.NewMockRows([]string{"tag", "count"}, [][]any{
				{"reasoning", 15},
				{"coding", 8},
			})
			return rows, nil
		},
	}

	counts, err := QueryCapabilityCounts(context.Background(), q)

	require.NoError(t, err)
	assert.Equal(t, map[string]int{"reasoning": 15, "coding": 8}, counts)
}

func TestQueryCrossCounts_KeyFormat(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			assert.Contains(t, sql, "domain")
			assert.Contains(t, sql, "difficulty")
			rows := dbtest.NewMockRows([]string{"domain", "difficulty", "count"}, [][]any{
				{"math", "hard", 3},
				{"science", "easy", 7},
			})
			return rows, nil
		},
	}

	counts, err := QueryCrossCounts(context.Background(), q, "domain", "difficulty")

	require.NoError(t, err)
	// Key format must be "a:b" (colon-separated)
	assert.Equal(t, map[string]int{"math:hard": 3, "science:easy": 7}, counts)
}

func TestQueryCrossCounts_Empty(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := dbtest.NewMockRows([]string{"a", "b", "count"}, [][]any{})
			return rows, nil
		},
	}

	counts, err := QueryCrossCounts(context.Background(), q, "domain", "difficulty")

	require.NoError(t, err)
	assert.Empty(t, counts)
}

func TestBuildEmptyCoverage_AllKeysPresent(t *testing.T) {
	cov := BuildEmptyCoverage()

	// All 7 keys must be present.
	assert.NotNil(t, cov)
	assert.IsType(t, map[string]int{}, cov.Domain)
	assert.IsType(t, map[string]int{}, cov.Difficulty)
	assert.IsType(t, map[string]int{}, cov.TaskShape)
	assert.IsType(t, map[string]int{}, cov.Capability)
	assert.IsType(t, map[string]int{}, cov.DomainXDifficulty)
	assert.NotEmpty(t, cov.GeneratedAt, "generated_at must not be empty")
}

func TestBuildEmptyCoverage_TotalCountZero(t *testing.T) {
	cov := BuildEmptyCoverage()

	assert.Equal(t, 0, cov.TotalCount)
	assert.Empty(t, cov.Domain)
	assert.Empty(t, cov.Difficulty)
	assert.Empty(t, cov.TaskShape)
	assert.Empty(t, cov.Capability)
	assert.Empty(t, cov.DomainXDifficulty)
}

func TestBuildCoverage_AssemblesAllAxes(t *testing.T) {
	callCount := 0
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 100 // total count
				return nil
			}}
		},
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			callCount++
			switch {
			case strings.Contains(sql, "unnest"):
				// capability query
				return dbtest.NewMockRows([]string{"tag", "count"}, [][]any{
					{"reasoning", 50},
				}), nil
			case strings.Contains(sql, "domain") && strings.Contains(sql, "difficulty") && strings.Count(sql, ",") >= 2:
				// cross counts: SELECT domain, difficulty, COUNT(*)
				return dbtest.NewMockRows([]string{"domain", "difficulty", "count"}, [][]any{
					{"math", "hard", 30},
				}), nil
			case strings.Contains(sql, "domain"):
				return dbtest.NewMockRows([]string{"domain", "count"}, [][]any{
					{"math", 60},
				}), nil
			case strings.Contains(sql, "difficulty"):
				return dbtest.NewMockRows([]string{"difficulty", "count"}, [][]any{
					{"hard", 40},
				}), nil
			case strings.Contains(sql, "task_shape"):
				return dbtest.NewMockRows([]string{"task_shape", "count"}, [][]any{
					{"open_ended", 70},
				}), nil
			default:
				return dbtest.NewMockRows([]string{}, [][]any{}), nil
			}
		},
	}

	cov, err := BuildCoverage(context.Background(), q)

	require.NoError(t, err)
	assert.Equal(t, 100, cov.TotalCount)
	assert.Equal(t, map[string]int{"math": 60}, cov.Domain)
	assert.Equal(t, map[string]int{"hard": 40}, cov.Difficulty)
	assert.Equal(t, map[string]int{"open_ended": 70}, cov.TaskShape)
	assert.Equal(t, map[string]int{"reasoning": 50}, cov.Capability)
	assert.Equal(t, map[string]int{"math:hard": 30}, cov.DomainXDifficulty)
	assert.NotEmpty(t, cov.GeneratedAt)
}

func TestQueryAxisCounts_InvalidColumn(t *testing.T) {
	// validateColumn must reject columns not in the allowlist (SQL injection guard).
	q := &dbtest.MockQuerier{}

	_, err := QueryAxisCounts(context.Background(), q, "'; DROP TABLE distillation_pairs; --")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name")
}

func TestQueryCrossCounts_InvalidColumnA(t *testing.T) {
	q := &dbtest.MockQuerier{}

	_, err := QueryCrossCounts(context.Background(), q, "malicious_col", "difficulty")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name")
}

func TestQueryCrossCounts_InvalidColumnB(t *testing.T) {
	q := &dbtest.MockQuerier{}

	_, err := QueryCrossCounts(context.Background(), q, "domain", "malicious_col")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid column name")
}

func TestBuildCoverage_DifficultyQueryError(t *testing.T) {
	// domain succeeds, difficulty fails.
	callCount := 0
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 10
				return nil
			}}
		},
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			callCount++
			switch callCount {
			case 1: // domain
				return dbtest.NewMockRows([]string{"domain", "count"}, [][]any{{"math", 5}}), nil
			default: // difficulty fails
				return nil, fmt.Errorf("difficulty query failed")
			}
		},
	}

	_, err := BuildCoverage(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_axis_counts(difficulty)")
}

func TestBuildCoverage_CapabilityQueryError(t *testing.T) {
	// domain, difficulty, task_shape succeed; capability fails.
	callCount := 0
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 10
				return nil
			}}
		},
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			callCount++
			switch callCount {
			case 1: // domain
				return dbtest.NewMockRows([]string{"domain", "count"}, [][]any{{"math", 5}}), nil
			case 2: // difficulty
				return dbtest.NewMockRows([]string{"difficulty", "count"}, [][]any{{"easy", 3}}), nil
			case 3: // task_shape
				return dbtest.NewMockRows([]string{"task_shape", "count"}, [][]any{{"qa", 2}}), nil
			default: // capability fails
				return nil, fmt.Errorf("capability query failed")
			}
		},
	}

	_, err := BuildCoverage(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_capability_counts")
}

func TestTableExists_ScanError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(_ ...any) error {
				return fmt.Errorf("scan failed: connection reset")
			}}
		},
	}

	_, err := TableExists(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "table_exists")
}

func TestQueryTotal_ScanError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(_ ...any) error {
				return fmt.Errorf("scan failed")
			}}
		},
	}

	_, err := QueryTotal(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_total")
}

func TestQueryAxisCounts_QueryError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return nil, fmt.Errorf("query failed: timeout")
		},
	}

	_, err := QueryAxisCounts(context.Background(), q, "domain")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_axis_counts(domain)")
}

func TestQueryCapabilityCounts_QueryError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return nil, fmt.Errorf("query failed: timeout")
		},
	}

	_, err := QueryCapabilityCounts(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_capability_counts")
}

func TestQueryCrossCounts_QueryError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			return nil, fmt.Errorf("query failed: timeout")
		},
	}

	_, err := QueryCrossCounts(context.Background(), q, "domain", "difficulty")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_cross_counts(domain, difficulty)")
}

func TestBuildCoverage_QueryTotalError(t *testing.T) {
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(_ ...any) error {
				return fmt.Errorf("total query failed")
			}}
		},
	}

	_, err := BuildCoverage(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_total")
}

func TestBuildCoverage_AxisQueryError(t *testing.T) {
	// QueryTotal succeeds but first QueryAxisCounts (domain) fails.
	callCount := 0
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 10
				return nil
			}}
		},
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			callCount++
			return nil, fmt.Errorf("axis query failed at call %d", callCount)
		},
	}

	_, err := BuildCoverage(context.Background(), q)

	require.Error(t, err)
	// The first axis query (domain) should fail.
	assert.Contains(t, err.Error(), "query_axis_counts(domain)")
}

// TestDefaultOutputIsInsideSharedVolume is a C2 regression guard.
// The default output path MUST be inside /workspace/output/ so that
// the init container writes coverage.json to the emptyDir shared volume.
func TestDefaultOutputIsInsideSharedVolume(t *testing.T) {
	assert.True(t,
		strings.HasPrefix(DefaultOutput, "/workspace/output/"),
		"DefaultOutput must be inside /workspace/output/, got: %s", DefaultOutput,
	)
}

func TestQueryAxisCounts_ScanError(t *testing.T) {
	// rows.Scan error during iteration must propagate with query_axis_counts prefix.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := &dbtest.MockRows{
				Data:   [][]any{{"math", 10}},
				Cursor: -1,
				ScanFn: func(_ []any, _ ...any) error {
					return fmt.Errorf("scan: column type mismatch")
				},
			}
			return rows, nil
		},
	}

	_, err := QueryAxisCounts(context.Background(), q, "domain")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_axis_counts(domain) scan")
}

func TestQueryCapabilityCounts_ScanError(t *testing.T) {
	// rows.Scan error during iteration must propagate with query_capability_counts prefix.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := &dbtest.MockRows{
				Data:   [][]any{{"reasoning", 5}},
				Cursor: -1,
				ScanFn: func(_ []any, _ ...any) error {
					return fmt.Errorf("scan: tag column error")
				},
			}
			return rows, nil
		},
	}

	_, err := QueryCapabilityCounts(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_capability_counts scan")
}

func TestQueryCrossCounts_ScanError(t *testing.T) {
	// rows.Scan error during iteration must propagate with query_cross_counts prefix.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			rows := &dbtest.MockRows{
				Data:   [][]any{{"math", "hard", 3}},
				Cursor: -1,
				ScanFn: func(_ []any, _ ...any) error {
					return fmt.Errorf("scan: cross column error")
				},
			}
			return rows, nil
		},
	}

	_, err := QueryCrossCounts(context.Background(), q, "domain", "difficulty")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_cross_counts(domain, difficulty) scan")
}

func TestBuildCoverage_TaskShapeQueryError(t *testing.T) {
	// domain and difficulty succeed; task_shape fails.
	callCount := 0
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 10
				return nil
			}}
		},
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			callCount++
			switch callCount {
			case 1: // domain
				return dbtest.NewMockRows([]string{"domain", "count"}, [][]any{{"math", 5}}), nil
			case 2: // difficulty
				return dbtest.NewMockRows([]string{"difficulty", "count"}, [][]any{{"easy", 3}}), nil
			default: // task_shape fails
				return nil, fmt.Errorf("task_shape query failed")
			}
		},
	}

	_, err := BuildCoverage(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_axis_counts(task_shape)")
}

func TestQueryAxisCounts_RowsErrError(t *testing.T) {
	// rows.Err() returning non-nil after successful iteration must be propagated.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			inner := dbtest.NewMockRows([]string{"domain", "count"}, [][]any{
				{"math", 10},
			})
			return &dbtest.MockRowsWithErr{Inner: inner, ErrVal: fmt.Errorf("network reset during iteration")}, nil
		},
	}

	_, err := QueryAxisCounts(context.Background(), q, "domain")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "network reset during iteration")
}

func TestQueryCapabilityCounts_RowsErrError(t *testing.T) {
	// rows.Err() returning non-nil after iteration in QueryCapabilityCounts.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			inner := dbtest.NewMockRows([]string{"tag", "count"}, [][]any{
				{"reasoning", 5},
			})
			return &dbtest.MockRowsWithErr{Inner: inner, ErrVal: fmt.Errorf("capability rows err")}, nil
		},
	}

	_, err := QueryCapabilityCounts(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "capability rows err")
}

func TestQueryCrossCounts_RowsErrError(t *testing.T) {
	// rows.Err() returning non-nil after iteration in QueryCrossCounts.
	q := &dbtest.MockQuerier{
		QueryFn: func(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
			inner := dbtest.NewMockRows([]string{"domain", "difficulty", "count"}, [][]any{
				{"math", "easy", 3},
			})
			return &dbtest.MockRowsWithErr{Inner: inner, ErrVal: fmt.Errorf("cross rows err")}, nil
		},
	}

	_, err := QueryCrossCounts(context.Background(), q, "domain", "difficulty")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cross rows err")
}

func TestBuildCoverage_CrossCountsQueryError(t *testing.T) {
	// domain, difficulty, task_shape, capability succeed; cross counts fails.
	callCount := 0
	q := &dbtest.MockQuerier{
		QueryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &dbtest.MockRow{ScanFn: func(dest ...any) error {
				*(dest[0].(*int)) = 10
				return nil
			}}
		},
		QueryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			callCount++
			switch callCount {
			case 1: // domain
				return dbtest.NewMockRows([]string{"domain", "count"}, [][]any{{"math", 5}}), nil
			case 2: // difficulty
				return dbtest.NewMockRows([]string{"difficulty", "count"}, [][]any{{"easy", 3}}), nil
			case 3: // task_shape
				return dbtest.NewMockRows([]string{"task_shape", "count"}, [][]any{{"qa", 2}}), nil
			case 4: // capability
				return dbtest.NewMockRows([]string{"tag", "count"}, [][]any{{"reasoning", 8}}), nil
			default: // cross counts fails
				return nil, fmt.Errorf("cross counts query failed")
			}
		},
	}

	_, err := BuildCoverage(context.Background(), q)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "query_cross_counts(domain, difficulty)")
}
