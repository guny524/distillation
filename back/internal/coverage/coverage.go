// Package coverage queries the distillation_pairs table for 4-axis distribution
// and assembles the coverage summary embedded into teacher prompts (and dumped
// to JSON by `distill coverage`) to identify under-represented areas.
package coverage

import (
	"context"
	"fmt"
	"time"

	"github.com/guny524/distillation/internal/db"
)

// tableName is the PostgreSQL table queried for coverage data.
const tableName = "distillation_pairs"

// allowedColumns is a whitelist of column names permitted in dynamic SQL queries.
// Prevents SQL injection in QueryAxisCounts and QueryCrossCounts.
var allowedColumns = map[string]bool{
	"domain": true, "difficulty": true, "task_shape": true,
}

func validateColumn(col string) error {
	if !allowedColumns[col] {
		return fmt.Errorf("invalid column name: %q (allowed: domain, difficulty, task_shape)", col)
	}
	return nil
}

// DefaultOutput is the default output path for the standalone
// `distill coverage` subcommand (manual inspection). The run pipeline builds
// coverage in-process and does not write this file.
const DefaultOutput = "/workspace/output/coverage.json"

// Coverage holds the 4-axis distribution plus the M3 lane/source axes and
// metadata. The M3 axes count only artifact-based records (rows with a
// source_lane / artifact_refs); legacy taxonomy rows are excluded from them.
type Coverage struct {
	TotalCount        int            `json:"total_count"`
	Domain            map[string]int `json:"domain"`
	Difficulty        map[string]int `json:"difficulty"`
	TaskShape         map[string]int `json:"task_shape"`
	Capability        map[string]int `json:"capability"`
	DomainXDifficulty map[string]int `json:"domain_x_difficulty"`
	SourceLane        map[string]int `json:"source_lane"`
	ArtifactSource    map[string]int `json:"artifact_source"`
	LaneXSource       map[string]int `json:"lane_x_source"`
	GeneratedAt       string         `json:"generated_at"`
}

// TableExists checks whether the distillation_pairs table exists in the database.
// Returns false on first deployment when the table has not been created yet.
func TableExists(ctx context.Context, q db.Querier) (bool, error) {
	var exists bool
	err := q.QueryRow(ctx,
		"SELECT EXISTS ("+
			"  SELECT 1 FROM information_schema.tables"+
			"  WHERE table_name = $1"+
			")",
		tableName,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("table_exists: %w", err)
	}
	return exists, nil
}

// QueryTotal returns the total row count from distillation_pairs.
func QueryTotal(ctx context.Context, q db.Querier) (int, error) {
	var count int
	err := q.QueryRow(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query_total: %w", err)
	}
	return count, nil
}

// QueryAxisCounts counts rows grouped by a single text column (domain, difficulty, task_shape).
func QueryAxisCounts(ctx context.Context, q db.Querier, column string) (map[string]int, error) {
	if err := validateColumn(column); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx,
		fmt.Sprintf("SELECT %s, COUNT(*) FROM %s GROUP BY %s", column, tableName, column),
	)
	if err != nil {
		return nil, fmt.Errorf("query_axis_counts(%s): %w", column, err)
	}
	defer rows.Close()

	result, scanErr := scanKeyCount(rows)
	if scanErr != nil {
		return nil, fmt.Errorf("query_axis_counts(%s) scan: %w", column, scanErr)
	}
	return result, nil
}

// QueryCapabilityCounts counts by unnested capability_tags (PostgreSQL array).
// The SQL uses unnest() to flatten the array before grouping.
func QueryCapabilityCounts(ctx context.Context, q db.Querier) (map[string]int, error) {
	rows, err := q.Query(ctx,
		fmt.Sprintf(
			"SELECT tag, COUNT(*) FROM %s, unnest(capability_tags) AS tag GROUP BY tag",
			tableName,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query_capability_counts: %w", err)
	}
	defer rows.Close()

	result, scanErr := scanKeyCount(rows)
	if scanErr != nil {
		return nil, fmt.Errorf("query_capability_counts scan: %w", scanErr)
	}
	return result, nil
}

// scanKeyCount iterates rows with (string, int) columns and collects them into a map.
// Used by QueryAxisCounts and QueryCapabilityCounts which share the same scan pattern.
func scanKeyCount(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) (map[string]int, error) {
	result := make(map[string]int)
	for rows.Next() {
		var key string
		var count int
		if err := rows.Scan(&key, &count); err != nil {
			return nil, err
		}
		result[key] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// QuerySourceLaneCounts counts artifact-based rows grouped by source_lane.
// Legacy rows (NULL source_lane) are excluded so the lane axis reflects only
// M3 records.
func QuerySourceLaneCounts(ctx context.Context, q db.Querier) (map[string]int, error) {
	rows, err := q.Query(ctx,
		fmt.Sprintf(
			"SELECT source_lane, COUNT(*) FROM %s WHERE source_lane IS NOT NULL GROUP BY source_lane",
			tableName,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query_source_lane_counts: %w", err)
	}
	defer rows.Close()

	result, scanErr := scanKeyCount(rows)
	if scanErr != nil {
		return nil, fmt.Errorf("query_source_lane_counts scan: %w", scanErr)
	}
	return result, nil
}

// QueryArtifactSourceCounts counts artifact_refs occurrences grouped by
// source_type. Like QueryCapabilityCounts it unnests the array, so a record
// with refs to two sources contributes to both buckets.
func QueryArtifactSourceCounts(ctx context.Context, q db.Querier) (map[string]int, error) {
	rows, err := q.Query(ctx,
		fmt.Sprintf(
			"SELECT elem->>'source_type' AS source_type, COUNT(*) "+
				"FROM %s, jsonb_array_elements(artifact_refs) AS elem "+
				"WHERE artifact_refs IS NOT NULL GROUP BY source_type",
			tableName,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query_artifact_source_counts: %w", err)
	}
	defer rows.Close()

	result, scanErr := scanKeyCount(rows)
	if scanErr != nil {
		return nil, fmt.Errorf("query_artifact_source_counts scan: %w", scanErr)
	}
	return result, nil
}

// QueryLaneSourceCross counts artifact_refs occurrences grouped by the
// (source_lane, artifact source_type) pair, keyed "lane:source_type".
func QueryLaneSourceCross(ctx context.Context, q db.Querier) (map[string]int, error) {
	rows, err := q.Query(ctx,
		fmt.Sprintf(
			"SELECT source_lane, elem->>'source_type' AS source_type, COUNT(*) "+
				"FROM %s, jsonb_array_elements(artifact_refs) AS elem "+
				"WHERE source_lane IS NOT NULL AND artifact_refs IS NOT NULL "+
				"GROUP BY source_lane, source_type",
			tableName,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query_lane_source_cross: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var lane, source string
		var count int
		if err := rows.Scan(&lane, &source, &count); err != nil {
			return nil, fmt.Errorf("query_lane_source_cross scan: %w", err)
		}
		result[fmt.Sprintf("%s:%s", lane, source)] = count
	}
	return result, rows.Err()
}

// QueryCrossCounts counts rows grouped by two columns combined as "a:b" key.
func QueryCrossCounts(ctx context.Context, q db.Querier, colA, colB string) (map[string]int, error) {
	if err := validateColumn(colA); err != nil {
		return nil, err
	}
	if err := validateColumn(colB); err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx,
		fmt.Sprintf(
			"SELECT %s, %s, COUNT(*) FROM %s GROUP BY %s, %s",
			colA, colB, tableName, colA, colB,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("query_cross_counts(%s, %s): %w", colA, colB, err)
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var a, b string
		var count int
		if err := rows.Scan(&a, &b, &count); err != nil {
			return nil, fmt.Errorf("query_cross_counts(%s, %s) scan: %w", colA, colB, err)
		}
		result[fmt.Sprintf("%s:%s", a, b)] = count
	}
	return result, rows.Err()
}

// BuildEmptyCoverage returns an empty coverage structure for initial state
// (table does not exist yet). All axis maps are empty, total_count is 0.
func BuildEmptyCoverage() *Coverage {
	return &Coverage{
		TotalCount:        0,
		Domain:            map[string]int{},
		Difficulty:        map[string]int{},
		TaskShape:         map[string]int{},
		Capability:        map[string]int{},
		DomainXDifficulty: map[string]int{},
		SourceLane:        map[string]int{},
		ArtifactSource:    map[string]int{},
		LaneXSource:       map[string]int{},
		GeneratedAt:       time.Now().UTC().Format(time.RFC3339),
	}
}

// BuildCoverage queries all axes and assembles the full coverage structure.
func BuildCoverage(ctx context.Context, q db.Querier) (*Coverage, error) {
	total, err := QueryTotal(ctx, q)
	if err != nil {
		return nil, err
	}

	domain, err := QueryAxisCounts(ctx, q, "domain")
	if err != nil {
		return nil, err
	}

	difficulty, err := QueryAxisCounts(ctx, q, "difficulty")
	if err != nil {
		return nil, err
	}

	taskShape, err := QueryAxisCounts(ctx, q, "task_shape")
	if err != nil {
		return nil, err
	}

	capability, err := QueryCapabilityCounts(ctx, q)
	if err != nil {
		return nil, err
	}

	crossCounts, err := QueryCrossCounts(ctx, q, "domain", "difficulty")
	if err != nil {
		return nil, err
	}

	sourceLane, err := QuerySourceLaneCounts(ctx, q)
	if err != nil {
		return nil, err
	}

	artifactSource, err := QueryArtifactSourceCounts(ctx, q)
	if err != nil {
		return nil, err
	}

	laneXSource, err := QueryLaneSourceCross(ctx, q)
	if err != nil {
		return nil, err
	}

	return &Coverage{
		TotalCount:        total,
		Domain:            domain,
		Difficulty:        difficulty,
		TaskShape:         taskShape,
		Capability:        capability,
		DomainXDifficulty: crossCounts,
		SourceLane:        sourceLane,
		ArtifactSource:    artifactSource,
		LaneXSource:       laneXSource,
		GeneratedAt:       time.Now().UTC().Format(time.RFC3339),
	}, nil
}
