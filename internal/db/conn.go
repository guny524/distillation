// Package db provides PostgreSQL connection utilities for the distillation pipeline.
// All three subcommands (coverage, load, export) share the same DB connection pattern.
package db

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier abstracts the subset of pgx.Conn methods used by the pipeline.
// Test code injects a mock implementation of this interface.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ConnConfig holds PostgreSQL connection parameters parsed from environment variables.
type ConnConfig struct {
	Host     string
	Port     string
	DBName   string
	User     string
	Password string
}

// ParseEnv reads PostgreSQL connection parameters from environment variables.
// Defaults match the Python scripts: localhost / 5432 / distillation / distillation / "".
func ParseEnv() ConnConfig {
	return ConnConfig{
		Host:     envOrDefault("POSTGRES_HOST", "localhost"),
		Port:     envOrDefault("POSTGRES_PORT", "5432"),
		DBName:   envOrDefault("POSTGRES_DB", "distillation"),
		User:     envOrDefault("POSTGRES_USER", "distillation"),
		Password: envOrDefault("POSTGRES_PASSWORD", ""),
	}
}

// DSN returns a PostgreSQL connection string (key=value format).
// Values containing spaces, single quotes, or backslashes are escaped
// per libpq connection string rules.
func (c ConnConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		escapeDSNValue(c.Host), escapeDSNValue(c.Port),
		escapeDSNValue(c.DBName), escapeDSNValue(c.User),
		escapeDSNValue(c.Password),
	)
}

// escapeDSNValue escapes a value for libpq key=value connection strings.
// Empty values and values containing spaces, single quotes, or backslashes
// are wrapped in single quotes with internal quotes/backslashes escaped.
func escapeDSNValue(v string) string {
	if v == "" {
		return "''"
	}
	if !strings.ContainsAny(v, " '\\") {
		return v
	}
	var b strings.Builder
	b.WriteByte('\'')
	for _, c := range v {
		if c == '\'' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteRune(c)
	}
	b.WriteByte('\'')
	return b.String()
}

// Connect establishes a PostgreSQL connection using environment variables.
// CronJob workloads use a single connection (no pool needed).
func Connect(ctx context.Context) (*pgx.Conn, error) {
	cfg := ParseEnv()
	conn, err := pgx.Connect(ctx, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("db connect (%s:%s/%s): %w", cfg.Host, cfg.Port, cfg.DBName, err)
	}
	return conn, nil
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
