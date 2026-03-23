package db

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEnvDefaults(t *testing.T) {
	// Unset all POSTGRES_* env vars to verify fallback defaults.
	// t.Setenv registers cleanup to restore the original value on test end.
	envKeys := []string{"POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"}
	for _, key := range envKeys {
		// Save original via t.Setenv (registers restore), then unset.
		t.Setenv(key, "")
		os.Unsetenv(key)
	}

	cfg := ParseEnv()

	assert.Equal(t, "localhost", cfg.Host, "default host")
	assert.Equal(t, "5432", cfg.Port, "default port")
	assert.Equal(t, "distillation", cfg.DBName, "default dbname")
	assert.Equal(t, "distillation", cfg.User, "default user")
	assert.Equal(t, "", cfg.Password, "default password (empty)")
}

func TestParseEnvCustom(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "db.example.com")
	t.Setenv("POSTGRES_PORT", "15432")
	t.Setenv("POSTGRES_DB", "mydb")
	t.Setenv("POSTGRES_USER", "admin")
	t.Setenv("POSTGRES_PASSWORD", "s3cret")

	cfg := ParseEnv()

	assert.Equal(t, "db.example.com", cfg.Host)
	assert.Equal(t, "15432", cfg.Port)
	assert.Equal(t, "mydb", cfg.DBName)
	assert.Equal(t, "admin", cfg.User)
	assert.Equal(t, "s3cret", cfg.Password)
}

func TestConnConfigDSN(t *testing.T) {
	cfg := ConnConfig{
		Host:     "myhost",
		Port:     "5433",
		DBName:   "testdb",
		User:     "testuser",
		Password: "testpass",
	}

	dsn := cfg.DSN()

	assert.Contains(t, dsn, "host=myhost")
	assert.Contains(t, dsn, "port=5433")
	assert.Contains(t, dsn, "dbname=testdb")
	assert.Contains(t, dsn, "user=testuser")
	assert.Contains(t, dsn, "password=testpass")
}

func TestConnConfigDSN_EmptyPassword(t *testing.T) {
	cfg := ConnConfig{
		Host:     "localhost",
		Port:     "5432",
		DBName:   "distillation",
		User:     "distillation",
		Password: "",
	}

	dsn := cfg.DSN()

	assert.Contains(t, dsn, "host=localhost")
	assert.Contains(t, dsn, "password=''")
}

func TestConnConfigDSN_SpecialCharsInPassword(t *testing.T) {
	cfg := ConnConfig{
		Host:     "localhost",
		Port:     "5432",
		DBName:   "distillation",
		User:     "distillation",
		Password: "my pass'word\\end",
	}

	dsn := cfg.DSN()

	// password with spaces/quotes/backslashes must be single-quoted and escaped
	assert.Contains(t, dsn, "password='my pass\\'word\\\\end'")
}

func TestEscapeDSNValue_NoEscapeNeeded(t *testing.T) {
	assert.Equal(t, "simple", escapeDSNValue("simple"))
	assert.Equal(t, "s3cret123", escapeDSNValue("s3cret123"))
}

func TestEscapeDSNValue_Empty(t *testing.T) {
	assert.Equal(t, "''", escapeDSNValue(""))
}

func TestEscapeDSNValue_WithSpaces(t *testing.T) {
	assert.Equal(t, "'my password'", escapeDSNValue("my password"))
}

func TestEscapeDSNValue_WithQuotes(t *testing.T) {
	assert.Equal(t, "'it\\'s'", escapeDSNValue("it's"))
}

func TestEscapeDSNValue_WithBackslash(t *testing.T) {
	assert.Equal(t, "'a\\\\b'", escapeDSNValue("a\\b"))
}

// TestQuerierInterface verifies that *pgx.Conn satisfies the Querier interface
// at compile time. This is a compile-time check, not a runtime test.
func TestQuerierInterface(t *testing.T) {
	// Compile-time interface satisfaction check.
	// If *pgx.Conn does not implement Querier, this will not compile.
	var _ Querier = (*pgx.Conn)(nil)
}

// TestConnectRequiresContext verifies that Connect accepts context and returns
// the expected types. We don't test actual DB connectivity here (no DB in CI),
// just that the function signature is correct and returns an error when
// connecting to a non-existent host.
func TestConnectReturnsErrorForBadHost(t *testing.T) {
	t.Setenv("POSTGRES_HOST", "nonexistent-host-that-does-not-resolve.invalid")
	t.Setenv("POSTGRES_PORT", "5432")

	ctx := context.Background()
	conn, err := Connect(ctx)

	require.Error(t, err, "Connect should fail for non-existent host")
	assert.Nil(t, conn)
}
