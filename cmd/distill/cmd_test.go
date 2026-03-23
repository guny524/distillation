package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/coverage"
	"github.com/guny524/distillation/internal/exporter"
)

// TestApp_CommandsRegistered verifies that the CLI app exposes all three subcommands.
func TestApp_CommandsRegistered(t *testing.T) {
	app := &cli.App{
		Name: "distill",
		Commands: []*cli.Command{
			coverageCommand(),
			loadCommand(),
			exportCommand(),
		},
	}

	names := make([]string, len(app.Commands))
	for i, cmd := range app.Commands {
		names[i] = cmd.Name
	}

	assert.Contains(t, names, "coverage", "coverage subcommand must be registered")
	assert.Contains(t, names, "load", "load subcommand must be registered")
	assert.Contains(t, names, "export", "export subcommand must be registered")
}

// TestCoverageCommand_DefaultOutputFlag verifies that the --output flag defaults
// to coverage.DefaultOutput, which must point to the shared emptyDir volume.
func TestCoverageCommand_DefaultOutputFlag(t *testing.T) {
	cmd := coverageCommand()

	var outputFlag *cli.StringFlag
	for _, f := range cmd.Flags {
		if sf, ok := f.(*cli.StringFlag); ok && sf.Name == "output" {
			outputFlag = sf
			break
		}
	}

	require.NotNil(t, outputFlag, "coverage command must have an --output flag")
	assert.Equal(t, coverage.DefaultOutput, outputFlag.Value,
		"--output default must equal coverage.DefaultOutput")
	assert.True(t,
		strings.HasPrefix(outputFlag.Value, "/workspace/output/"),
		"--output default must be inside /workspace/output/, got: %s", outputFlag.Value)
}

// TestExportCommand_DefaultFlags verifies that --output-dir and --shard-size defaults
// match the constants in the exporter package.
func TestExportCommand_DefaultFlags(t *testing.T) {
	cmd := exportCommand()

	var outputDirFlag *cli.StringFlag
	var shardSizeFlag *cli.IntFlag

	for _, f := range cmd.Flags {
		switch ff := f.(type) {
		case *cli.StringFlag:
			if ff.Name == "output-dir" {
				outputDirFlag = ff
			}
		case *cli.IntFlag:
			if ff.Name == "shard-size" {
				shardSizeFlag = ff
			}
		}
	}

	require.NotNil(t, outputDirFlag, "export command must have an --output-dir flag")
	assert.Equal(t, exporter.DefaultOutputDir, outputDirFlag.Value)

	require.NotNil(t, shardSizeFlag, "export command must have a --shard-size flag")
	assert.Equal(t, exporter.DefaultShardSize, shardSizeFlag.Value)
}

// TestLoadCommand_NoArgs_ReturnsError verifies that load without file arguments
// returns an error (not panics), enforcing the "at least one file required" contract.
func TestLoadCommand_NoArgs_ReturnsError(t *testing.T) {
	app := &cli.App{
		Name: "distill",
		Commands: []*cli.Command{
			loadCommand(),
		},
		// Suppress default ExitErrHandler to avoid os.Exit in tests.
		ExitErrHandler: func(_ *cli.Context, _ error) {},
	}

	err := app.Run([]string{"distill", "load"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one JSONL file path is required")
}
