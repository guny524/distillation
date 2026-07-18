package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/coverage"
	"github.com/guny524/distillation/internal/exporter"
	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/runner"
)

// TestApp_CommandsRegistered verifies that the CLI app exposes the data tools and
// every DB-queue stage worker subcommand (todos sec 2-5-3), and that the DEPRECATED
// monolithic `run` cycle is NOT registered (it must never run by accident; the stage
// workers supersede it).
func TestApp_CommandsRegistered(t *testing.T) {
	names := make([]string, 0)
	for _, cmd := range commands() {
		names = append(names, cmd.Name)
	}

	// Data tools.
	for _, n := range []string{"coverage", "load", "export"} {
		assert.Contains(t, names, n, "%s subcommand must be registered", n)
	}
	// Queue stage workers.
	for _, n := range []string{"ingest", "comprehend", "question", "presolve", "answer", "verify", "flush"} {
		assert.Contains(t, names, n, "%s worker subcommand must be registered", n)
	}
	// The DEPRECATED monolithic run cycle is intentionally unregistered so a bare
	// `distill` / `docker run` can never trigger it (Dockerfile has no default CMD).
	assert.NotContains(t, names, "run", "the DEPRECATED monolithic run cycle must NOT be registered")
}

// TestWorkerCommands_LoopFlags verifies the always-on workers carry --loop and
// --poll-interval, and the periodic workers (ingest/flush) do not.
func TestWorkerCommands_LoopFlags(t *testing.T) {
	hasFlag := func(cmd *cli.Command, name string) bool {
		for _, f := range cmd.Flags {
			for _, n := range f.Names() {
				if n == name {
					return true
				}
			}
		}
		return false
	}

	for _, cmd := range []*cli.Command{comprehendCommand(), questionCommand(), presolveCommand(), answerCommand(), verifyCommand()} {
		assert.True(t, hasFlag(cmd, "loop"), "%s (always-on) must have --loop", cmd.Name)
		assert.True(t, hasFlag(cmd, "poll-interval"), "%s (always-on) must have --poll-interval", cmd.Name)
	}
	for _, cmd := range []*cli.Command{ingestCommand(), flushCommand()} {
		assert.False(t, hasFlag(cmd, "loop"), "%s (periodic) must not have --loop", cmd.Name)
	}
}

// TestRunCommand_DefaultConfigFlag verifies that --config defaults to
// runner.DefaultConfigPath (relative to /workspace in the container).
func TestRunCommand_DefaultConfigFlag(t *testing.T) {
	cmd := runCommand()

	var configFlag *cli.StringFlag
	for _, f := range cmd.Flags {
		if sf, ok := f.(*cli.StringFlag); ok && sf.Name == "config" {
			configFlag = sf
			break
		}
	}

	require.NotNil(t, configFlag, "run command must have a --config flag")
	assert.Equal(t, runner.DefaultConfigPath, configFlag.Value)
}

// TestCoverageCommand_DefaultOutputFlag verifies that the --output flag
// defaults to coverage.DefaultOutput (manual-inspection dump path).
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

// TestStageQuotaChecks_WiresRolesPerStep verifies each always-on stage's gate reads
// EXACTLY the roles that step uses (todos sec 6-3-4 step->role table), loading the
// shipped settings.yaml: question->generator, presolve->judge, answer->
// teacher+translator, verify->verifier, comprehend->opencode provider. The student
// is LOCAL (no quota_url) and is gated by no stage.
func TestStageQuotaChecks_WiresRolesPerStep(t *testing.T) {
	cfg, err := runner.LoadConfig("../../config/settings.yaml")
	require.NoError(t, err)

	rolesOf := func(stage queue.Stage) []string {
		var names []string
		for _, c := range stageQuotaChecks(cfg, stage) {
			names = append(names, c.Role)
		}
		return names
	}

	assert.Equal(t, []string{"generator"}, rolesOf(queue.StageQuestion))
	assert.Equal(t, []string{"judge"}, rolesOf(queue.StagePresolve))
	assert.Equal(t, []string{"teacher", "translator"}, rolesOf(queue.StageAnswer),
		"answer needs BOTH teacher and translator, so it gates on both")
	assert.Equal(t, []string{"verifier"}, rolesOf(queue.StageVerify))
	assert.Equal(t, []string{"opencode"}, rolesOf(queue.StageComprehend))

	// The student is local (no quota_url), so no stage checks it.
	for _, stage := range []queue.Stage{
		queue.StageComprehend, queue.StageQuestion, queue.StagePresolve,
		queue.StageAnswer, queue.StageVerify,
	} {
		assert.NotContains(t, rolesOf(stage), "student", "the student is local; it is never gated")
	}

	// Every emitted check carries the subgate provider and a non-empty quota_url.
	for _, c := range stageQuotaChecks(cfg, queue.StageAnswer) {
		assert.Equal(t, "codex", c.Provider)
		assert.NotEmpty(t, c.URL)
	}
}
