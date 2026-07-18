package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

// commands is the full subcommand set: the DB-queue stage workers (todos sec
// 2-5-3) plus the data tools. Periodic workers: ingest, flush. Always-on (--loop)
// workers: comprehend, question, presolve, answer, verify. The legacy monolithic
// `run` cycle is intentionally NOT registered -- it is superseded by the stage
// workers and must never run by accident (see cmd/distill/run.go; the function is
// retained only until internal/runner is split out).
func commands() []*cli.Command {
	return []*cli.Command{
		migrateCommand(),
		ingestCommand(),
		comprehendCommand(),
		questionCommand(),
		presolveCommand(),
		answerCommand(),
		verifyCommand(),
		flushCommand(),
		coverageCommand(),
		loadCommand(),
		exportCommand(),
	}
}

func main() {
	app := &cli.App{
		Name:     "distill",
		Usage:    "Distillation pipeline CLI tools",
		Commands: commands(),
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
