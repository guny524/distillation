package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/flush"
	"github.com/guny524/distillation/internal/ingest"
	"github.com/guny524/distillation/internal/ingest/common"
	"github.com/guny524/distillation/internal/loader"
	"github.com/guny524/distillation/internal/pacing"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/runner"
	"github.com/guny524/distillation/internal/teacher"
)

// ingestLimitPerSource bounds how many artifacts each enabled source
// contributes per run. A run only consumes up to max_items_per_run of them, so
// a small bounded batch is enough; keeping it small avoids fetching a large
// dump slice on every cron tick.
const ingestLimitPerSource = 32

// runCommand is the legacy monolithic generation cycle. DEPRECATED (todos sec
// 2-5): it is superseded by the DB-queue stage workers (ingest/comprehend/
// question/presolve/answer/verify/flush), which decompose this single cycle into
// resumable, independently-operable stages. It is NO LONGER registered in the CLI
// (main.go) so it can never run by accident; the function is retained only because
// internal/runner (shared with worker.go via LoadConfig/Config/DefaultConfigPath)
// has not been split out yet. FOLLOW-UP: delete this file together with
// internal/runner's monolithic Run once runner.Config/LoadConfig are relocated.
func runCommand() *cli.Command {
	return &cli.Command{
		Name: "run",
		Usage: "DEPRECATED (use the stage workers): run one monolithic pacing-controlled " +
			"generation cycle (quota check -> k decision -> teacher calls -> schema validation -> DB insert)",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Value: runner.DefaultConfigPath,
				Usage: "path to the run config YAML (roles, pacing, prompt paths)",
			},
		},
		Action: func(c *cli.Context) error {
			ctx := c.Context

			cfg, err := runner.LoadConfig(c.String("config"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "[run] config load failed: %v\n", err)
				return err
			}

			// Single connection: advisory locks are session-level, so pacing
			// state, coverage queries, and inserts must share it.
			conn, err := db.Connect(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[run] DB connection failed: %v\n", err)
				return err
			}
			defer conn.Close(ctx)

			// Idempotent DDL: pacing state tables + distillation_pairs.
			if err := pacing.Migrate(ctx, conn); err != nil {
				fmt.Fprintf(os.Stderr, "[run] pacing migration failed: %v\n", err)
				return err
			}
			if err := loader.CreateTable(ctx, conn); err != nil {
				fmt.Fprintf(os.Stderr, "[run] create table failed: %v\n", err)
				return err
			}

			client, err := teacher.NewClient(cfg.Teacher)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[run] teacher client init failed: %v\n", err)
				return err
			}

			r, err := runner.New(cfg, pacing.NewPGStore(conn), conn, client)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[run] runner init failed: %v\n", err)
				return err
			}
			// In artifact mode, fetch a bounded batch from the enabled ingest
			// sources and feed it to the runner. All sources default to
			// enabled: false, so this is a clean no-op until the user opts a
			// source in (and provides its dump/token); a single source's fetch
			// failure is logged but does not abort the run.
			if cfg.Pipeline.Mode == runner.ModeArtifact {
				ingestCfg, err := ingest.LoadConfig(c.String("config"))
				if err != nil {
					fmt.Fprintf(os.Stderr, "[run] ingest config load failed: %v\n", err)
					return err
				}
				reg := ingest.NewRegistry(common.DefaultHTTPClient())
				arts, err := reg.FetchAll(ingestCfg, ingestLimitPerSource)
				if err != nil {
					// Partial results are still usable; log and proceed.
					fmt.Fprintf(os.Stderr, "[run] ingest partial failure: %v\n", err)
				}
				if len(arts) > 0 {
					r.Artifacts = pipeline.NewSliceSource(arts)
				}
			}

			runErr := r.Run(ctx)

			// DB-buffer flush cycle: once distillation_pairs reaches the
			// threshold, merge-export the buffer to a compressed Parquet batch
			// on the NAS and clear the flushed rows. This runs after the
			// generation lock is released; flush serializes itself with its own
			// NAS lock. A flush failure is logged but does not fail the run —
			// the generated rows are already durably committed in the DB buffer.
			if flushed, n, ferr := flush.New(cfg.Flush, conn).MaybeFlush(ctx); ferr != nil {
				fmt.Fprintf(os.Stderr, "[run] flush failed: %v\n", ferr)
			} else if flushed {
				fmt.Fprintf(os.Stderr, "[run] flushed %d row(s) to NAS\n", n)
			}
			return runErr
		},
	}
}
