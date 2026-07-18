package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/flush"
	"github.com/guny524/distillation/internal/ingest"
	"github.com/guny524/distillation/internal/ingest/common"
	"github.com/guny524/distillation/internal/pipeline"
	"github.com/guny524/distillation/internal/queue"
	"github.com/guny524/distillation/internal/runner"
	"github.com/guny524/distillation/internal/teacher"
	"github.com/guny524/distillation/internal/worker"
)

// configFlag is the shared --config flag every worker subcommand accepts.
func configFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:  "config",
		Value: runner.DefaultConfigPath,
		Usage: "path to the run config YAML (roles, pacing, pipeline, flush)",
	}
}

// loopFlags are the always-on worker flags: --loop keeps polling the queue,
// --poll-interval is the idle sleep between drained-empty passes.
func loopFlags() []cli.Flag {
	return []cli.Flag{
		configFlag(),
		&cli.BoolFlag{Name: "loop", Usage: "keep polling the queue (always-on worker); omit for a single drain pass"},
		&cli.DurationFlag{Name: "poll-interval", Value: 5 * time.Second, Usage: "idle sleep between passes when --loop is set"},
	}
}

// openDB connects and applies every idempotent migration a worker may touch:
// pacing tables (answer gate), distillation_pairs (flush projection), the queue
// table + cursors (all stage workers), and the stage payload columns.
// openDB connects only; it does NOT create the schema. Schema is applied once by
// the dedicated `distill migrate` job before any worker starts (compose migrate
// service / k8s migrate Job), so workers never race to CREATE tables and never
// need to check whether the schema exists.
func openDB(ctx context.Context) (*pgx.Conn, error) {
	conn, err := db.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("DB connection failed: %w", err)
	}
	return conn, nil
}

// buildStore wraps a connection as the worker Store (queue state machine +
// payload columns), with the configured stale-lock reclaim threshold.
func buildStore(conn *pgx.Conn, stale time.Duration) *worker.PGStore {
	return worker.NewPGStore(queue.NewStore(conn, stale), conn)
}

// buildPipeline builds the shared pipeline from the run config, routed through
// the reasoning-aware teacher client (so answer captures cot/cot_raw).
func buildPipeline(cfg runner.Config, client *teacher.Client) (*pipeline.Pipeline, error) {
	return pipeline.New(cfg.ToPipelineConfig(), client)
}

// stageQuotaChecks maps an always-on stage to the quota checks its gate reads
// before spending that stage's subscription roles (todos sec 6-3-4 step->role
// table: comprehend->opencode provider, question->generator, presolve->judge,
// answer->teacher+translator, verify->verifier). A role without a quota_url yields
// no check (real API / local model, e.g. the student -- checked nowhere). The
// comprehend check comes from the opencode config (its provider is not a teacher
// role); it is empty when opencode is disabled, since the provenance fallback makes
// no network call and spends no quota.
func stageQuotaChecks(cfg runner.Config, stage queue.Stage) []worker.QuotaCheck {
	roles := cfg.Pipeline.Roles.WithDefaults()
	switch stage {
	case queue.StageComprehend:
		oc := cfg.Comprehend.Opencode
		if !oc.Enabled || oc.QuotaURL == "" {
			return nil
		}
		return []worker.QuotaCheck{{Role: "opencode", Provider: oc.QuotaProvider, URL: oc.QuotaURL}}
	case queue.StageQuestion:
		return worker.QuotaChecksFor(cfg.Teacher.Roles, roles.Generator)
	case queue.StagePresolve:
		return worker.QuotaChecksFor(cfg.Teacher.Roles, roles.Judge)
	case queue.StageAnswer:
		return worker.QuotaChecksFor(cfg.Teacher.Roles, roles.Teacher, roles.Translator)
	case queue.StageVerify:
		return worker.QuotaChecksFor(cfg.Teacher.Roles, roles.Verifier)
	}
	return nil
}

// newStageGate builds the stateless quota gate for one stage from the wired teacher
// client (the /quota fetcher) and the stage's checks (todos sec 6-3-4).
func newStageGate(d stageDeps, stage queue.Stage) *worker.StatusGate {
	g := worker.NewStatusGate(d.client, stageQuotaChecks(d.cfg, stage), d.cfg.Pacing)
	g.Log = os.Stderr
	return g
}

// buildComprehender selects the comprehend stage's digest producer from config:
// the opencode agentic CLI (source fetch + auto-compact, subgate-backed) when
// enabled, else the no-network provenance fallback so the stage runs without the
// binary (todos sec 2-5-4). Nothing about the endpoint is hardcoded here -- the
// subgate baseURL/agent live in the opencode config file, the model/agent/paths
// come from settings.yaml.
func buildComprehender(cfg runner.Config) worker.Comprehender {
	oc := cfg.Comprehend.Opencode
	if !oc.Enabled {
		return worker.ProvenanceComprehender{}
	}
	return worker.NewOpencodeComprehender(worker.OpencodeConfig{
		BinPath:    oc.BinPath,
		ConfigPath: oc.ConfigPath,
		Model:      oc.Model,
		Agent:      oc.Agent,
		Timeout:    oc.Timeout(),
	}, os.Stderr)
}

// runLoop invokes once, repeating on the poll interval while loop is set. A
// single pass (loop=false) runs once and returns. Context cancellation (SIGTERM)
// ends the loop cleanly.
func runLoop(ctx context.Context, loop bool, interval time.Duration, once func(context.Context) error) error {
	for {
		if err := once(ctx); err != nil {
			return err
		}
		if !loop {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

// stageDeps carries the wired dependencies handed to each stage worker's build
// closure, so a command constructs only its specific worker.
type stageDeps struct {
	conn     *pgx.Conn
	store    *worker.PGStore
	pipe     *pipeline.Pipeline
	client   *teacher.Client
	cfg      runner.Config
	workerID string
}

// stageWorker is the shared setup for the five always-on stage workers: load
// config, open the DB, build the teacher client + pipeline + store, then hand
// the wired pieces to build so the caller constructs its specific worker.
func stageWorker(c *cli.Context, name string, build func(d stageDeps) func(context.Context) error) error {
	ctx := c.Context
	cfg, err := runner.LoadConfig(c.String("config"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] config load failed: %v\n", name, err)
		return err
	}
	conn, err := openDB(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] %v\n", name, err)
		return err
	}
	defer conn.Close(ctx)

	client, err := teacher.NewClient(cfg.Teacher)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] teacher client init failed: %v\n", name, err)
		return err
	}
	pipe, err := buildPipeline(cfg, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] pipeline init failed: %v\n", name, err)
		return err
	}

	once := build(stageDeps{
		conn: conn, store: buildStore(conn, cfg.Worker.StaleLock()), pipe: pipe, client: client,
		cfg: cfg, workerID: worker.ResolveWorkerID(),
	})
	return runLoop(ctx, c.Bool("loop"), c.Duration("poll-interval"), once)
}

// migrateCommand applies the pipeline schema once, then exits. It is the single
// place that creates tables (queue state machine + distillation_pairs buffer,
// internal/db/schema.sql). Deployments run it before the workers -- the compose
// migrate service and the k8s migrate Job -- so no worker ever creates or checks
// the schema itself.
func migrateCommand() *cli.Command {
	return &cli.Command{
		Name:  "migrate",
		Usage: "Apply the pipeline schema once and exit (run before the workers)",
		Action: func(c *cli.Context) error {
			ctx := c.Context
			conn, err := db.Connect(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[migrate] DB connection failed: %v\n", err)
				return err
			}
			defer conn.Close(ctx)
			if err := db.Migrate(ctx, conn); err != nil {
				fmt.Fprintf(os.Stderr, "[migrate] %v\n", err)
				return err
			}
			fmt.Fprintln(os.Stderr, "[migrate] schema applied")
			return nil
		},
	}
}

// ingestCommand is the periodic source-discovery job (1 pass).
func ingestCommand() *cli.Command {
	return &cli.Command{
		Name:  "ingest",
		Usage: "Discover source documents and enqueue them as work items (periodic, one batch)",
		Flags: []cli.Flag{configFlag()},
		Action: func(c *cli.Context) error {
			ctx := c.Context
			// Ingest only needs the `ingest:` section; it does not touch teacher
			// roles or pacing, so it loads that section alone rather than the full
			// (role-validated) run config.
			ingestCfg, err := ingest.LoadConfig(c.String("config"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ingest] ingest config load failed: %v\n", err)
				return err
			}

			conn, err := openDB(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ingest] %v\n", err)
				return err
			}
			defer conn.Close(ctx)

			reg := ingest.NewRegistry(common.DefaultHTTPClient())
			sources := reg.BuildEnabled(ingestCfg)
			// Per-run batch size from config (ingest.limit_per_source); the run
			// CADENCE is set by the scheduler (ofelia/CronJob), never here. 0 =
			// built-in default.
			limit := ingestCfg.LimitPerSource
			if limit <= 0 {
				limit = ingestLimitPerSource
			}
			w := worker.NewIngestWorker(queue.NewStore(conn, 0), sources, limit)
			w.Log = os.Stderr
			res, err := w.RunOnce(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ingest] failed: %v\n", err)
				return err
			}
			fmt.Fprintf(os.Stderr, "[ingest] enqueued=%d duplicates=%d failed=%d\n", res.Enqueued, res.Duplicates, res.Failed)
			return nil
		},
	}
}

// comprehendCommand drains the comprehend stage (always-on).
func comprehendCommand() *cli.Command {
	return &cli.Command{
		Name:  "comprehend",
		Usage: "Digest queued source documents for backtranslation (always-on with --loop)",
		Flags: loopFlags(),
		Action: func(c *cli.Context) error {
			return stageWorker(c, "comprehend", func(d stageDeps) func(context.Context) error {
				w := worker.NewComprehendWorker(d.store, buildComprehender(d.cfg), newStageGate(d, queue.StageComprehend), d.workerID)
				w.Log = os.Stderr
				return func(ctx context.Context) error { _, err := w.RunOnce(ctx); return err }
			})
		},
	}
}

// questionCommand drains the question stage (always-on).
func questionCommand() *cli.Command {
	return &cli.Command{
		Name:  "question",
		Usage: "Reverse-generate questions from comprehended items (always-on with --loop)",
		Flags: loopFlags(),
		Action: func(c *cli.Context) error {
			return stageWorker(c, "question", func(d stageDeps) func(context.Context) error {
				w := worker.NewQuestionWorker(d.store, d.pipe, newStageGate(d, queue.StageQuestion), d.workerID)
				w.Log = os.Stderr
				return func(ctx context.Context) error { _, err := w.RunOnce(ctx); return err }
			})
		},
	}
}

// presolveCommand drains the presolve stage (always-on).
func presolveCommand() *cli.Command {
	return &cli.Command{
		Name:  "presolve",
		Usage: "Student pre-filter gate: drop questions the student already solves (always-on with --loop)",
		Flags: loopFlags(),
		Action: func(c *cli.Context) error {
			return stageWorker(c, "presolve", func(d stageDeps) func(context.Context) error {
				// presolve is enabled unless the pipeline runs in taxonomy mode
				// (no student filter there); the student self-consistency k>0 is the
				// artifact-mode default.
				enabled := d.cfg.Pipeline.Mode == runner.ModeArtifact
				w := worker.NewPresolveWorker(d.store, d.pipe, enabled, newStageGate(d, queue.StagePresolve), d.workerID)
				w.Log = os.Stderr
				return func(ctx context.Context) error { _, err := w.RunOnce(ctx); return err }
			})
		},
	}
}

// answerCommand drains the answer stage under the pacing gate (always-on).
func answerCommand() *cli.Command {
	return &cli.Command{
		Name:  "answer",
		Usage: "Generate paced teacher trajectories for presolved items (always-on with --loop)",
		Flags: loopFlags(),
		Action: func(c *cli.Context) error {
			return stageWorker(c, "answer", func(d stageDeps) func(context.Context) error {
				w := worker.NewAnswerWorker(d.store, d.pipe, newStageGate(d, queue.StageAnswer), d.workerID)
				w.Log = os.Stderr
				return func(ctx context.Context) error { _, err := w.RunOnce(ctx); return err }
			})
		},
	}
}

// verifyCommand drains the verify stage (always-on).
func verifyCommand() *cli.Command {
	return &cli.Command{
		Name:  "verify",
		Usage: "Verify answered trajectories; drop failing lanes (always-on with --loop)",
		Flags: loopFlags(),
		Action: func(c *cli.Context) error {
			return stageWorker(c, "verify", func(d stageDeps) func(context.Context) error {
				w := worker.NewVerifyWorker(d.store, d.pipe, newStageGate(d, queue.StageVerify), d.workerID)
				w.Log = os.Stderr
				return func(ctx context.Context) error { _, err := w.RunOnce(ctx); return err }
			})
		},
	}
}

// flushCommand projects flush-gated items to the buffer and exports it (periodic).
func flushCommand() *cli.Command {
	return &cli.Command{
		Name:  "flush",
		Usage: "Project flush-gated items to distillation_pairs and export the buffer to the export volume (periodic)",
		Flags: []cli.Flag{configFlag()},
		Action: func(c *cli.Context) error {
			ctx := c.Context
			cfg, err := runner.LoadConfig(c.String("config"))
			if err != nil {
				fmt.Fprintf(os.Stderr, "[flush] config load failed: %v\n", err)
				return err
			}
			conn, err := openDB(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[flush] %v\n", err)
				return err
			}
			defer conn.Close(ctx)

			store := buildStore(conn, cfg.Worker.StaleLock())
			w := worker.NewFlushWorker(store, flush.New(cfg.Flush, conn), worker.ResolveWorkerID())
			w.Log = os.Stderr
			res, err := w.RunOnce(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[flush] failed: %v\n", err)
				return err
			}
			fmt.Fprintf(os.Stderr, "[flush] items=%d records=%d flushed=%v rows=%d\n",
				res.ItemsProjected, res.RecordsWritten, res.Flushed, res.RowsFlushed)
			return nil
		},
	}
}
