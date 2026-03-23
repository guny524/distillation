package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/coverage"
	"github.com/guny524/distillation/internal/db"
)

func coverageCommand() *cli.Command {
	return &cli.Command{
		Name:  "coverage",
		Usage: "Dump 4-axis coverage from PostgreSQL to JSON",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "output",
				Value: coverage.DefaultOutput,
				Usage: "output file path for coverage JSON",
			},
		},
		Action: func(c *cli.Context) error {
			ctx := c.Context
			outputPath := c.String("output")

			conn, err := db.Connect(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[coverage] DB connection failed: %v\n", err)
				return err
			}
			defer conn.Close(ctx)

			exists, err := coverage.TableExists(ctx, conn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[coverage] table check failed: %v\n", err)
				return err
			}

			var cov *coverage.Coverage
			if exists {
				cov, err = coverage.BuildCoverage(ctx, conn)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[coverage] build coverage failed: %v\n", err)
					return err
				}
			} else {
				cov = coverage.BuildEmptyCoverage()
				fmt.Fprintf(os.Stderr, "[coverage] table not found, writing empty coverage\n")
			}

			data, err := json.MarshalIndent(cov, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal coverage JSON: %w", err)
			}

			if dir := filepath.Dir(outputPath); dir != "." {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return fmt.Errorf("create output dir %s: %w", dir, err)
				}
			}

			if err := os.WriteFile(outputPath, data, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", outputPath, err)
			}

			fmt.Fprintf(os.Stderr, "[coverage] wrote %s (%d bytes, total_count=%d)\n",
				outputPath, len(data), cov.TotalCount)
			return nil
		},
	}
}
