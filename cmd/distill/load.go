package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/loader"
)

func loadCommand() *cli.Command {
	return &cli.Command{
		Name:      "load",
		Usage:     "Load JSONL files into PostgreSQL",
		ArgsUsage: "<file1.jsonl> [file2.jsonl ...]",
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				return fmt.Errorf("at least one JSONL file path is required")
			}

			ctx := c.Context

			conn, err := db.Connect(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[load] DB connection failed: %v\n", err)
				return err
			}
			defer conn.Close(ctx)

			if err := loader.CreateTable(ctx, conn); err != nil {
				fmt.Fprintf(os.Stderr, "[load] create table failed: %v\n", err)
				return err
			}

			var totalInserted, totalSkipped, totalFailed int

			for _, filePath := range c.Args().Slice() {
				if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
					fmt.Fprintf(os.Stderr, "[load] file not found, skipping: %s\n", filePath)
					continue
				}

				inserted, skipped, failed, processErr := loader.ProcessFile(ctx, conn, filePath)
				if processErr != nil {
					fmt.Fprintf(os.Stderr, "[load] error processing %s: %v\n", filePath, processErr)
					continue
				}

				totalInserted += inserted
				totalSkipped += skipped
				totalFailed += failed

				fmt.Fprintf(os.Stderr, "[load] %s: inserted=%d skipped=%d failed=%d\n",
					filePath, inserted, skipped, failed)
			}

			fmt.Fprintf(os.Stdout, "total: inserted=%d skipped=%d failed=%d\n",
				totalInserted, totalSkipped, totalFailed)

			if totalFailed > 0 {
				return fmt.Errorf("%d record(s) failed to load", totalFailed)
			}
			return nil
		},
	}
}
