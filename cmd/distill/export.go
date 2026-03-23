package main

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/urfave/cli/v2"

	"github.com/guny524/distillation/internal/db"
	"github.com/guny524/distillation/internal/exporter"
)

func exportCommand() *cli.Command {
	return &cli.Command{
		Name:  "export",
		Usage: "Export PostgreSQL to Parquet files",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "output-dir",
				Value: exporter.DefaultOutputDir,
				Usage: "output directory for Parquet shard files",
			},
			&cli.IntFlag{
				Name:  "shard-size",
				Value: exporter.DefaultShardSize,
				Usage: "maximum rows per Parquet shard file",
			},
			&cli.StringFlag{
				Name:  "compression",
				Value: exporter.DefaultCompression,
				Usage: "Parquet compression codec (zstd, snappy, gzip, none)",
			},
		},
		Action: func(c *cli.Context) error {
			ctx := c.Context
			outputDir := c.String("output-dir")
			shardSize := c.Int("shard-size")
			compression := c.String("compression")

			conn, err := db.Connect(ctx)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[export] DB connection failed: %v\n", err)
				return err
			}
			defer conn.Close(ctx)

			rows, err := exporter.FetchAllRows(ctx, conn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[export] fetch rows failed: %v\n", err)
				return err
			}

			record, err := exporter.BuildTable(rows, memory.DefaultAllocator)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[export] build table failed: %v\n", err)
				return err
			}
			defer record.Release()

			numShards, err := exporter.WriteShards(record, outputDir, shardSize, compression)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[export] write shards failed: %v\n", err)
				return err
			}

			fmt.Fprintf(os.Stdout, "exported %d rows to %d shard(s) in %s\n",
				record.NumRows(), numShards, outputDir)
			return nil
		},
	}
}
