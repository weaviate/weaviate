package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate/test/benchmark_bm25/lib"
)

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().IntVarP(&BatchSize, "batch-size", "b", DefaultBatchSize, "number of objects in a single import batch")
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import a dataset (or multiple datasets) into Weaviate",
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := lib.ClientFromOrigin(Origin)
		if err != nil {
			return err
		}

		ok, err := client.Misc().LiveChecker().Do(context.Background())
		if err != nil {
			return fmt.Errorf("weaviate is not ready: %w", err)
		}

		if !ok {
			return fmt.Errorf("weaviate is not ready")
		}

		datasets, err := lib.ParseDatasetConfig(DatasetConfigPath)
		if err != nil {
			return fmt.Errorf("parse dataset cfg file: %w", err)
		}

		c, err := lib.ParseCorpi(datasets.Datasets[0])
		if err != nil {
			return err
		}

		if err := client.Schema().AllDeleter().Do(context.Background()); err != nil {
			return fmt.Errorf("clear schema prior to import: %w", err)
		}

		if err := client.Schema().ClassCreator().
			WithClass(lib.SchemaFromDataset(datasets.Datasets[0])).
			Do(context.Background()); err != nil {
			return fmt.Errorf("create schema for %s: %w", datasets.Datasets[0], err)
		}

		_ = c
		fmt.Println(BatchSize)

		return nil
	},
}
