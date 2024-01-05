//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/benchmark_bm25/lib"
)

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.PersistentFlags().IntVarP(&BatchSize, "batch-size", "b", DefaultBatchSize, "number of objects in a single import batch")
	importCmd.PersistentFlags().IntVarP(&MultiplyProperties, "multiply-properties", "m", DefaultMultiplyProperties, "create artifical copies of real properties by setting a value larger than 1. The properties have identical contents, so it won't alter results, but leads to many more calculations.")
	importCmd.PersistentFlags().BoolVarP(&Vectorizer, "vectorizer", "v", DefaultVectorizer, "Vectorize import data with default vectorizer")
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
			return fmt.Errorf("weaviate is not ready at %v: %w", Origin, err)
		}

		if !ok {
			return fmt.Errorf("weaviate is not ready")
		}

		datasets, err := lib.ParseDatasetConfig(DatasetConfigPath)
		if err != nil {
			return fmt.Errorf("parse dataset cfg file: %w", err)
		}

		c, err := lib.ParseCorpi(datasets.Datasets[0], MultiplyProperties)
		if err != nil {
			return err
		}

		if err := client.Schema().AllDeleter().Do(context.Background()); err != nil {
			return fmt.Errorf("clear schema prior to import: %w", err)
		}

		if err := client.Schema().ClassCreator().
			WithClass(lib.SchemaFromDataset(datasets.Datasets[0], Vectorizer)).
			Do(context.Background()); err != nil {
			return fmt.Errorf("create schema for %s: %w", datasets.Datasets[0], err)
		}

		batch := client.Batch().ObjectsBatcher()
		for i, corp := range c {
			id := uuid.MustParse(fmt.Sprintf("%032x", i)).String()
			props := map[string]interface{}{
				"modulo_10":   i % 10,
				"modulo_100":  i % 100,
				"modulo_1000": i % 1000,
			}

			for key, value := range corp {
				props[key] = value
			}

			batch.WithObjects(&models.Object{
				ID:         strfmt.UUID(id),
				Class:      lib.ClassNameFromDatasetID(datasets.Datasets[0].ID),
				Properties: props,
			})

			if i != 0 && i%BatchSize == 0 {
				br, err := batch.Do(context.Background())
				if err != nil {
					return fmt.Errorf("batch %d: %w", i, err)
				}

				if err := lib.HandleBatchResponse(br); err != nil {
					return err
				}
			}

			if i%1000 == 0 {
				log.Printf("imported %d/%d objects", i, len(c))
			}
		}

		if len(c)&BatchSize != 0 {
			// we need to send one final batch
			br, err := batch.Do(context.Background())
			if err != nil {
				return fmt.Errorf("final batch: %w", err)
			}
			if err := lib.HandleBatchResponse(br); err != nil {
				return err
			}
			log.Printf("imported %d/%d objects", len(c), len(c))
		}

		return nil
	},
}
