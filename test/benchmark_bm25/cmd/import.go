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
	"strconv"
	"time"

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
	importCmd.PersistentFlags().IntVarP(&QueriesCount, "count", "c", DefaultQueriesCount, "run only the specified amount of queries, negative numbers mean unlimited")
	importCmd.PersistentFlags().IntVarP(&FilterObjectPercentage, "filter", "f", DefaultFilterObjectPercentage, "The given percentage of objects are filtered out. Off by default, use <=0 to disable")
	importCmd.PersistentFlags().Float32VarP(&Alpha, "alpha", "a", DefaultAlpha, "Weighting for keyword vs vector search. Alpha = 0 (Default) is pure BM25 search.")
	importCmd.PersistentFlags().StringVarP(&Ranking, "ranking", "r", DefaultRanking, "Which ranking algorithm should be used for hybrid search, rankedFusion (default) and relativeScoreFusion.")
	importCmd.PersistentFlags().IntVarP(&QueriesInterval, "query-interval", "i", DefaultQueriesInterval, "run queries every this number of inserts")
}

type IndexingExperimentResult struct {
	// The name of the dataset
	Dataset string
	// The number of objects in the dataset
	Objects int
	// The batch size used for importing
	MultiplyProperties int
	// Docs where vectorized
	Vectorize bool
	// The time it took to import the dataset
	ImportTime float64
	// Average time to import 1000 objects
	ImportTimePer1000 float64
	// Objects per second
	ObjectsPerSecond float64
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

		if err := client.Schema().AllDeleter().Do(context.Background()); err != nil {
			return fmt.Errorf("clear schema prior to import: %w", err)
		}

		experimentResults := make([]IndexingExperimentResult, len(datasets.Datasets))
		queryResults := make([]*QueryExperimentResult, 0)

		for di, dataset := range datasets.Datasets {

			c, err := lib.ParseCorpi(dataset, MultiplyProperties)
			if err != nil {
				return err
			}

			if err := client.Schema().ClassCreator().
				WithClass(lib.SchemaFromDataset(dataset, Vectorizer)).
				Do(context.Background()); err != nil {
				return fmt.Errorf("create schema for %s: %w", dataset, err)
			}

			queries := make([]lib.Query, 0)
			if QueriesInterval != -1 {
				log.Print("parse queries")
				queries, err = lib.ParseQueries(dataset, QueriesCount)
				if err != nil {
					return err
				}
				log.Print("queries parsed")
			}

			start := time.Now()
			startBatch := time.Now()
			batch := client.Batch().ObjectsBatcher()
			for i, corp := range c {
				id := uuid.MustParse(fmt.Sprintf("%032x", i)).String()
				props := map[string]interface{}{
					"modulo_10":   i % 10,
					"modulo_100":  i % 100,
					"modulo_1000": i % 1000,
				}
				indexCount := i + 1

				for key, value := range corp {
					props[key] = value
				}

				batch.WithObjects(&models.Object{
					ID:         strfmt.UUID(id),
					Class:      lib.ClassNameFromDatasetID(dataset.ID),
					Properties: props,
				})

				if indexCount%BatchSize == 0 {
					br, err := batch.Do(context.Background())
					if err != nil {
						return fmt.Errorf("batch %d: %w", indexCount, err)
					}

					if err := lib.HandleBatchResponse(br); err != nil {
						return err
					}
				}

				if indexCount%1000 == 0 {
					totalTimeBatch := time.Since(startBatch).Seconds()
					totalTime := time.Since(start).Seconds()
					startBatch = time.Now()
					log.Printf("imported %d/%d objects in %.3f, time per 1k objects: %.3f, objects per second: %.0f", indexCount, len(c), totalTimeBatch, totalTime/float64(indexCount)*1000, float64(indexCount)/totalTime)
				}

				if QueriesInterval > 0 && indexCount%QueriesInterval == 0 {
					result, err := query(client, queries, dataset)
					if err != nil {
						return fmt.Errorf("query: %w", err)
					}
					queryResults = append(queryResults, result)

				}
			}

			if len(c)%BatchSize != 0 {
				// we need to send one final batch
				br, err := batch.Do(context.Background())
				if err != nil {
					return fmt.Errorf("final batch: %w", err)
				}
				if err := lib.HandleBatchResponse(br); err != nil {
					return err
				}
				totalTimeBatch := time.Since(startBatch).Seconds()
				totalTime := time.Since(start).Seconds()
				log.Printf("imported finished %d/%d objects in %.3f, time per 1k objects: %.3f, objects per second: %.0f", len(c), len(c), totalTimeBatch, totalTime/float64(len(c))*1000, float64(len(c))/totalTime)
			}

			if QueriesInterval != -1 && len(c)%BatchSize != 0 {
				// run queries after full import
				result, err := query(client, queries, dataset)
				if err != nil {
					return fmt.Errorf("query: %w", err)
				}
				queryResults = append(queryResults, result)
			}

			experimentResults[di] = IndexingExperimentResult{
				Dataset:            dataset.ID,
				Objects:            len(c),
				MultiplyProperties: MultiplyProperties,
				Vectorize:          Vectorizer,
				ImportTime:         time.Since(start).Seconds(),
				ImportTimePer1000:  time.Since(start).Seconds() / float64(len(c)) * 1000,
				ObjectsPerSecond:   float64(len(c)) / time.Since(start).Seconds(),
			}
		}
		// pretty print results fas TSV
		fmt.Printf("\nIndexing Results:\n")
		fmt.Printf("Dataset\tObjects\tMultiplyProperties\tVectorizer\tImportTime\tImportTimePer1000\tObjectsPerSecond\n")
		for _, result := range experimentResults {
			fmt.Printf("%s\t%d\t%d\t%t\t%.3f\t%.3f\t%.0f\n", result.Dataset, result.Objects, result.MultiplyProperties, result.Vectorize, result.ImportTime, result.ImportTimePer1000, result.ObjectsPerSecond)
		}

		fmt.Printf("\nQuery Results:\n")
		fmt.Printf("Dataset\tObjects\tQueries\tFilterObjectPercentage\tAlpha\tRanking\tQueryTime\tQueryTimePer1000\tQueriesPerSecond\tQueryTimePer1000000Documents\tMin\tMax\tP50\tP90\tP99\tnDCG\tP@1\tP@5\n")
		for _, result := range queryResults {
			ranking, _ := strconv.ParseFloat(result.Ranking, 64) // Convert result.Ranking to float64
			fmt.Printf("%s\t%d\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n", result.Dataset, result.Objects, result.Queries, result.FilterObjectPercentage, result.Alpha, ranking, result.TotalQueryTime, result.QueryTimePer1000, result.QueriesPerSecond, result.QueryTimePer1000000Documents, float32(result.Min.Milliseconds())/1000.0, float32(result.Max.Milliseconds())/1000.0, float32(result.P50.Milliseconds())/1000.0, float32(result.P90.Milliseconds())/1000.0, float32(result.P99.Milliseconds())/1000.0, result.Scores.CurrentNDCG(), result.Scores.CurrentPrecisionAt1(), result.Scores.CurrentPrecisionAt5())
		}

		return nil
	},
}
