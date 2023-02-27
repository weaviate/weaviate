//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/test/benchmark_bm25/lib"
)

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.PersistentFlags().IntVarP(&BatchSize, "batch-size", "b", DefaultBatchSize, "number of objects in a single import batch")
	queryCmd.PersistentFlags().IntVarP(&QueriesCount, "count", "c", DefaultQueriesCount, "run only the specified amount of queries, negative numbers mean unlimited")
}

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Send queries for a dataset",
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
		log.Print("weaviate is ready")

		datasets, err := lib.ParseDatasetConfig(DatasetConfigPath)
		if err != nil {
			return fmt.Errorf("parse dataset cfg file: %w", err)
		}

		ds := datasets.Datasets[0]

		log.Print("parse queries")
		q, err := lib.ParseQueries(ds, QueriesCount)
		if err != nil {
			return err
		}
		log.Print("queries parsed")

		times := []time.Duration{}

		log.Print("start querying")

		for i, query := range q {
			before := time.Now()
			bm25 := &graphql.BM25ArgumentBuilder{}
			bm25.WithQuery(query)

			_, err := client.GraphQL().Get().WithClassName(lib.ClassNameFromDatasetID(ds.ID)).
				WithLimit(10).WithBM25(bm25).WithFields(graphql.Field{Name: "_additional { id }"}).Do(context.Background())
			if err != nil {
				return err
			}

			times = append(times, time.Since(before))

			if i%1000 == 0 {
				log.Printf("completed %d/%d queries", i, len(q))
			}
		}

		meta, err := client.GraphQL().Aggregate().WithClassName(lib.ClassNameFromDatasetID(ds.ID)).
			WithFields(graphql.Field{Name: "meta", Fields: []graphql.Field{{Name: "count"}}}).
			Do(context.Background())
		if err != nil {
			return err
		}

		objCount := int(meta.Data["Aggregate"].(map[string]interface{})[lib.ClassNameFromDatasetID(ds.ID)].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"].(float64))

		fmt.Printf("\nObjects imported: %d\n", objCount)
		stat := lib.AnalyzeLatencies(times)
		stat.PrettyPrint()
		return nil
	},
}
