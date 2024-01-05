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
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/test/benchmark_bm25/lib"
)

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.PersistentFlags().IntVarP(&BatchSize, "batch-size", "b", DefaultBatchSize, "number of objects in a single import batch")
	queryCmd.PersistentFlags().IntVarP(&QueriesCount, "count", "c", DefaultQueriesCount, "run only the specified amount of queries, negative numbers mean unlimited")
	queryCmd.PersistentFlags().IntVarP(&FilterObjectPercentage, "filter", "f", DefaultFilterObjectPercentage, "The given percentage of objects are filtered out. Off by default, use <=0 to disable")
	queryCmd.PersistentFlags().Float32VarP(&Alpha, "alpha", "a", DefaultAlpha, "Weighting for keyword vs vector search. Alpha = 0 (Default) is pure BM25 search.")
	queryCmd.PersistentFlags().StringVarP(&Ranking, "ranking´", "r", DefaultRanking, "Which ranking algorithm should be used for hybrid search, rankedFusion (default) and relativeScoreFusion.")
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

		scores := lib.Scores{}
		propNameWithId := lib.SanitizePropName(ds.Queries.PropertyWithId)
		className := lib.ClassNameFromDatasetID(ds.ID)
		for i, query := range q {
			before := time.Now()
			queryBuilder := client.GraphQL().Get().WithClassName(className).WithLimit(100).WithFields(graphql.Field{Name: "_additional { id }"}, graphql.Field{Name: propNameWithId})
			if Alpha == 0 {
				bm25 := &graphql.BM25ArgumentBuilder{}
				bm25.WithQuery(query.Query)
				queryBuilder.WithBM25(bm25)
			} else {
				hybrid := &graphql.HybridArgumentBuilder{}
				ranking := graphql.FusionType(Ranking)
				hybrid.WithQuery(query.Query).WithAlpha(Alpha).WithFusionType(ranking)
				queryBuilder.WithHybrid(hybrid)
			}
			if FilterObjectPercentage > 0 {
				filter := filters.Where()
				filter.WithPath([]string{"modulo_100"})
				filter.WithOperator(filters.GreaterThan)
				filter.WithValueInt(int64(FilterObjectPercentage))
				queryBuilder = queryBuilder.WithWhere(filter)
			}
			result, err := queryBuilder.Do(context.Background())
			if err != nil {
				return err
			}

			if result.Errors != nil {
				return errors.New(result.Errors[0].Message)
			}
			times = append(times, time.Since(before))

			logMsg := fmt.Sprintf("completed %d/%d queries.", i, len(q))

			if len(query.MatchingIds) > 0 && len(ds.Queries.PropertyWithId) > 0 {
				resultIds := result.Data["Get"].(map[string]interface{})[className].([]interface{})
				if err := scores.AddResult(query.MatchingIds, resultIds, propNameWithId); err != nil {
					return err
				}
				logMsg += fmt.Sprintf("nDCG score: %.04f", scores.CurrentNDCG())
			}
			if i%1000 == 0 && i > 0 {
				log.Printf(logMsg)
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
		scores.PrettyPrint()

		return nil
	},
}
