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
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	"github.com/spf13/cobra"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/test/benchmark_bm25/lib"
)

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.PersistentFlags().IntVarP(&BatchSize, "batch-size", "b", DefaultBatchSize, "number of objects in a single import batch")
	queryCmd.PersistentFlags().IntVarP(&QueriesCount, "count", "c", DefaultQueriesCount, "run only the specified amount of queries, negative numbers mean unlimited")
	queryCmd.PersistentFlags().IntVarP(&FilterObjectPercentage, "filter", "f", DefaultFilterObjectPercentage, "The given percentage of objects are filtered out. Off by default, use <=0 to disable")
	queryCmd.PersistentFlags().BoolVarP(&DumpResults, "dump-results", "d", false, "Print results for inspection")
}

type querySummary struct {
	Id             int
	Query          string
	MatchingIds    []int
	MatchingCorpus []lib.CorpusData
	Explanations []string
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

		corpus := lib.LoadCorpus(ds)

		// Put the corpus into a hashtable
		corpusMap := map[int]lib.CorpusData{}
		for _, v := range corpus {
			corpusMap[v.Id] = v
		}

		log.Print("queries parsed")

		times := []time.Duration{}
		querySumm := map[int]querySummary{}

		log.Print("start querying")

		scores := lib.Scores{}
		propNameWithId := lib.SanitizePropName(ds.Queries.PropertyWithId)
		className := lib.ClassNameFromDatasetID(ds.ID)
		for i, query := range q {
			before := time.Now()
			bm25 := &graphql.BM25ArgumentBuilder{}
			bm25.WithQuery(query.Query)

			bm25Query := client.GraphQL().Get().WithClassName(className).
				WithLimit(100).WithBM25(bm25).WithFields(graphql.Field{Name: "_additional { id explainScore }"}, graphql.Field{Name: propNameWithId})

			if FilterObjectPercentage > 0 {
				filter := filters.Where()
				filter.WithPath([]string{"modulo_100"})
				filter.WithOperator(filters.GreaterThan)
				filter.WithValueInt(int64(FilterObjectPercentage))
				bm25Query = bm25Query.WithWhere(filter)
			}

			result, err := bm25Query.Do(context.Background())
			if err != nil {
				return err
			}
			if result.Errors != nil {
				return errors.New(result.Errors[0].Message)
			}
			times = append(times, time.Since(before))

			logMsg := fmt.Sprintf("completed %d/%d queries.", i, len(q))

			qs := querySummary{Id: query.Id, Query: query.Query}

			if len(query.MatchingIds) > 0 && len(ds.Queries.PropertyWithId) > 0 {
				results := result.Data["Get"].(map[string]interface{})[className].([]interface{})
				//fmt.Printf("resultIds: %+v\n", results)
				matched, err , explanations:= scores.AddResult(query.MatchingIds, results, propNameWithId)
				if err != nil {
					return err
				}

				for i, id := range matched {
					qs.MatchingIds = append(qs.MatchingIds, id)
					qs.MatchingCorpus = append(qs.MatchingCorpus, corpusMap[id])
					qs.Explanations = append(qs.Explanations, string(explanations[i]))
				}
				querySumm[query.Id] = qs
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

		// Put all the query summaries into an array and sort it
		querySummaries := []querySummary{}
		for _, v := range querySumm {
			querySummaries = append(querySummaries, v)
		}
		sort.Slice(querySummaries, func(i, j int) bool {
			return querySummaries[i].Id < querySummaries[j].Id
		})
		
		if DumpResults {
		// Loop over querySummaries and print it neatly
		fmt.Println("Query results:")
		for _, qs := range querySummaries {
			if len(qs.MatchingIds) == 0 {
				continue
			}
			fmt.Printf("Query %d: %s\n", qs.Id, qs.Query)
			//fmt.Printf("Matching ids: %v\n", qs.MatchingIds)
			fmt.Printf("Matching corpus: %v\n",qs.MatchingIds )
			for i, c := range qs.MatchingCorpus {
				fmt.Printf("Match: %v, %v\n", c.Id, c.Text)
				fmt.Printf("Explanation: %v\n", qs.Explanations[i])
			}
		}
	}

		return nil
	},
}
