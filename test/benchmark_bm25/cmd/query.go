//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cmd

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/benchmark_bm25/lib"
)

type QueryExperimentResult struct {
	// The name of the dataset
	Dataset string
	// Dataset size
	Objects int
	// Query count
	Queries int
	// Filter object percentage
	FilterObjectPercentage int
	// Alpha
	Alpha float32
	// Ranking
	Ranking string
	// The time it took to query the dataset
	TotalQueryTime float64
	// Average query time
	AvgQueryTime time.Duration
	// Average time to query per 1000 indexed objects
	QueryTimePer1000000Documents float64
	// Objects per second
	QueriesPerSecond float64
	// Min query time
	Min time.Duration
	// Max query time
	Max time.Duration
	// P50 query time
	P50 time.Duration
	// P90 query time
	P90 time.Duration
	// P95 query time
	P99 time.Duration
	// Scores
	Scores lib.Scores
}

func init() {
	rootCmd.AddCommand(queryCmd)
	queryCmd.PersistentFlags().IntVarP(&QueriesCount, "count", "c", DefaultQueriesCount, "run only the specified amount of queries, negative numbers mean unlimited")
	queryCmd.PersistentFlags().IntVarP(&FilterObjectPercentage, "filter", "f", DefaultFilterObjectPercentage, "The given percentage of objects are filtered out. Off by default, use <=0 to disable")
	queryCmd.PersistentFlags().Float32VarP(&Alpha, "alpha", "a", DefaultAlpha, "Weighting for keyword vs vector search. Alpha = 0 (Default) is pure BM25 search.")
	queryCmd.PersistentFlags().StringVarP(&Ranking, "ranking", "r", DefaultRanking, "Which ranking algorithm should be used for hybrid search, rankedFusion (default) and relativeScoreFusion.")
	queryCmd.PersistentFlags().IntVarP(&Limit, "limit", "l", DefaultLimit, "Limit the number of results returned by the query")
	queryCmd.PersistentFlags().BoolVarP(&AdditionalExplanations, "additional-explanations", "e", DefaultAdditionalExplanations, "Request additional explanations for the query results")
	queryCmd.PersistentFlags().BoolVarP(&PrintDetailedResults, "print-detailed-results", "p", DefaultPrintDetailedResults, "Print detailed results")
}

func writeQueryResultsToFile(results *models.GraphQLResponse, filename, collection, queryId string, additionalExplanations bool) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"queryId", "id", "score"}
	if additionalExplanations {
		header = append(header, "explainScore")
	}

	// Write the header
	writer.Write(header)
	for _, obj := range results.Data["Get"].(map[string]interface{})[collection].([]interface{}) {
		id := obj.(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
		score := obj.(map[string]interface{})["_additional"].(map[string]interface{})["score"].(string)

		// round score to 2 decimal places
		scoreFloat, err := strconv.ParseFloat(score, 64)
		if err != nil {
			log.Fatal(err)
		}
		score = fmt.Sprintf("%.2f", scoreFloat)

		line := []string{queryId, id, score}
		if additionalExplanations {
			line = append(line, obj.(map[string]interface{})["_additional"].(map[string]interface{})["explainScore"].(string))
		}

		writer.Write(line)
	}
}

func query(client *weaviate.Client, q lib.Queries, ds lib.Dataset, index int) (*QueryExperimentResult, error) {
	propNameWithId := lib.SanitizePropName(ds.Queries.PropertyWithId)
	propertiesToMatch := ds.Queries.PropertiesToMatch
	for i := 0; i < len(propertiesToMatch); i++ {
		propertiesToMatch[i] = lib.SanitizePropName(propertiesToMatch[i])
	}
	className := lib.ClassNameFromDatasetID(ds.ID)
	times := []time.Duration{}
	scores := lib.Scores{}

	// unix timestamp as string
	t := strconv.FormatInt(time.Now().Unix(), 10)
	propName := strings.Join(propertiesToMatch, "_")
	if len(propertiesToMatch) == 0 {
		propName = "all"
	}
	if index >= 0 {
		propName = propName + "_" + strconv.Itoa(index)
	}
	filename := t + "_" + className + "_" + propName + ".csv"

	additionalFlags := "id"
	if PrintDetailedResults {
		additionalFlags = "id score explainScore"
	}
	for i, query := range q {
		before := time.Now()
		var queryBuilder *graphql.GetBuilder

		queryBuilder = client.GraphQL().Get().WithClassName(className).WithLimit(Limit).WithFields(graphql.Field{Name: "_additional { " + additionalFlags + " }"}, graphql.Field{Name: propNameWithId})
		if Alpha == 0 {
			bm25 := &graphql.BM25ArgumentBuilder{}
			bm25.WithQuery(query.Query)
			bm25.WithProperties(propertiesToMatch...)
			queryBuilder.WithBM25(bm25)
		} else {
			hybrid := &graphql.HybridArgumentBuilder{}
			ranking := graphql.FusionType(Ranking)
			hybrid.WithQuery(query.Query).WithAlpha(Alpha).WithFusionType(ranking)
			hybrid.WithProperties(propertiesToMatch)
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
			return nil, err
		}

		if result.Errors != nil {
			return nil, errors.New(result.Errors[0].Message)
		}
		times = append(times, time.Since(before))

		// print result scores and ids to a csv file
		if PrintDetailedResults {
			queryId := strconv.Itoa(i)
			writeQueryResultsToFile(result, filename, className, queryId, AdditionalExplanations)
		}

		logMsg := fmt.Sprintf("completed %d/%d queries.", i, len(q))

		if len(query.MatchingIds) > 0 && len(ds.Queries.PropertyWithId) > 0 {
			resultIds := result.Data["Get"].(map[string]interface{})[className].([]interface{})
			if err := scores.AddResult(query.MatchingIds, resultIds, propNameWithId); err != nil {
				return nil, err
			}
			logMsg += fmt.Sprintf("nDCG score: %.04f", scores.CurrentNDCG())
		}
		if i%1000 == 0 && i > 0 {
			log.Print(logMsg)
		}
	}

	meta, err := client.GraphQL().Aggregate().WithClassName(lib.ClassNameFromDatasetID(ds.ID)).
		WithFields(graphql.Field{Name: "meta", Fields: []graphql.Field{{Name: "count"}}}).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	objCount := int(meta.Data["Aggregate"].(map[string]interface{})[lib.ClassNameFromDatasetID(ds.ID)].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"].(float64))

	stat := lib.AnalyzeLatencies(times)
	stat.PrettyPrint()
	scores.PrettyPrint()
	totalTime := 0.0
	for _, t := range times {
		totalTime += t.Seconds()
	}

	result := QueryExperimentResult{
		Dataset:                      ds.ID,
		Objects:                      objCount,
		Queries:                      len(q),
		FilterObjectPercentage:       FilterObjectPercentage,
		Alpha:                        Alpha,
		Ranking:                      Ranking,
		TotalQueryTime:               totalTime,
		AvgQueryTime:                 stat.Mean,
		QueriesPerSecond:             1 / stat.Mean.Seconds(),
		QueryTimePer1000000Documents: float64(stat.Mean.Milliseconds()) * 1000000 / float64(objCount),
		Min:                          stat.Min,
		Max:                          stat.Max,
		P50:                          stat.P50,
		P90:                          stat.P90,
		P99:                          stat.P99,
		Scores:                       scores,
	}

	return &result, nil
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

		results := make([]*QueryExperimentResult, len(datasets.Datasets))
		for di, ds := range datasets.Datasets {
			log.Print("querying dataset " + ds.ID)
			log.Print("parse queries")
			q, err := lib.ParseQueries(ds, QueriesCount)
			if err != nil {
				return err
			}
			log.Print("queries parsed")
			log.Print("start querying")

			result, err := query(client, q, ds, -1)
			if err != nil {
				return err
			}

			results[di] = result

		}

		fmt.Printf("\nQuery Results:\n")
		fmt.Printf("Dataset\tObjects\tQueries\tFilterObjectPercentage\tAlpha\tRanking\tQueryTime\tAvgQueryTime\tQueriesPerSecond\tQueryTimePer1000000Documents\tMin\tMax\tP50\tP90\tP99\tnDCG\tP@1\tP@5\n")
		for _, result := range results {
			ranking, _ := strconv.ParseFloat(result.Ranking, 64) // Convert result.Ranking to float64
			fmt.Printf("%s\t%d\t%d\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n", result.Dataset, result.Objects, result.Queries, result.FilterObjectPercentage, result.Alpha, ranking, result.TotalQueryTime, result.AvgQueryTime.Seconds(), result.QueriesPerSecond, result.QueryTimePer1000000Documents, result.Min.Seconds(), result.Max.Seconds(), result.P50.Seconds(), result.P90.Seconds(), result.P99.Seconds(), result.Scores.CurrentNDCG(), result.Scores.CurrentPrecisionAt1(), result.Scores.CurrentPrecisionAt5())
		}

		return nil
	},
}
