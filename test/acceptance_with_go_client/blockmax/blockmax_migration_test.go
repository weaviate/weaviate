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

package blockmax

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
)

var (
	migrationDataPath   = filepath.Join(".temp", "blockmax_migration")
	resultsMapPath      = filepath.Join(migrationDataPath, "results_map")
	resultsBlockmaxPath = filepath.Join(migrationDataPath, "results_blockmax")
	queriesPath         = filepath.Join(migrationDataPath, "queries.json")

	className       = "Movies"
	searchableProps = []string{
		"cast", "director", "genres", "keywords", "originalLanguage",
		"runtime", "runtime", "status", "tagline", "title",
	}
)

const (
	perPropLimit = 32
)

func Test_SearchDocsResultsMap(t *testing.T) {
	err := os.RemoveAll(resultsMapPath)
	require.NoError(t, err)
	err = os.MkdirAll(resultsMapPath, 0o777)
	require.NoError(t, err)

	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	fields := make([]graphql.Field, len(searchableProps))
	for i := range searchableProps {
		fields[i] = graphql.Field{Name: searchableProps[i]}
	}
	resp, err := client.GraphQL().Get().WithClassName(className).WithFields(fields...).Do(context.Background())
	require.NoError(t, err)

	extractors := map[string]*propExtractor{}
	for _, prop := range searchableProps {
		extractors[prop] = newPropExtractor(prop)
	}

	docs := resp.Data["Get"].(map[string]any)[className].([]any)
	for i := range docs {
		doc := docs[i].(map[string]any)

		for prop := range extractors {
			extractors[prop].extract(doc)
		}
	}

	queriesByProp := map[string][]string{}
	for prop := range extractors {
		queriesByProp[prop] = extractors[prop].vals()
	}

	binQueriesByProp, err := json.MarshalIndent(queriesByProp, "", "\t")
	require.NoError(t, err)

	file, err := os.OpenFile(queriesPath, os.O_WRONLY|os.O_CREATE, 0o777)
	require.NoError(t, err)
	_, err = file.Write(binQueriesByProp)
	require.NoError(t, err)
	file.Close()

	for prop := range extractors {
		runQueriesAndSaveResults(t, client, prop, queriesByProp[prop], resultsMapPath)
	}
}

func Test_SearchDocsResultsBlockmax(t *testing.T) {
	err := os.RemoveAll(resultsBlockmaxPath)
	require.NoError(t, err)
	err = os.MkdirAll(resultsBlockmaxPath, 0o777)
	require.NoError(t, err)

	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	binQueriesByProp, err := os.ReadFile(queriesPath)
	require.NoError(t, err)
	queriesByProp := map[string][]string{}
	err = json.Unmarshal(binQueriesByProp, &queriesByProp)
	require.NoError(t, err)

	blockmaxResults := make(map[string][][]any, len(queriesByProp))
	for prop := range queriesByProp {
		blockmaxResults[prop] = make([][]any, len(queriesByProp[prop]))
		for i, queryResult := range runQueriesAndSaveResults(t, client, prop, queriesByProp[prop], resultsBlockmaxPath) {
			blockmaxResults[prop][i] = queryResult
		}
	}

	mapResults := make(map[string][][]any, len(queriesByProp))
	for prop := range queriesByProp {
		mapResults[prop] = make([][]any, len(queriesByProp[prop]))
		for i := range queriesByProp[prop] {
			queryResultPath := filepath.Join(resultsMapPath, fmt.Sprintf("%s_%02d.json", prop, i+1))
			binQueryResult, err := os.ReadFile(queryResultPath)
			require.NoError(t, err)
			queryResult := []any{}
			err = json.Unmarshal(binQueryResult, &queryResult)
			require.NoError(t, err)

			mapResults[prop][i] = queryResult
		}
	}

	for prop := range mapResults {
		for i := range mapResults[prop] {
			// fmt.Printf("prop %s i %d\n\n", prop, i)
			// fmt.Printf("map\n%+v\n\n", mapResults[prop][i])
			// fmt.Printf("blockmax\n%+v\n\n", mapResults[prop][i])
			assert.ElementsMatch(t, mapResults[prop][i], blockmaxResults[prop][i])
		}
	}
}

func runQueriesAndSaveResults(t *testing.T, client *wvt.Client,
	prop string, queries []string, resultsPath string,
) [][]any {
	fields := []graphql.Field{
		{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
		{Name: prop},
	}
	results := make([][]any, len(queries))

	for i := range queries {
		resp, err := client.GraphQL().Get().
			WithBM25(client.GraphQL().Bm25ArgBuilder().WithQuery(queries[i]).WithProperties(prop)).
			WithFields(fields...).
			WithClassName(className).
			Do(context.Background())
		require.NoError(t, err)
		require.Empty(t, resp.Errors)

		queryResult := resp.Data["Get"].(map[string]any)[className].([]any)
		binQueryResult, err := json.MarshalIndent(queryResult, "", "\t")
		require.NoError(t, err)

		queryResultPath := filepath.Join(resultsPath, fmt.Sprintf("%s_%02d.json", prop, i+1))
		file, err := os.OpenFile(queryResultPath, os.O_WRONLY|os.O_CREATE, 0o777)
		require.NoError(t, err)
		_, err = file.Write(binQueryResult)
		require.NoError(t, err)
		file.Close()

		results[i] = queryResult
	}
	return results
}

type propExtractor struct {
	valsMap map[string]struct{}
	prop    string
}

func newPropExtractor(prop string) *propExtractor {
	return &propExtractor{
		valsMap: map[string]struct{}{},
		prop:    prop,
	}
}

func (e *propExtractor) extract(doc map[string]any) {
	if len(e.valsMap) >= perPropLimit {
		return
	}

	val := doc[e.prop].(string)
	e.valsMap[val] = struct{}{}
}

func (e *propExtractor) vals() []string {
	arr := make([]string, len(e.valsMap))
	i := 0
	for k := range e.valsMap {
		arr[i] = k
		i++
	}
	return arr
}
