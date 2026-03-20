//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package reindex_singlenode

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

const retokenizeClassName = "RetokenizeTest"

var retokenizeObjects = []map[string]interface{}{
	{"filepath": "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go", "description": "primary application source code"},
	{"filepath": "/code/github.com/weaviate/weaviate/README.md", "description": "documentation readme file"},
	{"filepath": "/code/github.com/weaviate/weaviate/main.go", "description": "entry point for the server"},
	{"filepath": "/code/github.com/other/project/main.go", "description": "alternative project launcher"},
	{"filepath": "/code/github.com/other/project/README.md", "description": "alternative project documentation"},
	{"filepath": "/code/docs/tutorial/getting_started.md", "description": "beginner tutorial guide"},
	{"filepath": "/code/docs/tutorial/advanced.md", "description": "expert level tutorial"},
	{"filepath": "/home/user/documents/report.pdf", "description": "quarterly financial report"},
	{"filepath": "/home/user/documents/notes.txt", "description": "personal meeting notes"},
	{"filepath": "/var/log/system.log", "description": "operating system log file"},
}

type retokenizeBaseline struct {
	name string
	ids  []string
}

var retokenizeBaselines []retokenizeBaseline
var filterEqualDescBaseline []string

func testChangeTokenization(t *testing.T, restURI string) {
	class := &models.Class{
		Class: retokenizeClassName,
		Properties: []*models.Property{
			{Name: "filepath", DataType: []string{"text"}, Tokenization: "word"},
			{Name: "description", DataType: []string{"text"}, Tokenization: "word"},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	for i, props := range retokenizeObjects {
		obj := &models.Object{Class: retokenizeClassName, Properties: props}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	type bm25Query struct {
		name, property, query string
	}
	bm25Queries := []bm25Query{
		{"full_path_search", "filepath", "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go"},
		{"partial_token_weaviate", "filepath", "weaviate"},
		{"description_search", "description", "tutorial"},
	}

	retokenizeBaselines = make([]retokenizeBaseline, len(bm25Queries))
	for i, bq := range bm25Queries {
		ids := retokenizeBM25Query(t, bq.property, bq.query)
		retokenizeBaselines[i] = retokenizeBaseline{name: bq.name, ids: ids}
	}

	require.Greater(t, len(retokenizeBaselines[0].ids), 1, "full_path with WORD should match multiple")
	require.Greater(t, len(retokenizeBaselines[1].ids), 0, "partial_token should match")

	filterEqualDescBaseline = retokenizeFilterQuery(t, "description", "Equal", "tutorial")
	require.Greater(t, len(filterEqualDescBaseline), 0)

	var queryFailures atomic.Int64
	var queryRuns atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			for i, bl := range retokenizeBaselines {
				bq := bm25Queries[i]
				ids, err := retokenizeBM25QuerySafe(t, bq.property, bq.query)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
					continue
				}
				if bq.property == "description" && !idsMatchUnordered(bl.ids, ids) {
					queryFailures.Add(1)
				}
			}
			ids, err := retokenizeFilterQuerySafe(t, "description", "Equal", "tutorial")
			queryRuns.Add(1)
			if err != nil {
				queryFailures.Add(1)
			} else if !idsMatchUnordered(filterEqualDescBaseline, ids) {
				queryFailures.Add(1)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	taskID := submitIndexUpdate(t, restURI, retokenizeClassName, "filepath", `{"searchable":{"tokenization":"field"}}`)
	t.Logf("submitted reindex task: %s", taskID)

	awaitReindexViaIndexes(t, restURI, retokenizeClassName, "filepath", "searchable")
	awaitReindexFinished(t, restURI, taskID)

	require.Eventually(t, func() bool {
		cls := helper.GetClass(t, retokenizeClassName)
		for _, prop := range cls.Properties {
			if prop.Name == "filepath" {
				return prop.Tokenization == "field"
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "tokenization should change to field")

	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load())

	postFullPath := retokenizeBM25Query(t, "filepath", "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postFullPath, 1, "full_path with FIELD should match one")

	postPartial := retokenizeBM25Query(t, "filepath", "weaviate")
	assert.Empty(t, postPartial, "partial_token with FIELD should match none")

	postDesc := retokenizeBM25Query(t, "description", "tutorial")
	assert.ElementsMatch(t, retokenizeBaselines[2].ids, postDesc)

	postFilterWeaviate := retokenizeFilterQuery(t, "filepath", "Equal", "weaviate")
	assert.Empty(t, postFilterWeaviate)

	postFilterFull := retokenizeFilterQuery(t, "filepath", "Equal",
		"/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postFilterFull, 1)

	postFilterLike := retokenizeFilterQuery(t, "filepath", "Like", "weaviate*")
	assert.Empty(t, postFilterLike)

	updatedClass := helper.GetClass(t, retokenizeClassName)
	for _, prop := range updatedClass.Properties {
		switch prop.Name {
		case "filepath":
			assert.Equal(t, "field", prop.Tokenization)
		case "description":
			assert.Equal(t, "word", prop.Tokenization)
		}
	}
}

func testChangeTokenizationPostRestart(t *testing.T) {
	postFullPath := retokenizeBM25Query(t, "filepath", "/code/github.com/weaviate/weaviate/all_the_awesome_stuff.go")
	assert.Len(t, postFullPath, 1, "post-restart: full_path with FIELD should match one")

	postPartial := retokenizeBM25Query(t, "filepath", "weaviate")
	assert.Empty(t, postPartial, "post-restart: partial_token should match none")

	postDesc := retokenizeBM25Query(t, "description", "tutorial")
	assert.ElementsMatch(t, retokenizeBaselines[2].ids, postDesc)

	postFilterEqual := retokenizeFilterQuery(t, "filepath", "Equal", "weaviate")
	assert.Empty(t, postFilterEqual)

	postFilterLike := retokenizeFilterQuery(t, "filepath", "Like", "weaviate*")
	assert.Empty(t, postFilterLike)

	restartClass := helper.GetClass(t, retokenizeClassName)
	for _, prop := range restartClass.Properties {
		if prop.Name == "filepath" {
			assert.Equal(t, "field", prop.Tokenization, "post-restart: should be field")
		}
	}
}

func retokenizeBM25Query(t *testing.T, property, query string) []string {
	t.Helper()
	ids, err := retokenizeBM25QuerySafe(t, property, query)
	require.NoError(t, err)
	return ids
}

func retokenizeBM25QuerySafe(t *testing.T, property, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: [%q]}) {
				filepath
				description
				_additional { id }
			}
		}
	}`, retokenizeClassName, query, property)
	return runGraphQLQuery(t, retokenizeClassName, gqlQuery)
}

func retokenizeFilterQuery(t *testing.T, property, operator, value string) []string {
	t.Helper()
	ids, err := retokenizeFilterQuerySafe(t, property, operator, value)
	require.NoError(t, err)
	return ids
}

func retokenizeFilterQuerySafe(t *testing.T, property, operator, value string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(where: {operator: %s, path: [%q], valueText: %q}) {
				filepath
				description
				_additional { id }
			}
		}
	}`, retokenizeClassName, operator, property, value)
	return runGraphQLQuery(t, retokenizeClassName, gqlQuery)
}
