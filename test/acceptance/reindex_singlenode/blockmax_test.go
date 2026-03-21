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

const blockmaxClassName = "ReindexTest"

var blockmaxDocuments = []string{
	"alpha bravo charlie delta echo foxtrot",
	"golf hotel india juliet kilo lima",
	"mike november oscar papa quebec romeo",
	"sierra tango uniform victor whiskey xray",
	"yankee zulu alpha bravo charlie delta",
	"echo foxtrot golf hotel india juliet",
	"kilo lima mike november oscar papa",
	"quebec romeo sierra tango uniform victor",
	"whiskey xray yankee zulu alpha bravo",
	"charlie delta echo foxtrot golf hotel",
	"india juliet kilo lima mike november",
	"oscar papa quebec romeo sierra tango",
	"uniform victor whiskey xray yankee zulu",
	"alpha charlie echo golf india kilo",
	"mike oscar quebec sierra uniform whiskey",
	"yankee bravo delta foxtrot hotel juliet",
	"lima november papa romeo tango victor",
	"xray zulu alpha echo india mike",
	"oscar sierra uniform yankee charlie foxtrot",
	"hotel kilo november quebec romeo victor",
	"alpha alpha alpha bravo bravo charlie",
	"delta delta delta echo echo foxtrot",
	"golf golf golf hotel hotel india",
	"juliet juliet juliet kilo kilo lima",
	"mike mike mike november november oscar",
}

var blockmaxBM25Queries = []string{
	"alpha",
	"bravo charlie",
	"echo foxtrot golf",
	"mike november oscar",
}

type blockmaxBaseline struct {
	query string
	ids   []string
}

var blockmaxBaselines []blockmaxBaseline

func testBlockmaxMigration(t *testing.T, restURI string) {
	class := &models.Class{
		Class: blockmaxClassName,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			Bm25: &models.BM25Config{K1: 1.2, B: 0.75},
		},
		Vectorizer: "none",
	}
	helper.CreateClass(t, class)

	createdClass := helper.GetClass(t, blockmaxClassName)
	require.False(t, createdClass.InvertedIndexConfig.UsingBlockMaxWAND)

	for i, text := range blockmaxDocuments {
		obj := &models.Object{
			Class:      blockmaxClassName,
			Properties: map[string]interface{}{"text": text},
		}
		require.NoError(t, helper.CreateObject(t, obj), "failed to create object %d", i)
	}

	blockmaxBaselines = make([]blockmaxBaseline, len(blockmaxBM25Queries))
	for i, q := range blockmaxBM25Queries {
		ids := blockmaxBM25Query(t, q)
		require.NotEmpty(t, ids, "baseline query %q returned no results", q)
		blockmaxBaselines[i] = blockmaxBaseline{query: q, ids: ids}
	}

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
			for _, bl := range blockmaxBaselines {
				ids, err := blockmaxBM25QuerySafe(t, bl.query)
				queryRuns.Add(1)
				if err != nil {
					queryFailures.Add(1)
				} else if !idsMatchUnordered(bl.ids, ids) {
					queryFailures.Add(1)
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	taskID := submitIndexUpdate(t, restURI, blockmaxClassName, "text", `{"searchable":{"rebuild":true}}`)
	t.Logf("submitted reindex task: %s", taskID)

	awaitReindexViaIndexes(t, restURI, blockmaxClassName, "text", "searchable")
	awaitReindexFinished(t, restURI, taskID)

	close(stopCh)
	wg.Wait()

	t.Logf("background queries: %d runs, %d failures", queryRuns.Load(), queryFailures.Load())
	assert.Zero(t, queryFailures.Load(), "BM25 queries failed during migration")

	updatedClass := helper.GetClass(t, blockmaxClassName)
	require.True(t, updatedClass.InvertedIndexConfig.UsingBlockMaxWAND)

	for _, bl := range blockmaxBaselines {
		ids := blockmaxBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-migration query %q results differ", bl.query)
	}
}

func testBlockmaxPostRestart(t *testing.T) {
	for _, bl := range blockmaxBaselines {
		ids := blockmaxBM25Query(t, bl.query)
		assert.ElementsMatch(t, bl.ids, ids,
			"post-restart query %q results differ", bl.query)
	}
}

func blockmaxBM25Query(t *testing.T, query string) []string {
	t.Helper()
	ids, err := blockmaxBM25QuerySafe(t, query)
	require.NoError(t, err, "BM25 query %q failed", query)
	return ids
}

func blockmaxBM25QuerySafe(t *testing.T, query string) ([]string, error) {
	t.Helper()
	gqlQuery := fmt.Sprintf(`{
		Get {
			%s(bm25: {query: %q, properties: ["text"]}) {
				text
				_additional { id }
			}
		}
	}`, blockmaxClassName, query)
	return runGraphQLQuery(t, blockmaxClassName, gqlQuery)
}
