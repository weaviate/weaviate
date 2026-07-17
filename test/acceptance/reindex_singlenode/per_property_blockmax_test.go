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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testPerPropertyBlockmaxNoOp pins per-property blockmax truth on a
// multi-searchable-property class: NO_OP, rebuild eligibility, and status
// algorithm must reflect each property's own migrated bucket, not the
// class-wide flag (which stays deferred until EVERY searchable property has
// migrated; see shouldDeferBlockmaxFlip).
func testPerPropertyBlockmaxNoOp(t *testing.T, restURI string) {
	const class = "PerPropBlockmax"
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}, Tokenization: "word"},
			{Name: "body", DataType: []string{"text"}, Tokenization: "word"},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
		Vectorizer:          "none",
	})
	defer helper.DeleteClass(t, class)

	// Both searchable props start on WAND (USE_INVERTED_SEARCHABLE=false).
	require.False(t, helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND)

	for i := 0; i < 25; i++ {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class,
			Properties: map[string]interface{}{
				"title": fmt.Sprintf("alpha bravo charlie %d", i),
				"body":  fmt.Sprintf("delta echo foxtrot %d", i),
			},
		}), "object %d", i)
	}

	// Pre-migration: both properties report algorithm=wand.
	assertSearchableAlgorithm(t, restURI, class, "title", "wand", "")
	assertSearchableAlgorithm(t, restURI, class, "body", "wand", "")

	// Migrate ONLY title to blockmax.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, class, "title", "searchable")

	// The class-wide flip stays deferred while body is still WAND.
	require.False(t, helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND,
		"class flip must stay deferred while body is still WAND")

	// Status honesty: title reports blockmax (its bucket migrated), body wand.
	assertSearchableAlgorithm(t, restURI, class, "title", "blockmax", "")
	assertSearchableAlgorithm(t, restURI, class, "body", "wand", "")

	// Core: repeat blockmax PUT on the already-migrated title → 200 NO_OP.
	resp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	require.Equalf(t, http.StatusOK, resp.StatusCode,
		"repeat blockmax PUT on an already-migrated property must be 200 NO_OP, got: %s", resp.Body)
	require.Contains(t, resp.Body, "NO_OP")

	// Rebuild on the already-blockmax title must be accepted (202), not 400'd
	// with "cannot rebuild a WAND searchable index".
	rebuildTask := reindexhelpers.RebuildIndex(t, restURI, class, "title", "searchable")
	reindexhelpers.AwaitReindexFinished(t, restURI, rebuildTask)

	// Migrate body too → the class-wide flip now completes.
	bodyTask := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "body", "searchable", `{"algorithm":"blockmax"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, bodyTask)
	reindexhelpers.AwaitReindexViaIndexes(t, restURI, class, "body", "searchable")

	require.Eventually(t, func() bool {
		return helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND
	}, 15*time.Second, 250*time.Millisecond,
		"class flip must complete once every searchable property is blockmax")

	assertSearchableAlgorithm(t, restURI, class, "title", "blockmax", "")
	assertSearchableAlgorithm(t, restURI, class, "body", "blockmax", "")

	// With the class flag now on, a repeat blockmax PUT on either prop is
	// also 200 NO_OP (the class-flag fast path).
	noopResp := reindexhelpers.SubmitIndexUpsertRaw(t, restURI, class, "body", "searchable", `{"algorithm":"blockmax"}`)
	assert.Equal(t, http.StatusOK, noopResp.StatusCode, "post-flip repeat blockmax PUT must be NO_OP")
}

// TestSuppress_PerPropertyBlockmaxNoOp keeps this file compiling in isolation;
// the entry point is the suite subtest.
func TestSuppress_PerPropertyBlockmaxNoOp(t *testing.T) {
	assert.NotNil(t, testPerPropertyBlockmaxNoOp)
}
