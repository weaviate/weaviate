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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// testPostDeleteNoSyntheticEntry pins that once a DELETE for a (property,
// indexType) is accepted, GET /indexes must never synthesize an
// "indexing@100%" finalize-window entry for it (mergeReindexStatus's
// FINISHED-task bridge would otherwise resurrect the just-deleted index for
// the whole finalize window).
//
// Stricter than testDeleteThenReEnableIndexingBleed (which only requires the
// bleed to eventually clear): here the entry must be absent at EVERY sample
// across the finalize window right after the DELETE.
func testPostDeleteNoSyntheticEntry(t *testing.T, restURI string) {
	const class = "PostDeleteNoBleed"
	trueVal, falseVal := true, false
	helper.CreateClass(t, &models.Class{
		Class: class,
		Properties: []*models.Property{
			{Name: "score", DataType: []string{"int"}, IndexFilterable: &trueVal, IndexRangeFilters: &falseVal},
		},
		Vectorizer: "none",
	})
	defer helper.DeleteClass(t, class)

	for _, s := range []int{10, 20, 30, 40, 50} {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			Class: class, Properties: map[string]interface{}{"score": s},
		}))
	}

	// Enable rangeFilters and wait for FINISHED. The FINISHED task lingers in
	// DTM for the finalize window — the source of the bleed.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "score", "rangeFilters", `{}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, taskID)

	// DELETE it, then assert the rangeFilters entry is absent at every sample
	// across the finalize window (2× scheduler tick, clamped ≥3s).
	deleteIndex(t, restURI, class, "score", "rangeFilters")

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		resp := reindexhelpers.GetIndexes(t, restURI, class)
		for _, prop := range resp.Properties {
			if prop.Name != "score" {
				continue
			}
			for _, idx := range prop.Indexes {
				require.NotEqualf(t, "rangeFilters", idx.Type,
					"after DELETE, GET /indexes must not synthesize a finalize-window entry for the deleted rangeFilters index; got %+v", idx)
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// TestSuppress_PostDeleteNoSyntheticEntry keeps this file compiling in
// isolation; the entry point is the suite subtest.
func TestSuppress_PostDeleteNoSyntheticEntry(t *testing.T) {
	assert.NotNil(t, testPostDeleteNoSyntheticEntry)
}
