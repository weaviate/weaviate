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

package reindex_blockmax_ageout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

const stoppedRunLargeObjects = 100_000

// A stopped change-algorithm run on a class with the blockmax flag off leaves
// the small tenant on blockmax and the large one on WAND. After a restart, the
// resubmit the refusals name must migrate the large tenant.
func TestBlockmaxStoppedChangeAlgorithmResubmitAfterRestart(t *testing.T) {
	ctx := context.Background()
	compose, err := reindexhelpers.SingleNodeCompose().Start(ctx)
	require.NoError(t, err)
	defer func() { _ = compose.Terminate(ctx) }()
	defer func() {
		if t.Failed() {
			dumpLogs(ctx, t, compose.GetWeaviate().Container())
		}
	}()
	restURI := compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	const class = "StoppedChangeAlgorithm"
	bm25Class(t, class, true)
	require.False(t, helper.GetClass(t, class).InvertedIndexConfig.UsingBlockMaxWAND)
	helper.CreateTenants(t, class, []*models.Tenant{
		{Name: "small", ActivityStatus: models.TenantActivityStatusHOT},
		{Name: "large", ActivityStatus: models.TenantActivityStatusHOT},
	})
	for _, tenant := range []string{"small", "large"} {
		createDoc(t, class, shortTitle, tenant)
		createDoc(t, class, longTitle, tenant)
	}
	batchImportLarge(t, restURI, class)

	reindexhelpers.SubmitIndexUpsert(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	require.Eventually(t, func() bool { return bananaScoresEqual(t, class, "small") },
		2*time.Minute, 100*time.Millisecond, "the small tenant must swap to blockmax")
	cancelled := reindexhelpers.CancelIndex(t, restURI, class, "title", "searchable")
	require.Equal(t, "CANCELLED", cancelled.Status, "the large tenant must still be running at cancel")
	require.False(t, bananaScoresEqual(t, class, "large"), "the large tenant stays on WAND after the cancel")

	require.NoError(t, compose.StopAt(ctx, 0, nil))
	require.NoError(t, compose.StartAt(ctx, 0))
	restURI = compose.GetWeaviate().URI()
	helper.SetupClient(restURI)

	retry := reindexhelpers.SubmitIndexUpsert(t, restURI, class, "title", "searchable", `{"algorithm":"blockmax"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, retry, reindexhelpers.WithTimeout(5*time.Minute))
	assertEqualBananaScores(t, class, "large", "the large tenant after the resubmit")
	assertEqualBananaScores(t, class, "small", "the small tenant after the resubmit")
	require.Equal(t, "blockmax", searchableAlgorithm(t, restURI, class, "title"))
}

func bananaScoresEqual(t *testing.T, class, tenant string) bool {
	scores := bananaScores(t, class, tenant)
	short, long := scores[shortTitle], scores[longTitle]
	return short > 0 && long > 0 && short-long < 1e-4 && long-short < 1e-4
}

func batchImportLarge(t *testing.T, restURI, class string) {
	t.Helper()
	const batchSize = 1000
	for start := 0; start < stoppedRunLargeObjects; start += batchSize {
		objects := make([]map[string]any, 0, batchSize)
		for i := start; i < start+batchSize; i++ {
			objects = append(objects, map[string]any{
				"class": class, "tenant": "large",
				"properties": map[string]any{"title": fmt.Sprintf("bulk %d", i), "body": "filler"},
			})
		}
		raw, err := json.Marshal(map[string]any{"objects": objects})
		require.NoError(t, err)
		resp, err := http.Post(fmt.Sprintf("http://%s/v1/batch/objects", restURI), "application/json", bytes.NewReader(raw))
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "batch import: %s", string(respBody))
	}
}
