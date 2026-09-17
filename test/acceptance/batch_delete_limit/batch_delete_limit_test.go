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

package batch_delete_limit

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	className = "BatchDeleteLimit"
	// queryMaximumResults is what the container runs with. The deployed default is
	// 10000, which no acceptance test can cross in reasonable time.
	queryMaximumResults = 10
	objectCount         = 50
	// drainCallLimit stops a non-terminating drain from hanging the suite.
	drainCallLimit = 20
)

// TestBatchDeleteObjectsStopsAtTheLimit walks the bound over the public REST API: what
// one call deletes, what the reply says about the rest, and that repeating the call as
// the reply asks removes every matching object.
func TestBatchDeleteObjectsStopsAtTheLimit(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("QUERY_MAXIMUM_RESULTS", strconv.Itoa(queryMaximumResults)).
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "stringProp", DataType: schema.DataTypeText.PropString()},
		},
	})
	defer helper.DeleteClass(t, className)

	objs := make([]*models.Object, objectCount)
	for i := range objs {
		objs[i] = &models.Object{
			Class:      className,
			ID:         strfmt.UUID(fmt.Sprintf("8d5a3aa2-3c8d-4589-9ae1-3f638f506%03d", i)),
			Properties: map[string]interface{}{"stringProp": fmt.Sprintf("element %d", i)},
		}
	}
	helper.CreateObjectsBatch(t, objs)

	t.Run("a dry run reports the bound and deletes nothing", func(t *testing.T) {
		results := batchDeleteAll(t, true)

		require.Equal(t, int64(queryMaximumResults+1), results.Matches,
			"the count stops one above the limit")
		require.Len(t, results.Objects, queryMaximumResults)
		require.Equal(t, int64(queryMaximumResults), results.Limit)
		require.Equal(t, int64(0), results.Successful)
	})

	t.Run("repeating the call deletes every matching object", func(t *testing.T) {
		deleted := int64(0)
		calls := 0
		for ; calls < drainCallLimit; calls++ {
			results := batchDeleteAll(t, false)
			if results.Matches == 0 {
				break
			}
			require.NotEmpty(t, results.Objects, "a non-zero match count must delete something")
			require.LessOrEqual(t, results.Successful, int64(queryMaximumResults))
			require.Equal(t, int64(0), results.Failed)
			deleted += results.Successful
		}

		require.Less(t, calls, drainCallLimit, "the drain never reported zero matches")
		require.Equal(t, int64(objectCount), deleted)
	})
}

func batchDeleteAll(t *testing.T, dryRun bool) *models.BatchDeleteResponseResults {
	t.Helper()

	output := "verbose"
	all := "*"
	resp, err := helper.Client(t).Batch.BatchObjectsDelete(
		batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{
			DryRun: &dryRun,
			Output: &output,
			Match: &models.BatchDeleteMatch{
				Class: className,
				Where: &models.WhereFilter{
					Operator:  models.WhereFilterOperatorLike,
					Path:      []string{"id"},
					ValueText: &all,
				},
			},
		}),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, resp.Payload.Results)

	return resp.Payload.Results
}
