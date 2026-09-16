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

package compaction_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/compactions"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const (
	categoryCollection = "RoaringSetRestart"
	objectsPerCategory = 250
)

var categories = []string{"alpha", "beta", "gamma"}

// categoryBucket is the roaring set bucket the filterable property writes,
// named the way helpers.BucketFromPropNameLSM names it.
const categoryBucket = "property_category"

// importCategories writes objectsPerCategory objects per category, so a filter
// on any one of them has a count the test can assert exactly.
func importCategories(t *testing.T) {
	t.Helper()

	objects := make([]*models.Object, 0, len(categories)*objectsPerCategory)
	for _, category := range categories {
		for i := 0; i < objectsPerCategory; i++ {
			objects = append(objects, &models.Object{
				Class:      categoryCollection,
				ID:         strfmt.UUID(uuid.New().String()),
				Properties: map[string]interface{}{"category": category},
			})
		}
	}

	helper.CreateObjectsBatch(t, objects)
}

// countByCategory runs the filtered query the roaring set index answers.
func countByCategory(t *testing.T, category string) int {
	t.Helper()

	query := fmt.Sprintf(`
		{
			Aggregate {
				%s(where: {path: ["category"], operator: Equal, valueText: %q}) {
					meta { count }
				}
			}
		}`, categoryCollection, category)

	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	agg := result.Get("Aggregate", categoryCollection).AsSlice()[0].(map[string]interface{})
	meta := agg["meta"].(map[string]interface{})
	count, err := meta["count"].(json.Number).Int64()
	require.NoError(t, err)
	return int(count)
}

func requireCategoryCounts(t *testing.T, want int, msg string) {
	t.Helper()
	for _, category := range categories {
		require.Equal(t, want, countByCategory(t, category), msg)
	}
}

// startNodeWithCategoryClass brings up a node whose memtables flush on a short
// timer and creates the collection under test on it.
func startNodeWithCategoryClass(t *testing.T, ctx context.Context) *docker.DockerCompose {
	t.Helper()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "2").
		Start(ctx)
	require.NoError(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())

	helper.CreateClass(t, &models.Class{
		Class:      categoryCollection,
		Vectorizer: "none",
		Properties: []*models.Property{{
			Name:     "category",
			DataType: []string{"text"},
			// Filterable is the roaring set index; searchable is the map index and
			// would not exercise this flush path.
			IndexFilterable: ptrTo(true),
			IndexSearchable: ptrTo(false),
			Tokenization:    models.PropertyTokenizationField,
		}},
	})

	return compose
}

func ptrTo[T any](v T) *T { return &v }

// TestRoaringSetSurvivesRestart reads a filterable index back after the node
// restarts, so the segments the flush wrote are parsed by a fresh process
// rather than answered from the memtable that produced them.
//
// Each row imports twice: once before the segment poll, so a flushed segment
// exists, and once after, so a memtable holding unflushed data exists at the
// moment the node stops. That second batch is what the two stop modes then
// route differently — a nil timeout lets the container's shutdown handler run
// and flush it, while a zero timeout kills the process and leaves it to be
// replayed from the commit log.
func TestRoaringSetSurvivesRestart(t *testing.T) {
	zero := time.Duration(0)

	tests := []struct {
		name        string
		stopTimeout *time.Duration
	}{
		{name: "shutdown flushes the memtable", stopTimeout: nil},
		{name: "kill leaves the commit log to be replayed", stopTimeout: &zero},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			compose := startNodeWithCategoryClass(t, ctx)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()
			defer helper.ResetClient()

			container := compose.GetWeaviate().Container()
			shard := getShardNameForClass(t, categoryCollection)

			importCategories(t)
			requireCategoryCounts(t, objectsPerCategory,
				"the filter must be right before the restart, or the restart proves nothing")

			// Poll rather than sleep: this is what makes "a segment was written by
			// the dirty-flush cycle" a fact the test checked rather than a timing
			// assumption.
			require.Eventually(t, func() bool {
				return compactions.TotalSegmentFileCount(ctx, container,
					categoryCollection, shard, categoryBucket) >= 1
			}, 60*time.Second, time.Second,
				"no segment appeared in %s, so the flush under test never ran", categoryBucket)

			// A level above 0 is a segment some compaction produced, so the data
			// read back after the restart has been through both writers rather
			// than the flush alone. Compaction needs two segments to start, which
			// is why the import below runs before the wait rather than after.
			importCategories(t)
			require.Eventually(t, func() bool {
				maxLevel, _ := compactions.MaxLevelAndCount(ctx, container,
					categoryCollection, shard, categoryBucket)
				return maxLevel >= 1
			}, 120*time.Second, 2*time.Second,
				"no compacted segment appeared in %s, so the restart reads flushed segments only",
				categoryBucket)

			// Unflushed at stop time, so the stop mode decides which writer
			// persists it. Asserted rather than timed: if the dirty cycle had
			// flushed this batch too, both rows would exercise the same writer and
			// the table would prove one thing twice.
			segments := compactions.TotalSegmentFileCount(ctx, container,
				categoryCollection, shard, categoryBucket)
			importCategories(t)
			require.Equal(t, segments, compactions.TotalSegmentFileCount(ctx, container,
				categoryCollection, shard, categoryBucket),
				"the second batch reached a segment before the stop, so the stop mode decides nothing")

			require.NoError(t, compose.StopAt(ctx, 0, tt.stopTimeout))
			require.NoError(t, compose.StartAt(ctx, 0))
			helper.SetupClient(compose.GetWeaviate().URI())

			requireCategoryCounts(t, 3*objectsPerCategory,
				"the filterable index does not answer with the objects it held before the restart")
		})
	}
}
