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

package db

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The swap window is the gap between a semantic migration's per-shard bucket
// flip and the cluster-wide schema flip that OnTaskCompleted commits once
// every node has acknowledged. runtimeSwap tears the double-write mirror down
// on the way out, and the ordinary write path gates on the schema flag, which
// is still off — so a write accepted in the window is stored, indexed nowhere,
// and never healed (weaviate/etienne-claude-issues#449). Shards flip one at a
// time and the schema flip waits for the last one, so the first shard to flip
// sits in the window for the whole swap phase.

const semanticWindowSeedObjects = 25

// semanticWindowToken is absent from makeConvergenceTestObjects' dictionary, so a
// hit on it can only come from the object written inside the window.
const semanticWindowToken = "zulu"

// enterSemanticSwapWindow seeds a shard, runs the migration through its bucket flip,
// and stops before the schema flip — the state the window's writes land in.
func enterSemanticSwapWindow(t *testing.T, ctx context.Context, class *models.Class, propName string,
	migration ReindexMigrationType,
	newTask func(*testing.T, *Index, string, string) *ShardReindexTaskGeneric,
) *Shard {
	t.Helper()

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		true, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	for _, obj := range makeConvergenceTestObjects(t, semanticWindowSeedObjects, class.Class) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task := newTask(t, idx, class.Class, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))

	// The same two steps runShardSwapPhase runs, in the same order.
	maybeWirePerPropOverlaySet(shard, &ReindexTaskPayload{
		MigrationType: migration,
		Collection:    class.Class,
		Properties:    []string{propName},
	}, []*ShardReindexTaskGeneric{task})
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	return shard
}

// noIndexAtAllClass gives the property neither a filterable nor a searchable
// index, so shard init creates no buckets for it at all — not even the length
// and null ones the class-level settings ask for.
func noIndexAtAllClass(className, propName string) *models.Class {
	class := newTestClassWithProps(className, []string{propName})
	class.Properties[0].IndexFilterable = boolPtr(false)
	class.Properties[0].IndexSearchable = boolPtr(false)
	return class
}

func TestWriteDuringSemanticMigrationSwapWindow(t *testing.T) {
	const propName = "title"

	tests := []struct {
		name      string
		newClass  func(className, propName string) *models.Class
		migration ReindexMigrationType
		newTask   func(*testing.T, *Index, string, string) *ShardReindexTaskGeneric
		flip      func(prop *models.Property)
		find      func(*testing.T, context.Context, *Shard, string, string) []*storobj.Object
	}{
		{
			name:      "enable-filterable",
			newClass:  newEnableFilterableTestClass,
			migration: ReindexTypeEnableFilterable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, propName)
				return task
			},
			flip: func(prop *models.Property) { prop.IndexFilterable = boolPtr(true) },
			find: findByEqualFilter,
		},
		{
			name:      "enable-filterable on a property with no inverted index at all",
			newClass:  noIndexAtAllClass,
			migration: ReindexTypeEnableFilterable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, propName)
				return task
			},
			flip: func(prop *models.Property) { prop.IndexFilterable = boolPtr(true) },
			find: findByEqualFilter,
		},
		{
			name: "enable-searchable",
			newClass: func(className, propName string) *models.Class {
				return newEnableSearchableTestClass(className, []string{propName})
			},
			migration: ReindexTypeEnableSearchable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, propName,
					models.PropertyTokenizationWord)
				return task
			},
			flip: func(prop *models.Property) { prop.IndexSearchable = boolPtr(true) },
			find: findByBM25,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SemanticWindow_" + uuid.NewString()[:8]
			class := tc.newClass(className, propName)
			shard := enterSemanticSwapWindow(t, ctx, class, propName, tc.migration, tc.newTask)

			windowObj := createTestObjectWithText(className, semanticWindowToken)
			require.NoError(t, shard.PutObject(ctx, windowObj),
				"a write in the swap window must be accepted")

			// The cluster-wide flip lands, which is when the migrated index
			// starts serving queries.
			tc.flip(class.Properties[0])

			found := tc.find(t, ctx, shard, className, propName)
			require.Equal(t, []string{windowObj.ID().String()}, objectIDs(found),
				"the object written during the swap window is missing from the migrated index — "+
					"the write was accepted, stored, and indexed nowhere")
		})
	}
}

// The length and null buckets belong to a property the live schema indexes.
// Forcing an index flag on at write time makes the analyzer emit a property
// that has neither, and both the add and the delete leg would then fail on the
// missing bucket — turning silent loss into a loud rejection of every write to
// the collection.
func TestWriteDuringSwapWindowSkipsAbsentLengthAndNullBuckets(t *testing.T) {
	const propName = "title"

	ctx := testCtx()
	className := "SemanticWindowBuckets_" + uuid.NewString()[:8]
	class := noIndexAtAllClass(className, propName)
	require.True(t, class.InvertedIndexConfig.IndexPropertyLength)
	require.True(t, class.InvertedIndexConfig.IndexNullState)

	shard := enterSemanticSwapWindow(t, ctx, class, propName, ReindexTypeEnableFilterable,
		func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
			task, _ := newEnableFilterableTask(t, idx, className, propName)
			return task
		})

	require.Nil(t, shard.store.Bucket(helpers.BucketFromPropNameLengthLSM(propName)),
		"precondition: the property-length bucket must be absent, or this test proves nothing")
	require.Nil(t, shard.store.Bucket(helpers.BucketFromPropNameNullLSM(propName)),
		"precondition: the null-state bucket must be absent, or this test proves nothing")

	obj := createTestObjectWithText(className, semanticWindowToken)
	require.NoError(t, shard.PutObject(ctx, obj),
		"the add leg must not reach the absent length and null buckets")

	// Overwriting the same id runs the previous version through the delete
	// leg, which reaches for the same two buckets.
	overwrite := createTestObjectWithText(className, "yankee")
	overwrite.Object.ID = obj.Object.ID
	require.NoError(t, shard.PutObject(ctx, overwrite),
		"the delete leg must not reach the absent length and null buckets")

	class.Properties[0].IndexFilterable = boolPtr(true)
	require.Empty(t, objectIDs(findByEqualFilter(t, ctx, shard, className, propName)),
		"the overwritten value must be gone from the migrated index")
}

func findByEqualFilter(t *testing.T, ctx context.Context, shard *Shard,
	className, propName string,
) []*storobj.Object {
	t.Helper()
	found, _, err := shard.ObjectSearch(ctx, semanticWindowSeedObjects,
		propEqualsFilter(className, propName, semanticWindowToken), nil, nil, nil,
		additional.Properties{}, nil)
	require.NoError(t, err)
	return found
}

func findByBM25(t *testing.T, ctx context.Context, shard *Shard,
	_, propName string,
) []*storobj.Object {
	t.Helper()
	found, _, err := shard.ObjectSearch(ctx, semanticWindowSeedObjects, nil,
		&searchparams.KeywordRanking{
			Type:       "bm25",
			Properties: []string{propName},
			Query:      semanticWindowToken,
		}, nil, nil, additional.Properties{}, []string{propName})
	require.NoError(t, err)
	return found
}
