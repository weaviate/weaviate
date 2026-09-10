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
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// The swap window is the gap between a semantic migration's per-shard bucket
// flip and the cluster-wide schema flip. Once the double-write mirror is torn
// down, the write path gates on the still-off schema flag, so a write in the
// window is stored but indexed nowhere (weaviate/etienne-claude-issues#449).

const semanticWindowSeedObjects = 25

// semanticWindowToken is absent from makeConvergenceTestObjects' dictionary, so a
// hit on it can only come from the object written inside the window.
const semanticWindowToken = "zulu"

// semanticWindowScore is above every value makeFilterableToRangeableTestObjects
// cycles through, so a range query over the corpus can only hit the object
// written inside the window.
const semanticWindowScore = int64(1000)

// seedText returns the corpus to import plus the object to write once the shard
// is inside the window.
func seedText(t *testing.T, className string) ([]*storobj.Object, *storobj.Object) {
	return makeConvergenceTestObjects(t, semanticWindowSeedObjects, className),
		createTestObjectWithText(className, semanticWindowToken)
}

// seedRangeable is the numeric sibling of seedText.
func seedRangeable(t *testing.T, className string) ([]*storobj.Object, *storobj.Object) {
	return makeFilterableToRangeableTestObjects(t, semanticWindowSeedObjects, className),
		&storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]any{filterableToRangeablePropName: semanticWindowScore},
			},
		}
}

// enterSemanticSwapWindow seeds a shard, runs the migration through its bucket flip,
// and stops before the schema flip — the state the window's writes land in.
// failAfterFlip breaks the swap after the flip instead of letting it finish.
func enterSemanticSwapWindow(t *testing.T, ctx context.Context, class *models.Class, propName string,
	corpus []*storobj.Object, migration ReindexMigrationType,
	newTask func(*testing.T, *Index, string, string) *ShardReindexTaskGeneric,
	failAfterFlip bool,
) *Shard {
	t.Helper()

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		true, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	for _, obj := range corpus {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task := newTask(t, idx, class.Class, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))

	if failAfterFlip {
		// rename(2) refuses to replace a non-empty directory, and the swap
		// renames the displaced bucket onto this path only after it has
		// flipped the bucket pointer.
		occupied := filepath.Join(shard.pathLSM(), task.backupBucketName(propName))
		require.NoError(t, os.MkdirAll(occupied, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(occupied, "blocker"), nil, 0o644))
	}

	logger, _ := logrustest.NewNullLogger()
	p := &ReindexProvider{logger: logger, localNode: "node1", serverCtx: ctx}
	res := p.runShardSwapPhase(ctx, &ReindexTaskPayload{
		MigrationType: migration,
		Collection:    class.Class,
		Properties:    []string{propName},
	}, "unit-1", shard.Name(), shard, []*ShardReindexTaskGeneric{task}, logger)
	if failAfterFlip {
		require.NotEmpty(t, res.Errs, "the swap has to fail, or this row proves nothing")
	} else {
		require.Empty(t, res.Errs)
	}

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
	const textPropName = "title"

	tests := []struct {
		name          string
		newClass      func(className, propName string) *models.Class
		seed          func(*testing.T, string) ([]*storobj.Object, *storobj.Object)
		migration     ReindexMigrationType
		newTask       func(*testing.T, *Index, string, string) *ShardReindexTaskGeneric
		flip          func(prop *models.Property)
		find          func(*testing.T, context.Context, *Shard, string, string) []*storobj.Object
		failAfterFlip bool
	}{
		{
			name:      "enable-filterable",
			newClass:  newEnableFilterableTestClass,
			seed:      seedText,
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
			seed:      seedText,
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
			seed:      seedText,
			migration: ReindexTypeEnableSearchable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, propName,
					models.PropertyTokenizationWord)
				return task
			},
			flip: func(prop *models.Property) { prop.IndexSearchable = boolPtr(true) },
			find: findByBM25,
		},
		{
			name: "enable-rangeable",
			newClass: func(className, _ string) *models.Class {
				return newNoLiveIndexRangeableTestClass(className)
			},
			seed:      seedRangeable,
			migration: ReindexTypeEnableRangeable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				// The production strategy, whose OnMigrationComplete is what
				// makes the swapped bucket queryable on this shard.
				return newFilterableToRangeableTaskWithStrategy(t, idx, className, propName,
					&FilterableToRangeableStrategy{propNames: []string{propName}, generation: 1})
			},
			flip: func(prop *models.Property) { prop.IndexRangeFilters = boolPtr(true) },
			find: findByGreaterThanFilter,
		},
		{
			// The bucket pointer is already flipped when the swap gives up,
			// so the overlay is the only thing routing the window's writes
			// to the bucket that now holds the property.
			name:      "enable-filterable where the swap fails after the bucket flip",
			newClass:  newEnableFilterableTestClass,
			seed:      seedText,
			migration: ReindexTypeEnableFilterable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, propName)
				return task
			},
			flip:          func(prop *models.Property) { prop.IndexFilterable = boolPtr(true) },
			find:          findByEqualFilter,
			failAfterFlip: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SemanticWindow_" + uuid.NewString()[:8]
			class := tc.newClass(className, textPropName)
			propName := class.Properties[0].Name
			corpus, windowObj := tc.seed(t, className)
			shard := enterSemanticSwapWindow(t, ctx, class, propName, corpus, tc.migration,
				tc.newTask, tc.failAfterFlip)

			require.NotEmpty(t, shard.SnapshotPropertyOverlay([]string{propName}),
				"the bucket pointer is flipped, so the overlay must outlive the swap phase")

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

// A property an overlay forces on has no length/null bucket yet; both write
// legs must skip them or every write to the collection fails.
func TestWriteDuringSwapWindowSkipsAbsentLengthAndNullBuckets(t *testing.T) {
	const propName = "title"

	ctx := testCtx()
	className := "SemanticWindowBuckets_" + uuid.NewString()[:8]
	class := noIndexAtAllClass(className, propName)
	require.True(t, class.InvertedIndexConfig.IndexPropertyLength)
	require.True(t, class.InvertedIndexConfig.IndexNullState)

	corpus, _ := seedText(t, className)
	shard := enterSemanticSwapWindow(t, ctx, class, propName, corpus, ReindexTypeEnableFilterable,
		func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
			task, _ := newEnableFilterableTask(t, idx, className, propName)
			return task
		}, false)

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

func findByGreaterThanFilter(t *testing.T, ctx context.Context, shard *Shard,
	className, propName string,
) []*storobj.Object {
	t.Helper()
	found, _, err := shard.ObjectSearch(ctx, semanticWindowSeedObjects,
		&filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorGreaterThan,
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
			Value: &filters.Value{Value: int(semanticWindowScore) - 1, Type: schema.DataTypeInt},
		}}, nil, nil, nil, additional.Properties{}, nil)
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
