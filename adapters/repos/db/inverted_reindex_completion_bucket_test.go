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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestPromotedCompletionServesTheIndexItAdvertises(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	tests := []struct {
		name      string
		class     func(className string) *models.Class
		newTask   func(t *testing.T, idx *Index, className, unitID string) *ShardReindexTaskGeneric
		enable    func(prop *models.Property)
		canonical func(propName string) string
		serves    func(t *testing.T, b *lsmkv.Bucket) map[string][]uint64
	}{
		{
			name:  "enable-filterable",
			class: func(cn string) *models.Class { return newEnableFilterableTestClass(cn, propName) },
			newTask: func(t *testing.T, idx *Index, cn, unitID string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, cn, unitID, propName)
				return task
			},
			enable:    func(p *models.Property) { p.IndexFilterable = boolPtr(true) },
			canonical: helpers.BucketFromPropNameLSM,
			serves:    fingerprintRoaringSetBucket,
		},
		{
			name:  "enable-searchable",
			class: func(cn string) *models.Class { return newEnableSearchableTestClass(cn, []string{propName}) },
			newTask: func(t *testing.T, idx *Index, cn, unitID string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, cn, propName, models.PropertyTokenizationWord, unitID)
				return task
			},
			enable:    func(p *models.Property) { p.IndexSearchable = boolPtr(true) },
			canonical: helpers.BucketSearchableFromPropNameLSM,
			serves:    fingerprintInvertedBucket,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CompletionBucket_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, tc.class(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)

			for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task := tc.newTask(t, idx, className, shard.migrationUnit())
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))
			postClass := tc.class(className)
			for _, prop := range postClass.Properties {
				if prop.Name == propName {
					tc.enable(prop)
				}
			}
			reloaded, err := idx.initShard(ctx, shardName, postClass, nil, true, true)
			require.NoError(t, err)
			promoted := reloaded.(*Shard)
			defer promoted.Shutdown(ctx)

			canonical := tc.canonical(propName)
			before := tc.serves(t, promoted.store.Bucket(canonical))
			require.NotEmpty(t, before, "fixture: the migration has to have produced an index")

			require.NoError(t, promoted.store.ShutdownBucket(ctx, canonical))
			require.Nil(t, promoted.store.Bucket(canonical), "fixture: the bucket has to be closed")

			require.NoError(t, task.RunSwapOnShard(ctx, promoted),
				"the completion retry must not report success over a closed bucket")

			require.Equal(t, before, tc.serves(t, promoted.store.Bucket(canonical)),
				"after the completion the property must serve exactly what the migration built")
		})
	}
}

func completionGateClass(className string) *models.Class {
	on := true
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
		},
		Properties: []*models.Property{
			{
				Name: "title", DataType: schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &on, IndexSearchable: &on,
			},
			{
				Name: "score", DataType: schema.DataTypeInt.PropString(),
				IndexFilterable: &on, IndexRangeFilters: &on,
			},
		},
	}
}

func TestTheCompletionGateOnlyTouchesAClosedBucket(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		strategy  MigrationStrategy
		canonical func(string) string
		reopens   bool
	}{
		{
			name: "enable-filterable", propName: "title", reopens: true,
			strategy:  &EnableFilterableStrategy{propNames: []string{"title"}},
			canonical: helpers.BucketFromPropNameLSM,
		},
		{
			name: "enable-searchable", propName: "title", reopens: true,
			strategy:  &EnableSearchableStrategy{propNames: []string{"title"}},
			canonical: helpers.BucketSearchableFromPropNameLSM,
		},
		{
			name: "filterable-to-rangeable", propName: "score", reopens: true,
			strategy:  &FilterableToRangeableStrategy{propNames: []string{"score"}},
			canonical: helpers.BucketRangeableFromPropNameLSM,
		},
		{
			name: "searchable-retokenize", propName: "title",
			strategy:  &SearchableRetokenizeStrategy{propName: "title"},
			canonical: helpers.BucketSearchableFromPropNameLSM,
		},
		{
			name: "filterable-retokenize", propName: "title",
			strategy:  &FilterableRetokenizeStrategy{propName: "title"},
			canonical: helpers.BucketFromPropNameLSM,
		},
		{
			name: "rebuild-searchable", propName: "title",
			strategy:  &RebuildSearchableStrategy{propNames: []string{"title"}},
			canonical: helpers.BucketSearchableFromPropNameLSM,
		},
		{
			name: "roaringset-refresh", propName: "title",
			strategy:  &RoaringSetRefreshStrategy{},
			canonical: helpers.BucketFromPropNameLSM,
		},
		{
			name: "map-to-blockmax", propName: "title",
			strategy:  &MapToBlockmaxStrategy{},
			canonical: helpers.BucketSearchableFromPropNameLSM,
		},
	}
	require.Len(t, tests, len(strategiesByMigrationDir(1)),
		"a strategy missing from this table would never have its gate behavior checked")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CompletionGate_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, completionGateClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			task := &ShardReindexTaskGeneric{strategy: tt.strategy, logger: idx.logger}
			canonical := tt.canonical(tt.propName)
			props := []string{tt.propName}

			t.Run("the bucket is already open", func(t *testing.T) {
				require.NotNil(t, shard.store.Bucket(canonical), "fixture: shard init opens it")
				shard.setRangeableLocallyReady(tt.propName, true)
				marked := len(shard.getSearchableBlockmaxProperties())

				require.NoError(t, task.ensureCanonicalBucketsOpen(ctx, shard, props))

				require.Equal(t, marked, len(shard.getSearchableBlockmaxProperties()),
					"the gate fired a start-of-migration hook over a bucket that needed nothing, "+
						"and the slice it grew is scanned on every object update")
				require.True(t, shard.IsRangeableLocallyReady(tt.propName),
					"and it took a property off the representation it is already serving from")
			})

			t.Run("the bucket is closed", func(t *testing.T) {
				require.NoError(t, shard.store.ShutdownBucket(ctx, canonical))
				require.Nil(t, shard.store.Bucket(canonical), "fixture: the bucket has to be closed")

				err := task.ensureCanonicalBucketsOpen(ctx, shard, props)
				if !tt.reopens {
					require.ErrorContains(t, err, "refusing to report migration complete")
					require.ErrorContains(t, err, canonical)
					require.Nil(t, shard.store.Bucket(canonical))
					return
				}
				require.NoError(t, err)
				require.NotNil(t, shard.store.Bucket(canonical),
					"the completion may not advertise an index nothing serves")
			})
		})
	}
}
