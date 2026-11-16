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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

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

func TestTheCompletionGateRefusesAClosedBucket(t *testing.T) {
	tests := []struct {
		name      string
		propName  string
		strategy  MigrationStrategy
		canonical func(string) string
	}{
		{
			name: "enable-filterable", propName: "title",
			strategy:  &EnableFilterableStrategy{propNames: []string{"title"}},
			canonical: helpers.BucketFromPropNameLSM,
		},
		{
			name: "enable-searchable", propName: "title",
			strategy:  &EnableSearchableStrategy{propNames: []string{"title"}},
			canonical: helpers.BucketSearchableFromPropNameLSM,
		},
		{
			name: "filterable-to-rangeable", propName: "score",
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
		{
			name: "null-state", propName: "title",
			strategy:  &NullLengthStrategy{propNames: []string{"title"}},
			canonical: helpers.BucketFromPropNameNullLSM,
		},
		{
			name: "property-length", propName: "title",
			strategy:  &NullLengthStrategy{length: true, propNames: []string{"title"}},
			canonical: helpers.BucketFromPropNameLengthLSM,
		},
	}
	require.Len(t, tests, len(strategiesByCode(1)),
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

			require.NoError(t, task.ensureCanonicalBucketsOpen(ctx, shard, props))

			require.NoError(t, shard.store.ShutdownBucket(ctx, canonical))
			err := task.ensureCanonicalBucketsOpen(ctx, shard, props)
			require.ErrorContains(t, err, "refusing to report migration complete")
			require.ErrorContains(t, err, canonical)
			require.Nil(t, shard.store.Bucket(canonical))
		})
	}
}
