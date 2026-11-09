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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

// A property that gets no in-memory rep still answers range filters from its
// disk segments.
func TestShardRangeableInMemoryPerProperty(t *testing.T) {
	const (
		namedProp   = "namedPrice"
		unnamedProp = "unnamedPrice"
		numObjects  = 10
		threshold   = 7
	)

	tests := []struct {
		name       string
		all        bool
		props      []string
		wantInMemo map[string]bool
	}{
		{
			name:       "no property named",
			wantInMemo: map[string]bool{namedProp: false, unnamedProp: false},
		},
		{
			name:       "one property named",
			props:      []string{namedProp},
			wantInMemo: map[string]bool{namedProp: true, unnamedProp: false},
		},
		{
			name:       "every property named",
			props:      []string{config.AllProperties},
			wantInMemo: map[string]bool{namedProp: true, unnamedProp: true},
		},
		{
			// INDEX_RANGEABLE_IN_MEMORY=true covers a property no list names
			name:       "the collection-wide flag covers both",
			all:        true,
			wantInMemo: map[string]bool{namedProp: true, unnamedProp: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "RangeableInMemoryPerProp" + uuid.NewString()[:8]
			shard := newRangeableInMemoryPerPropShard(t, ctx, className,
				namedProp, unnamedProp, tt.all, tt.props)

			wantDocIDs := putRangeableInMemoryPerPropObjects(t, ctx, shard, className,
				namedProp, unnamedProp, numObjects, threshold)
			require.NoError(t, shard.store.FlushMemtables(ctx))

			for propName, wantInMemo := range tt.wantInMemo {
				bucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
				require.NotNil(t, bucket, "rangeable bucket of %q", propName)

				assert.Equal(t, wantInMemo, bucket.RangeableServesFromMemory(),
					"%q must serve range reads from %s", propName,
					map[bool]string{true: "the in-memory rep", false: "disk"}[wantInMemo])
				assert.Equal(t, wantDocIDs, rangeableDocIDsAtLeast(t, bucket, int64(threshold)),
					"%q answers the same range filter whichever path serves it", propName)
			}
		})
	}
}

// newRangeableInMemoryPerPropShard builds a shard with two rangeable int props.
func newRangeableInMemoryPerPropShard(t *testing.T, ctx context.Context,
	className, propA, propB string, all bool, props []string,
) *Shard {
	t.Helper()
	rangeable := true
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
		},
		Properties: []*models.Property{
			{Name: propA, DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: &rangeable},
			{Name: propB, DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: &rangeable},
		},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, func(idx *Index) {
			idx.Config.IndexRangeableInMemory = all
			idx.Config.IndexRangeableInMemoryProps = props
		})
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })
	return shard
}

// putRangeableInMemoryPerPropObjects writes n objects carrying the same value in
// both properties, returning the doc IDs of those at or above threshold.
func putRangeableInMemoryPerPropObjects(t *testing.T, ctx context.Context, shard *Shard,
	className, propA, propB string, n, threshold int,
) []uint64 {
	t.Helper()
	var wantDocIDs []uint64
	for i := 0; i < n; i++ {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					propA: int64(i),
					propB: int64(i),
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
		if i >= threshold {
			wantDocIDs = append(wantDocIDs, obj.DocID)
		}
	}
	require.Len(t, wantDocIDs, n-threshold)
	return wantDocIDs
}

// Names match exactly, the way ReindexIndexesAtStartup matches its own.
func TestIndexConfigKeepRangeableInMemory(t *testing.T) {
	tests := []struct {
		name  string
		all   bool
		props []string
		want  map[string]bool
	}{
		{
			name: "nothing configured",
			want: map[string]bool{"price": false, "Price": false},
		},
		{
			name: "the collection-wide flag covers a property no list names",
			all:  true,
			want: map[string]bool{"price": true, "weight": true},
		},
		{
			name:  "a named property, and only that one",
			props: []string{"price"},
			want:  map[string]bool{"price": true, "weight": false},
		},
		{
			name:  "every property of the collection",
			props: []string{config.AllProperties},
			want:  map[string]bool{"price": true, "weight": true},
		},
		{
			name:  "a name differing in case does not match",
			props: []string{"price"},
			want:  map[string]bool{"price": true, "Price": false, "PRICE": false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := IndexConfig{
				IndexRangeableInMemory:      tt.all,
				IndexRangeableInMemoryProps: tt.props,
			}
			for propName, want := range tt.want {
				assert.Equal(t, want, cfg.keepRangeableInMemory(propName), propName)
			}
		})
	}
}

// A key not matching the class name exactly would leave every property
// unnamed. No other test sees that, because both build IndexConfig by hand.
func TestMigratorAddClassFlattensRangeableInMemoryProps(t *testing.T) {
	const className = "Foo"

	tests := []struct {
		name       string
		configured map[string][]string
		want       []string
	}{
		{
			name: "nothing configured",
		},
		{
			name:       "the collection's own properties, not another's",
			configured: map[string][]string{className: {"price"}, "Bar": {"weight"}},
			want:       []string{"price"},
		},
		{
			name:       "a collection the value does not name",
			configured: map[string][]string{"Bar": {"weight"}},
		},
		{
			// indexID lowercases the class name for the directory, and the
			// lookup must not
			name:       "a key differing in case does not match",
			configured: map[string][]string{"foo": {"price"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			repo, migrator, _ := newLazyLoadRepo(t, singleShardState())
			t.Cleanup(func() { repo.Shutdown(context.Background()) })
			repo.config.IndexRangeableInMemoryProps = tt.configured

			require.NoError(t, migrator.AddClass(ctx, newClassWithWarmProp(className)))

			idx := repo.GetIndex(schema.ClassName(className))
			require.NotNil(t, idx)
			require.Equal(t, tt.want, idx.Config.IndexRangeableInMemoryProps)
		})
	}
}
