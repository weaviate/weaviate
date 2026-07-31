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

//go:build integrationTest

package aggregator

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// bloom estimates are within a few percent of the true key count at these sizes
const cardinalityTolerancePct = 5

type bucketSpec struct {
	distinct int
	// noBloom makes GetKeysCount fail, the way a bucket built without bloom
	// filters does in production
	noBloom bool
}

func TestApproximateCardinality(t *testing.T) {
	const prop = "title"

	tests := []struct {
		name       string
		filterable *bucketSpec
		searchable *bucketSpec
		expect     int
		wantNil    bool
		wantErr    bool
	}{
		{
			name:       "filterable bucket only",
			filterable: &bucketSpec{distinct: 5000},
			expect:     5000,
		},
		{
			name:       "searchable bucket only",
			searchable: &bucketSpec{distinct: 3000},
			expect:     3000,
		},
		{
			// the two buckets index the same values differently, so the estimate
			// is the larger of the two, not their sum
			name:       "both buckets, searchable larger",
			filterable: &bucketSpec{distinct: 500},
			searchable: &bucketSpec{distinct: 4000},
			expect:     4000,
		},
		{
			name:       "both buckets, filterable larger",
			filterable: &bucketSpec{distinct: 4000},
			searchable: &bucketSpec{distinct: 500},
			expect:     4000,
		},
		{
			name:    "no bucket for the property",
			wantNil: true,
		},
		{
			name:       "one bucket errors, the other still counts",
			filterable: &bucketSpec{distinct: 2000, noBloom: true},
			searchable: &bucketSpec{distinct: 3000},
			expect:     3000,
		},
		{
			name:       "the only bucket errors",
			filterable: &bucketSpec{distinct: 2000, noBloom: true},
			wantErr:    true,
		},
		{
			name:       "every bucket errors",
			filterable: &bucketSpec{distinct: 2000, noBloom: true},
			searchable: &bucketSpec{distinct: 3000, noBloom: true},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := newCardinalityStore(ctx, t)
			if tt.filterable != nil {
				createFilterableBucket(ctx, t, store, prop, *tt.filterable)
			}
			if tt.searchable != nil {
				createSearchableBucket(ctx, t, store, prop, *tt.searchable)
			}

			a := &Aggregator{store: store}
			est, err := a.approximateCardinality(schema.PropertyName(prop))

			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, est)
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				require.Nil(t, est)
				return
			}
			require.NotNil(t, est)
			assert.InDelta(t, tt.expect, float64(*est),
				float64(tt.expect)*cardinalityTolerancePct/100)
		})
	}
}

func TestAddApproximateCardinalities(t *testing.T) {
	const (
		counted   = "counted"   // has a bucket, requests an estimate
		broken    = "broken"    // bucket cannot be counted
		absent    = "absent"    // no bucket on this shard
		untouched = "untouched" // requests no estimate
	)
	const distinct = 1000

	ctx := context.Background()
	store := newCardinalityStore(ctx, t)
	createFilterableBucket(ctx, t, store, counted, bucketSpec{distinct: distinct})
	createFilterableBucket(ctx, t, store, broken, bucketSpec{distinct: 500, noBloom: true})
	createFilterableBucket(ctx, t, store, untouched, bucketSpec{distinct: 700})

	logger, _ := test.NewNullLogger()
	a := &Aggregator{
		logger: logger,
		store:  store,
		params: aggregation.Params{
			Properties: []aggregation.ParamProperty{
				{Name: counted, ApproximateCardinality: true},
				{Name: broken, ApproximateCardinality: true},
				{Name: absent, ApproximateCardinality: true},
				{Name: untouched, Aggregators: []aggregation.Aggregator{{Type: "count"}}},
			},
		},
	}

	res := &aggregation.Result{Groups: []aggregation.Group{
		{},
		{Properties: map[string]aggregation.Property{
			counted:   {Type: aggregation.PropertyTypeText},
			untouched: {Type: aggregation.PropertyTypeText},
		}},
	}}

	a.addApproximateCardinalities(res)

	require.Len(t, res.Groups, 2)
	for i, group := range res.Groups {
		t.Run(fmt.Sprintf("group %d", i), func(t *testing.T) {
			require.NotNil(t, group.Properties[counted].ApproximateCardinality)
			assert.InDelta(t, distinct, float64(*group.Properties[counted].ApproximateCardinality),
				distinct*cardinalityTolerancePct/100)

			assert.Nil(t, group.Properties[untouched].ApproximateCardinality)
			assert.NotContains(t, group.Properties, broken,
				"a bucket that cannot be counted must not produce an entry")
			assert.NotContains(t, group.Properties, absent)
		})
	}

	// aggregations already computed for the property survive the attachment
	assert.Equal(t, aggregation.PropertyTypeText, res.Groups[1].Properties[counted].Type)
	assert.Equal(t, aggregation.PropertyTypeText, res.Groups[1].Properties[untouched].Type)
}

func newCardinalityStore(ctx context.Context, t *testing.T) *lsmkv.Store {
	t.Helper()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })
	return store
}

// createFilterableBucket mirrors the roaringset bucket a filterable property
// gets, whose keys are the distinct property values.
func createFilterableBucket(ctx context.Context, t *testing.T, store *lsmkv.Store,
	propName string, spec bucketSpec,
) {
	t.Helper()
	name := helpers.BucketFromPropNameLSM(propName)
	require.NoError(t, store.CreateOrLoadBucket(ctx, name,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		lsmkv.WithUseBloomFilter(!spec.noBloom)))

	b := store.Bucket(name)
	for i := 0; i < spec.distinct; i++ {
		require.NoError(t, b.RoaringSetAddList(cardinalityKey(propName, i), []uint64{uint64(i)}))
	}
	require.NoError(t, b.FlushAndSwitch())
}

// createSearchableBucket mirrors the map bucket a searchable property gets,
// whose keys are the distinct tokens.
func createSearchableBucket(ctx context.Context, t *testing.T, store *lsmkv.Store,
	propName string, spec bucketSpec,
) {
	t.Helper()
	name := helpers.BucketSearchableFromPropNameLSM(propName)
	require.NoError(t, store.CreateOrLoadBucket(ctx, name,
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection),
		lsmkv.WithUseBloomFilter(!spec.noBloom)))

	b := store.Bucket(name)
	for i := 0; i < spec.distinct; i++ {
		require.NoError(t, b.MapSet(cardinalityKey(propName, i), lsmkv.MapPair{
			Key:   []byte(fmt.Sprintf("%08d", i)),
			Value: []byte{0, 0, 0, 0, 0, 0, 0, 1},
		}))
	}
	require.NoError(t, b.FlushAndSwitch())
}

func cardinalityKey(propName string, i int) []byte {
	return []byte(fmt.Sprintf("%s-value-%06d", propName, i))
}
