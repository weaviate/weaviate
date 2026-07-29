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

package aggregator

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestTextPropertyFromInvertedCutoff(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroup("compactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("compactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const prop = "category"
	bucketName := helpers.BucketFromPropNameLSM(prop)
	require.NoError(t, store.CreateOrLoadBucket(ctx, bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		lsmkv.WithUseBloomFilter(true)))
	b := store.Bucket(bucketName)

	// three live values with distinct doc counts, one value fully deleted
	require.NoError(t, b.RoaringSetAddList([]byte("red"), []uint64{1, 2, 3}))
	require.NoError(t, b.RoaringSetAddList([]byte("green"), []uint64{4, 5}))
	require.NoError(t, b.RoaringSetAddList([]byte("blue"), []uint64{6}))
	require.NoError(t, b.RoaringSetAddList([]byte("dead"), []uint64{7}))
	require.NoError(t, b.RoaringSetRemoveOne([]byte("dead"), 7))
	require.NoError(t, b.FlushAndSwitch())

	limit := 5
	topOccs := []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(&limit)}
	ua := newUnfilteredAggregator(&Aggregator{store: store, logger: logger})

	t.Run("cutoff above the cardinality: exact values and live counts", func(t *testing.T) {
		res, err := ua.textProperty(ctx, aggregation.ParamProperty{
			Name: prop, Aggregators: topOccs, TopOccurrencesCutoff: 10,
		})
		require.NoError(t, err)
		require.False(t, res.TextAggregation.CutoffExceeded)
		assert.Equal(t, 6, res.TextAggregation.Count)
		assert.Equal(t, []aggregation.TextOccurrence{
			{Value: "red", Occurs: 3},
			{Value: "green", Occurs: 2},
			{Value: "blue", Occurs: 1},
		}, res.TextAggregation.Items)
	})

	t.Run("cutoff below the cardinality: sentinel instead of values", func(t *testing.T) {
		res, err := ua.textProperty(ctx, aggregation.ParamProperty{
			Name: prop, Aggregators: topOccs, TopOccurrencesCutoff: 2,
		})
		require.NoError(t, err)
		require.True(t, res.TextAggregation.CutoffExceeded)
		assert.Empty(t, res.TextAggregation.Items)
		assert.Zero(t, res.TextAggregation.Count)
	})

	t.Run("no cutoff: request would use the objects bucket", func(t *testing.T) {
		// no objects bucket exists in this store, so the classic path errors —
		// proving cutoff=0 does not take the inverted path
		_, err := ua.textProperty(ctx, aggregation.ParamProperty{
			Name: prop, Aggregators: topOccs,
		})
		require.Error(t, err)
	})
}
