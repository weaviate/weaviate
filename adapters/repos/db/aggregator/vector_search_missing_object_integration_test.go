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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// fixedVectorIndex answers every search with the same ranked doc IDs, so the
// test controls exactly which doc ID the index still knows.
type fixedVectorIndex struct {
	ids   []uint64
	dists []float32
}

func (f *fixedVectorIndex) Type() common.IndexType { return common.IndexTypeHNSW }
func (f *fixedVectorIndex) Multivector() bool      { return false }

func (f *fixedVectorIndex) SearchByVector(context.Context, []float32, int, helpers.AllowList) ([]uint64, []float32, error) {
	return append([]uint64{}, f.ids...), append([]float32{}, f.dists...), nil
}

func (f *fixedVectorIndex) SearchByVectorDistance(context.Context, []float32, float32, int64, helpers.AllowList) ([]uint64, []float32, error) {
	return append([]uint64{}, f.ids...), append([]float32{}, f.dists...), nil
}

// TestAggregatorObjectVectorSearch_MissingObjectKeepsDistancesAligned checks
// that the hybrid fusion input (objs[i] paired with dists[i]) stays aligned
// when the vector index still ranks a doc ID whose row is gone from the objects
// bucket (a delete whose index tombstone has not landed yet) and when a
// certainty cutoff trims the ranked ids: the missing object is dropped together
// with its distance, and the distances are trimmed with the ids.
func TestAggregatorObjectVectorSearch_MissingObjectKeepsDistancesAligned(t *testing.T) {
	const count = 5
	ctx := context.Background()
	ranked := []uint64{0, 1, 2, 3, 4}
	dists := []float32{0.1, 0.2, 0.3, 0.4, 0.5}

	cases := []struct {
		name      string
		missing   []uint64
		certainty float64 // 0 = no cutoff; targetDist = (1 - certainty) * 2
		wantIDs   []uint64
	}{
		{name: "missing object is the best hit", missing: []uint64{0}, wantIDs: []uint64{1, 2, 3, 4}},
		{name: "missing object is in the middle", missing: []uint64{2}, wantIDs: []uint64{0, 1, 3, 4}},
		{name: "missing object is the worst hit", missing: []uint64{4}, wantIDs: []uint64{0, 1, 2, 3}},
		{name: "cutoff keeps three hits, nothing missing", certainty: 0.85, wantIDs: []uint64{0, 1, 2}},
		{name: "cutoff keeps three hits, one missing", missing: []uint64{1}, certainty: 0.85, wantIDs: []uint64{0, 2}},
		{name: "cutoff excludes every hit", certainty: 0.99, wantIDs: []uint64{}},
		{name: "cutoff excludes every hit and one is missing", missing: []uint64{0}, certainty: 0.99, wantIDs: []uint64{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := objectsBucketWith(t, ctx, count)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)
			for _, docID := range tc.missing {
				deleteFixtureObject(t, bucket, docID)
			}

			logger, _ := test.NewNullLogger()
			limit := count
			agg := &Aggregator{
				logger:      logger,
				store:       store,
				params:      aggregation.Params{ObjectLimit: &limit, Certainty: tc.certainty},
				vectorIndex: &fixedVectorIndex{ids: ranked, dists: dists},
			}

			objs, gotDists, err := agg.objectVectorSearch(ctx, models.Vector([]float32{1, 0}), nil)
			require.NoError(t, err)
			require.Len(t, objs, len(tc.wantIDs))
			require.Len(t, gotDists, len(objs), "one distance per returned object")
			for i, obj := range objs {
				require.Equal(t, tc.wantIDs[i], obj.DocID, "object at position %d", i)
				require.Equal(t, dists[obj.DocID], gotDists[i],
					"object at position %d (doc %d) carries another object's distance", i, obj.DocID)
			}
		})
	}
}

// objectsBucketWith builds an objects bucket shaped like Shard.initObjectBucket
// holding count objects with doc IDs 0..count-1, keyed like deleteFixtureObject
// expects.
func objectsBucketWith(t *testing.T, ctx context.Context, count int) *lsmkv.Store {
	t.Helper()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithKeepTombstones(true),
		lsmkv.WithCalcCountNetAdditions(true),
		lsmkv.WithClassName("AggregatorFixture"),
	))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	for i := 0; i < count; i++ {
		docID := uint64(i)
		obj := storobj.FromObject(&models.Object{
			Class:      "AggregatorFixture",
			ID:         strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i))).String()),
			Properties: map[string]interface{}{"value": float64(i)},
		}, nil, nil, nil)
		obj.DocID = docID
		value, err := obj.MarshalBinary()
		require.NoError(t, err)
		key, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
		require.NoError(t, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(t, bucket.Put(key, value,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
	}
	require.NoError(t, bucket.FlushAndSwitch())
	return store
}

// deleteFixtureObject removes the row objectsBucketWith wrote for docID,
// including its doc-ID secondary key, without telling any index.
func deleteFixtureObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64) {
	t.Helper()
	key, err := uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(docID))).MarshalBinary()
	require.NoError(t, err)
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	require.NoError(t, bucket.Delete(key,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
}
