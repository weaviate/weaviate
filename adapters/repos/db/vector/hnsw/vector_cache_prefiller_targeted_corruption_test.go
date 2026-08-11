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

package hnsw

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// putTruncatedObject stores a value whose header is intact but whose body is cut short,
// modelling a torn write: the declared vector or properties schema reaches past the end
// of the stored value.
func putTruncatedObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64, keepBytes int,
	payloadBytes int, legacyVec []float32, named map[string][]float32,
) {
	t.Helper()
	obj := storobj.New(docID)
	obj.Object = models.Object{
		ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", docID)),
		Class:      "Test",
		Properties: map[string]interface{}{"filler": strings.Repeat("x", payloadBytes)},
	}
	obj.Vector = legacyVec
	obj.Vectors = named
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	require.Less(t, keepBytes, len(data), "truncation must actually remove bytes")

	require.NoError(t, bucket.Put(keyForDocID(docID), data[:keepBytes]))
}

// TestPrefillTargetedSkipsTruncatedRows: a row whose declared vector or schema runs past
// the stored value is corrupt. The scan must skip it and keep going — never cache a
// vector decoded from whatever bytes follow, and never abort the prefill for the healthy
// rows around it. Values are handed to the decoder as subslices of an mmapped segment
// whose capacity extends past the row, so an unbounded decode of these rows reads the
// neighbouring row rather than failing.
func TestPrefillTargetedSkipsTruncatedRows(t *testing.T) {
	const payload = 16 << 10

	cases := []struct {
		name      string
		target    string
		keepBytes int
		legacyVec []float32
		named     map[string][]float32
	}{
		{
			// header and length field intact, declared legacy vector reaches past the end
			name:      "legacy vector beyond value end",
			target:    "",
			keepBytes: 200,
			legacyVec: make([]float32, 100),
		},
		{
			// front sections decode cleanly but place the vector tail past the end
			name:      "named vector tail beyond value end",
			target:    "custom",
			keepBytes: 1000,
			named:     map[string][]float32{"custom": {7, 7, 7}},
		},
		{
			// too short to even hold the vector length field
			name:      "value shorter than the header",
			target:    "custom",
			keepBytes: 20,
			named:     map[string][]float32{"custom": {7, 7, 7}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)

			const healthy = 10
			exp := map[uint64][]float32{}
			live := map[uint64]bool{}
			for i := uint64(0); i < healthy; i++ {
				vec := []float32{float32(i) + 1, float32(i) + 2}
				var legacyVec []float32
				named := map[string][]float32{}
				if tc.target == "" {
					legacyVec = vec
				} else {
					named[tc.target] = vec
				}
				putTargetedObject(t, bucket, i, i, payload, legacyVec, named)
				exp[i] = vec
				live[i] = true
			}

			const corruptID = healthy
			putTruncatedObject(t, bucket, corruptID, tc.keepBytes, payload, tc.legacyVec, tc.named)
			live[corruptID] = true // indexed, so only the corruption can exclude it
			require.NoError(t, bucket.FlushAndSwitch())

			id := "vectors_" + tc.target
			if tc.target == "" {
				id = "main"
			}
			logger, _ := test.NewNullLogger()
			c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
			c.Grow(healthy + 1)
			h := newTargetedTestIndex(store, c, id, live, healthy+1)

			require.NoError(t, h.prefillFromScan(context.Background(),
				func(ctx context.Context, onVector prefillOnVector) error {
					return h.scanObjectVectorsTargeted(ctx, bucket, tc.target, onVector)
				}))

			requireCacheContains(t, c, exp)
			require.Equal(t, int64(healthy), c.CountVectors(),
				"the corrupt row must be skipped, not decoded from neighbouring bytes")
		})
	}
}

// TestPrefillTargetedSkipsRowUnderForeignKey models the corruption the scan
// cannot detect itself: an index node whose offsets resolve to another live
// row. Those bytes decode cleanly, so only the uuid disagreeing with the key
// separates them from a healthy row.
func TestPrefillTargetedSkipsRowUnderForeignKey(t *testing.T) {
	const payload = 16 << 10
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	const healthy = 5
	exp := map[uint64][]float32{}
	live := map[uint64]bool{}
	for i := uint64(0); i < healthy; i++ {
		vec := []float32{float32(i) + 1, float32(i) + 2}
		putTargetedObject(t, bucket, i, i, payload, vec, nil)
		exp[i] = vec
		live[i] = true
	}

	// a well-formed row carrying another object's uuid, stored under this key
	const foreignID = healthy
	putTargetedObject(t, bucket, foreignID+1000, foreignID, payload, []float32{9, 9}, nil)
	data, err := bucket.Get(keyForDocID(foreignID + 1000))
	require.NoError(t, err)
	require.NoError(t, bucket.Put(keyForDocID(foreignID), data))
	// drop the donor row, so only the mismatched copy remains
	require.NoError(t, bucket.Delete(keyForDocID(foreignID+1000)))
	live[foreignID] = true // indexed, so only the key mismatch can exclude it
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(healthy + 1)
	h := newTargetedTestIndex(store, c, "main", live, healthy+1)

	require.NoError(t, h.prefillFromScan(context.Background(),
		func(ctx context.Context, onVector prefillOnVector) error {
			return h.scanObjectVectorsTargeted(ctx, bucket, "", onVector)
		}))

	requireCacheContains(t, c, exp)
	require.Equal(t, int64(healthy), c.CountVectors(),
		"a row whose uuid does not match its key must not be cached")
}
