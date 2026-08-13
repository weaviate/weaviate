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
	"encoding/binary"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/entities/storobj"
)

// putTruncatedObject stores a value whose header is intact but whose body is cut short,
// modelling a torn write: the declared vector or properties schema reaches past the end
// of the stored value.
func putTruncatedObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64, keepBytes int,
	payloadBytes int, legacyVec []float32, named map[string][]float32,
) {
	t.Helper()
	data := marshalTestObject(t, docID, docID, payloadBytes, legacyVec, named)
	require.Less(t, keepBytes, len(data), "truncation must actually remove bytes")

	require.NoError(t, bucket.Put(keyForDocID(docID), data[:keepBytes]))
}

// TestPrefillTargetedSkipsTruncatedRows: a row declaring a vector or schema past the
// stored value is corrupt. The scan must skip it and carry on — one torn write must
// not abort the prefill for the healthy rows around it, nor cache anything decoded
// from whatever follows the value.
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
		// Sizes that stop inside the front sections VectorFromBinary walks to reach
		// the target-vector segment. They are short enough to reach the whole-value
		// fallback rather than the tail read, so the walk itself is what has to bound
		// them — an unbounded one slices past the value and panics out of the scan.
		{
			name:      "truncated at the legacy vector length field",
			target:    "custom",
			keepBytes: 44,
			named:     map[string][]float32{"custom": {7, 7, 7}},
		},
		{
			name:      "truncated one byte into the class name length",
			target:    "custom",
			keepBytes: 45,
			named:     map[string][]float32{"custom": {7, 7, 7}},
		},
		{
			name:      "truncated inside the class name",
			target:    "custom",
			keepBytes: 60,
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

// putWrongTailOffsetObject rewrites the schema length so the tail offset it implies
// still lands inside the value, just in the wrong place. tailStart is pos+4+schemaLen,
// so the length field sits at tailStart-4-schemaLen and halving it moves the implied
// tail into the middle of the schema.
func putWrongTailOffsetObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64,
	payloadBytes int, named map[string][]float32,
) {
	t.Helper()
	data := marshalTestObject(t, docID, docID, payloadBytes, nil, named)

	tailStart, schemaLen, ok, err := storobj.VectorTailOffsetFromPrefix(data[:min(prefillPeekBytes, len(data))])
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, schemaLen/2, uint32(prefillTargetedMinSchemaLen),
		"the corrupt length must still clear the tail gate, or the row takes the fallback instead")

	lenField := int(tailStart) - 4 - int(schemaLen)
	binary.LittleEndian.PutUint32(data[lenField:lenField+4], schemaLen/2)
	require.NoError(t, bucket.Put(keyForDocID(docID), data))
}

// TestPrefillTargetedSkipsRowWithWrongTailOffset covers the corruption that gets past
// the tailStart >= ValueSize guard: an offset that is in range and wrong. Every other
// case in this file is a truncation, which that guard catches. Here the tail read
// succeeds and hands the decoder schema bytes, so only its own bound checks stop the
// row being cached as a vector.
func TestPrefillTargetedSkipsRowWithWrongTailOffset(t *testing.T) {
	const healthy = 10
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	exp := map[uint64][]float32{}
	live := map[uint64]bool{}
	for i := uint64(0); i < healthy; i++ {
		vec := []float32{float32(i) + 1, float32(i) + 2}
		putTargetedObject(t, bucket, i, i, targetedGatePayload, nil,
			map[string][]float32{"custom": vec})
		exp[i] = vec
		live[i] = true
	}

	const corruptID = healthy
	putWrongTailOffsetObject(t, bucket, corruptID, targetedGatePayload,
		map[string][]float32{"custom": {7, 7, 7}})
	live[corruptID] = true // indexed, so only the corruption can exclude it
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(healthy + 1)
	h := newTargetedTestIndex(store, c, "vectors_custom", live, healthy+1)

	require.NoError(t, h.prefillFromScan(context.Background(),
		func(ctx context.Context, onVector prefillOnVector) error {
			return h.scanObjectVectorsTargeted(ctx, bucket, "custom", onVector)
		}))

	requireCacheContains(t, c, exp)
	require.Equal(t, int64(healthy), c.CountVectors(),
		"the row must be skipped, not cached as a vector decoded from its own schema")
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
