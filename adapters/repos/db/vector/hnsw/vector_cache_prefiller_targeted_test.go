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
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// putTargetedObject writes an object whose bucket key (keyID) is independent of its
// docID, so tests can model updates: the same key rewritten with a new docID, the
// old docID's row surviving in an older segment.
func putTargetedObject(t *testing.T, bucket *lsmkv.Bucket, keyID, docID uint64,
	payloadBytes int, legacyVec []float32, named map[string][]float32,
) {
	t.Helper()
	obj := storobj.New(docID)
	obj.Object = models.Object{
		ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", keyID)),
		Class:      "Test",
		Properties: map[string]interface{}{"filler": strings.Repeat("x", payloadBytes)},
	}
	obj.Vector = legacyVec
	obj.Vectors = named
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, bucket.Put(keyForDocID(keyID), data))
}

func newTargetedTestIndex(store *lsmkv.Store, c cache.Cache[float32], id string,
	liveNodes map[uint64]bool, nodesLen int,
) *hnsw {
	logger, _ := test.NewNullLogger()
	nodes := make([]*vertex, nodesLen)
	for id := range liveNodes {
		nodes[id] = &vertex{level: 0}
	}
	return &hnsw{
		store:             store,
		cache:             c,
		nodes:             nodes,
		id:                id,
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
		shardedNodeLocks:  common.NewDefaultShardedRWLocks(),
		tombstoneLock:     &sync.RWMutex{},
		tombstones:        map[uint64]struct{}{},
	}
}

// prefillTargeted runs the targeted scan directly, bypassing the avg-entry-size
// routing gate so both the tail path and the small-schema fallback get exercised.
func prefillTargeted(t *testing.T, h *hnsw, target string) error {
	t.Helper()
	bucket := h.store.Bucket(helpers.ObjectsBucketLSM)
	return h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
		return h.scanObjectVectorsTargeted(ctx, bucket, target, onVector)
	})
}

// TestPrefillTargetedMatchesCursorScan is the contract test: on identical data —
// updates and deletes included — the targeted prefill must produce exactly the
// cache the existing cursor-scan prefill produces, for named and legacy targets
// and for schemas below and above the tail-read threshold.
func TestPrefillTargetedMatchesCursorScan(t *testing.T) {
	cases := []struct {
		name    string
		payload int
		target  string
	}{
		{"named target, small schema (whole-read fallback)", 1024, "custom"},
		{"named target, large schema (tail reads)", 16 << 10, "custom"},
		{"legacy target, large schema", 16 << 10, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)

			live := map[uint64]bool{}
			put := func(keyID, docID uint64, dims int) {
				vec := make([]float32, dims)
				for j := range vec {
					vec[j] = float32(docID) + float32(j)*0.25
				}
				var legacyVec []float32
				named := map[string][]float32{}
				if tc.target == "" {
					legacyVec = vec
				} else {
					named[tc.target] = vec
					named["sibling"] = []float32{9, 9}
				}
				putTargetedObject(t, bucket, keyID, docID, tc.payload, legacyVec, named)
			}

			// segment 1: docs 0..29 (doc 15 with a vector larger than the peek)
			for i := uint64(0); i < 30; i++ {
				dims := 3
				if i == 15 {
					dims = 300
				}
				put(i, i, dims)
				live[i] = true
			}
			require.NoError(t, bucket.FlushAndSwitch())
			// segment 2: key 5 updated under docID 100, key 7 deleted
			put(5, 100, 4)
			live[100] = true
			delete(live, 5)
			require.NoError(t, bucket.Delete(keyForDocID(7)))
			delete(live, 7)
			require.NoError(t, bucket.FlushAndSwitch())
			// memtable: docs 30..34
			for i := uint64(30); i < 35; i++ {
				put(i, i, 3)
				live[i] = true
			}

			id := "vectors_" + tc.target
			if tc.target == "" {
				id = "main"
			}
			logger, _ := test.NewNullLogger()
			mustHit := func(_ context.Context, id uint64) ([]float32, error) {
				return nil, fmt.Errorf("unexpected cache miss for id %d", id)
			}
			collect := func(prefill func(h *hnsw) error) map[uint64][]float32 {
				c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
				c.Grow(101)
				h := newTargetedTestIndex(store, c, id, live, 101)
				require.NoError(t, prefill(h))
				out := map[uint64][]float32{}
				for docID := range live {
					v, err := c.Get(context.Background(), docID)
					require.NoError(t, err)
					out[docID] = v
				}
				require.Equal(t, int64(len(live)), c.CountVectors())
				return out
			}

			viaCursor := collect(func(h *hnsw) error {
				return h.prefillCacheParallel(context.Background())
			})
			viaTargeted := collect(func(h *hnsw) error {
				return prefillTargeted(t, h, tc.target)
			})
			require.Equal(t, viaCursor, viaTargeted)
		})
	}
}

// TestPrefillTargetedHNSWExclusions covers the deliberate divergences from the
// cursor scan: rows whose doc is not indexed, or whose node is HNSW-tombstoned
// while the bucket row is still live, must not be prefilled.
func TestPrefillTargetedHNSWExclusions(t *testing.T) {
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	exp := map[uint64][]float32{}
	live := map[uint64]bool{}
	for i := uint64(0); i < 10; i++ {
		vec := []float32{float32(i), float32(i) + 0.5}
		putTargetedObject(t, bucket, i, i, 16<<10, nil, map[string][]float32{"custom": vec})
		exp[i] = vec
		live[i] = true
	}
	// doc 20: in the bucket, never indexed; doc 5: indexed but HNSW-tombstoned
	putTargetedObject(t, bucket, 20, 20, 16<<10, nil, map[string][]float32{"custom": {5, 5}})
	require.NoError(t, bucket.FlushAndSwitch())
	delete(exp, 5)

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(21)
	h := newTargetedTestIndex(store, c, "vectors_custom", live, 21)
	h.tombstones[5] = struct{}{}
	require.NoError(t, prefillTargeted(t, h, "custom"))

	requireCacheContains(t, c, exp)
}

// TestUseTargetedPrefillScanGate: the env flag alone is not enough — small-entry
// buckets stay on the cursor scan, where targeted reads would only add index-walk
// overhead.
func TestUseTargetedPrefillScanGate(t *testing.T) {
	build := func(n uint64, payload int) *lsmkv.Bucket {
		store := newTestObjectsStore(t)
		bucket := store.Bucket(helpers.ObjectsBucketLSM)
		for i := uint64(0); i < n; i++ {
			putTargetedObject(t, bucket, i, i, payload, nil, map[string][]float32{"custom": {1, 2}})
		}
		require.NoError(t, bucket.FlushAndSwitch())
		return bucket
	}
	// enough entries that per-segment fixed overhead does not dominate the average
	small := build(200, 100)
	large := build(20, 16<<10)

	h := newTargetedTestIndex(nil, nil, "vectors_custom", nil, 0)

	t.Setenv("HNSW_PREFILL_TARGETED_READS", "true")
	require.False(t, h.useTargetedPrefillScan(small))
	require.True(t, h.useTargetedPrefillScan(large))

	t.Setenv("HNSW_PREFILL_TARGETED_READS", "false")
	require.False(t, h.useTargetedPrefillScan(large))
}
