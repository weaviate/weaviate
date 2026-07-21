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
	}
}

// TestPrefillCacheTargetedEndToEnd runs the targeted-read prefill against
// multi-segment named-vector data with superseded, deleted, and unindexed rows. The
// liveness filter must make the cache exactly match the live set — for both the
// large-schema tail path and the small-schema whole-read fallback.
func TestPrefillCacheTargetedEndToEnd(t *testing.T) {
	payloads := []struct {
		name    string
		payload int
	}{
		{"small schema, whole-read fallback", 1024},
		{"large schema, tail reads", 16 << 10},
	}

	for _, pl := range payloads {
		t.Run(pl.name, func(t *testing.T) {
			t.Setenv("HNSW_PREFILL_TARGETED_READS", "true")

			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)

			exp := map[uint64][]float32{}
			live := map[uint64]bool{}
			vec := func(id uint64) []float32 {
				return []float32{float32(id), float32(id) + 0.5, float32(id) * 2}
			}
			putLive := func(keyID, docID uint64) {
				putTargetedObject(t, bucket, keyID, docID, pl.payload, nil,
					map[string][]float32{"custom": vec(docID), "sibling": {9, 9}})
				exp[docID] = vec(docID)
				live[docID] = true
			}

			// segment 1: docs 0..29
			for i := uint64(0); i < 30; i++ {
				putLive(i, i)
			}
			require.NoError(t, bucket.FlushAndSwitch())

			// segment 2: key 5 updated under docID 100 (docID 5 is now dead but its row
			// survives in segment 1), key 7 deleted (docID 7 dead)
			putLive(5, 100)
			delete(exp, 5)
			delete(live, 5)
			require.NoError(t, bucket.Delete(keyForDocID(7)))
			delete(exp, 7)
			delete(live, 7)
			require.NoError(t, bucket.FlushAndSwitch())

			// memtable: docs 30..34, plus doc 40 indexed but lacking the target vector,
			// plus doc 50 present in the bucket but never indexed (no live node)
			for i := uint64(30); i < 35; i++ {
				putLive(i, i)
			}
			putTargetedObject(t, bucket, 40, 40, pl.payload, nil, map[string][]float32{"sibling": {1}})
			live[40] = true
			putTargetedObject(t, bucket, 50, 50, pl.payload, nil, map[string][]float32{"custom": {5, 5}})

			logger, _ := test.NewNullLogger()
			mustHit := func(_ context.Context, id uint64) ([]float32, error) {
				return nil, fmt.Errorf("unexpected cache miss for id %d", id)
			}
			c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
			c.Grow(101)

			h := newTargetedTestIndex(store, c, "vectors_custom", live, 101)
			require.NoError(t, h.prefillCacheParallel(context.Background()))

			requireCacheContains(t, c, exp)
		})
	}
}

// TestPrefillCacheTargetedLegacyVectors covers the legacy (unnamed) target: the
// vector sits in the value's front, served from the peek when it fits and via a
// bounded prefix read when it does not — with a properties payload large enough
// that a whole-value read would dwarf either.
func TestPrefillCacheTargetedLegacyVectors(t *testing.T) {
	t.Setenv("HNSW_PREFILL_TARGETED_READS", "true")

	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	exp := map[uint64][]float32{}
	live := map[uint64]bool{}
	put := func(id uint64, dims int) {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(id) + float32(j)*0.25
		}
		putTargetedObject(t, bucket, id, id, 16<<10, v, nil)
		exp[id] = v
		live[id] = true
	}

	// dims 3: vector fits in the 512B peek; dims 300: 44+4*300 > 512 forces the
	// bounded prefix read
	for i := uint64(0); i < 10; i++ {
		put(i, 3)
	}
	for i := uint64(10); i < 20; i++ {
		put(i, 300)
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(20)

	h := newTargetedTestIndex(store, c, "main", live, 20)
	require.NoError(t, h.prefillCacheParallel(context.Background()))

	requireCacheContains(t, c, exp)
}

// TestPrefillCacheTargetedMatchesCursorScan pins the equivalence of the two scan
// implementations on identical live data.
func TestPrefillCacheTargetedMatchesCursorScan(t *testing.T) {
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	const n = 200
	live := map[uint64]bool{}
	for i := uint64(0); i < n; i++ {
		putTargetedObject(t, bucket, i, i, 10<<10, nil,
			map[string][]float32{"custom": {float32(i), float32(i) + 1}})
		live[i] = true
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}

	run := func(env string) map[uint64][]float32 {
		t.Setenv("HNSW_PREFILL_TARGETED_READS", env)
		c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
		c.Grow(n)
		h := newTargetedTestIndex(store, c, "vectors_custom", live, n)
		require.NoError(t, h.prefillCacheParallel(context.Background()))
		out := map[uint64][]float32{}
		for i := uint64(0); i < n; i++ {
			v, err := c.Get(context.Background(), i)
			require.NoError(t, err)
			out[i] = v
		}
		return out
	}

	require.Equal(t, run("false"), run("true"))
}
