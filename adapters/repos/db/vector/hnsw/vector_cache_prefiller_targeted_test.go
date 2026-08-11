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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
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
	h := newPrefillTestIndex(id, store, c, nodesLen, distancer.NewDotProductProvider(), logger)
	for i := range h.nodes {
		if !liveNodes[uint64(i)] {
			h.nodes[i] = nil // never indexed
		}
	}
	return h
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
			// The baseline runs through prefillCacheParallel, which consults the
			// ambient flag: with it exported the large-schema cases would route the
			// baseline to the targeted scan too and compare it against itself.
			t.Setenv("HNSW_PREFILL_TARGETED_READS", "")

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
			collect := func(prefill func(h *hnsw) error) map[uint64][]float32 {
				c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
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
				require.False(t, h.useTargetedPrefillScan(bucket),
					"baseline must take the cursor scan or the comparison is vacuous")
				return h.prefillCacheParallel(context.Background())
			})
			viaTargeted := collect(func(h *hnsw) error {
				return prefillTargeted(t, h, tc.target)
			})
			require.Equal(t, viaCursor, viaTargeted)
		})
	}
}

// TestPrefillHNSWExclusions: a row whose doc is not indexed, and one whose node is
// HNSW-tombstoned while its bucket row is still live, must not be prefilled. Both
// scans are driven, because which nodes are worth caching is index policy — the read
// strategy must not change what ends up resident.
func TestPrefillHNSWExclusions(t *testing.T) {
	cases := []struct {
		name    string
		prefill func(t *testing.T, h *hnsw) error
	}{
		{"targeted scan", func(t *testing.T, h *hnsw) error {
			return prefillTargeted(t, h, "custom")
		}},
		{"cursor scan", func(t *testing.T, h *hnsw) error {
			t.Setenv("HNSW_PREFILL_TARGETED_READS", "")
			return h.prefillCacheParallel(context.Background())
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
			c.Grow(21)
			h := newTargetedTestIndex(store, c, "vectors_custom", live, 21)
			h.tombstones[5] = struct{}{}
			require.NoError(t, tc.prefill(t, h))

			requireCacheContains(t, c, exp)
		})
	}
}

// putMismatchedRow writes a row under one key while its object carries another's
// uuid. Only the targeted scan can detect this: it navigates by index offset and so
// must verify it landed on the row it asked for, where the cursor scan reads rows in
// order and never consults the key.
func putMismatchedRow(t *testing.T, bucket *lsmkv.Bucket, keyID, uuidID, docID uint64,
	payloadBytes int, named map[string][]float32,
) {
	t.Helper()
	obj := storobj.New(docID)
	obj.Object = models.Object{
		ID:         strfmt.UUID(testObjectUUID(uuidID)),
		Class:      "Test",
		Properties: map[string]interface{}{"filler": strings.Repeat("x", payloadBytes)},
	}
	obj.Vectors = named
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, bucket.Put(keyForDocID(keyID), data))
}

// TestPrefillCacheParallelRoutesToTargetedScan exercises the routing glue itself:
// every other targeted test calls the scan directly, so nothing covers the branch in
// prefillCacheParallel, the gate's polarity, or the target vector it forwards.
//
// The discriminator is the one divergence the read strategy legitimately owns — the
// key/uuid cross-check. The liveness filter is deliberately not usable here: it is
// index policy and both scans share it, which is what the excluded ids below assert.
func TestPrefillCacheParallelRoutesToTargetedScan(t *testing.T) {
	const (
		indexedCount = 10 // docs 0..9
		tombstonedID = 5
		unindexedID  = 20
		foreignKeyID = 21 // holds a row whose uuid says 22
		foreignDocID = 11 // indexed and live, so only the key check can exclude it
		nodesLen     = 23
	)

	cases := []struct {
		name     string
		flag     string
		targeted bool
	}{
		{"flag on routes to the targeted scan", "true", true},
		{"flag off stays on the cursor scan", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HNSW_PREFILL_TARGETED_READS", tc.flag)

			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)

			want := map[uint64][]float32{}
			live := map[uint64]bool{foreignDocID: true}
			for i := uint64(0); i < indexedCount; i++ {
				vec := []float32{float32(i), float32(i) + 0.5}
				putTargetedObject(t, bucket, i, i, 16<<10, nil, map[string][]float32{"custom": vec})
				want[i] = vec
				live[i] = true
			}
			// served by the bucket, excluded by both scans: never indexed, tombstoned
			putTargetedObject(t, bucket, unindexedID, unindexedID, 16<<10, nil,
				map[string][]float32{"custom": {5, 5}})
			delete(want, tombstonedID)

			foreignVec := []float32{7, 7}
			putMismatchedRow(t, bucket, foreignKeyID, foreignKeyID+1, foreignDocID, 16<<10,
				map[string][]float32{"custom": foreignVec})
			if !tc.targeted {
				want[foreignDocID] = foreignVec
			}
			// only flushed segments count towards EstimatedEntrySize, and the 16KB
			// payloads are what carry it past the gate's minimum
			require.NoError(t, bucket.FlushAndSwitch())

			logger, _ := test.NewNullLogger()
			c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
			c.Grow(nodesLen)
			// id "vectors_custom" makes getTargetVector yield "custom": a call site
			// forwarding the wrong target would decode no vector at all
			h := newTargetedTestIndex(store, c, "vectors_custom", live, nodesLen)
			h.tombstones[tombstonedID] = struct{}{}

			require.Equal(t, tc.targeted, h.useTargetedPrefillScan(bucket),
				"bucket does not put prefillCacheParallel on the expected path")
			require.NoError(t, h.prefillCacheParallel(context.Background()))

			requireCacheContains(t, c, want)
		})
	}
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

func TestPrefillStoppedByShutdown(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	tests := []struct {
		name string
		err  error
		ctx  context.Context
		want bool
	}{
		{"shutdown mid-prefill", context.Canceled, cancelled, true},
		{"worker failure reported as cancellation", context.Canceled, live, false},
		{"wrapped worker failure", fmt.Errorf("scan: %w", context.Canceled), live, false},
		{"real error during shutdown", errors.New("corrupt segment"), cancelled, false},
		{"real error", errors.New("corrupt segment"), live, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, prefillStoppedByShutdown(tt.err, tt.ctx))
		})
	}
}
