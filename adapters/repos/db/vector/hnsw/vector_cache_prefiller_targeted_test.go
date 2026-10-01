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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/storobj"
)

// putTargetedObject writes an object whose bucket key (keyID) is independent of its
// docID, so tests can model updates: the same key rewritten with a new docID, the
// old docID's row surviving in an older segment.
func putTargetedObject(t *testing.T, bucket *lsmkv.Bucket, keyID, docID uint64,
	payloadBytes int, legacyVec []float32, named map[string][]float32,
) {
	t.Helper()
	require.NoError(t, bucket.Put(keyForDocID(keyID),
		marshalTestObject(t, keyID, docID, payloadBytes, legacyVec, named)))
}

// targetedGatePayload clears prefillTargetedMinAvgEntrySize, so a test that means to
// exercise the targeted path is not silently routed to the cursor scan instead.
const targetedGatePayload = 160 << 10

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

// targetedScanCounts reads back how the scan served its rows. Asserting on the
// counters is the only way to tell the tail read from the whole-value fallback: both
// decode the same vector, so every contract test in this file passes with the tail
// branch disabled outright.
func targetedScanCounts(t *testing.T, hook *test.Hook) (tail, whole int64) {
	t.Helper()
	found := false
	for _, e := range hook.AllEntries() {
		if e.Message != "targeted vector cache prefill scan finished" {
			continue
		}
		found = true
		tail += e.Data["tail_reads"].(int64)
		whole += e.Data["whole_value_fallbacks"].(int64)
	}
	require.True(t, found, "the targeted scan did not report how it served its rows")
	return tail, whole
}

// TestTargetedScanTakesTheTailRead: the two-read path is the entire feature, so a scan
// that quietly fell back to whole-value reads on every row has to fail. On a bucket
// past both gates every row must take the tail.
func TestTargetedScanTakesTheTailRead(t *testing.T) {
	const n = 40
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTargetedObject(t, bucket, i, i, targetedGatePayload, nil,
			map[string][]float32{"custom": {float32(i), 1}})
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, hook := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)
	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)

	require.NoError(t, prefillTargeted(t, h, "custom"))
	require.EqualValues(t, n, c.CountVectors())

	tail, whole := targetedScanCounts(t, hook)
	assert.EqualValues(t, n, tail, "every row past both gates must take the tail read")
	assert.Zero(t, whole, "a whole-value fallback here reads the same bytes as the "+
		"cursor scan and pays for the peek on top")
}

// TestTargetedScanCountsTheWholeValueFallback is the other half: a schema under the
// tail gate must be reported as a fallback rather than pass as a tail read, or the
// counter cannot distinguish the two paths it exists to distinguish.
func TestTargetedScanCountsTheWholeValueFallback(t *testing.T) {
	const n = 20
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTargetedObject(t, bucket, i, i, 8, nil, // schema far below prefillTargetedMinSchemaLen
			map[string][]float32{"custom": {float32(i), 1}})
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, hook := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)
	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)

	require.NoError(t, prefillTargeted(t, h, "custom"))
	require.EqualValues(t, n, c.CountVectors())

	tail, whole := targetedScanCounts(t, hook)
	assert.Zero(t, tail)
	assert.EqualValues(t, n, whole)
}

// TestTailReadsFireMajorityBoundary pins the strict majority the probe applies. A
// bucket that is half legacy-shaped is not one the targeted path can serve, so 8 of 16
// must be rejected and 9 of 16 admitted; with every existing case all-fire or no-fire,
// turning <= into < would move routing silently.
func TestTailReadsFireMajorityBoundary(t *testing.T) {
	// a legacy vector this wide pushes the schema length past the peek, so those rows
	// cannot resolve a tail; the rest can
	legacy := make([]float32, 200)

	for _, tc := range []struct {
		name   string
		firing int
		want   bool
	}{
		{"8 of 16 is not a majority", 8, false},
		{"9 of 16 is", 9, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)
			for i := 0; i < prefillTailProbeRows; i++ {
				var vec []float32
				if i >= tc.firing {
					vec = legacy
				}
				putTargetedObject(t, bucket, uint64(i), uint64(i), targetedGatePayload, vec,
					map[string][]float32{"custom": {1, 2}})
			}
			require.NoError(t, bucket.FlushAndSwitch())

			h := newTargetedTestIndex(store, nil, "vectors_custom", nil, 0)
			require.Equal(t, tc.want, h.tailReadsFire(context.Background(), bucket))
		})
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
			// The baseline runs through prefillCacheParallel, which consults the
			// ambient flag: with it exported the large-schema cases would route the
			// baseline to the targeted scan too and compare it against itself.
			t.Setenv(prefillTargetedReadsEnv, "")

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
				require.False(t, h.useTargetedPrefillScan(context.Background(), bucket),
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
			t.Setenv(prefillTargetedReadsEnv, "")
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
				putTargetedObject(t, bucket, i, i, targetedGatePayload, nil, map[string][]float32{"custom": vec})
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
	require.NoError(t, bucket.Put(keyForDocID(keyID),
		marshalTestObject(t, uuidID, docID, payloadBytes, nil, named)))
}

// TestPrefillCacheParallelRoutesToTargetedScan drives the branch in prefillCacheParallel.
// Every other targeted test calls the scan directly, so nothing else covers that branch,
// that the flag being off keeps the cursor scan, or that the target vector is forwarded.
// The discriminator is the key/uuid check, the one divergence a read strategy owns; the
// liveness filter is shared, as the excluded ids below assert.
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
			t.Setenv(prefillTargetedReadsEnv, tc.flag)

			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)

			want := map[uint64][]float32{}
			live := map[uint64]bool{foreignDocID: true}
			for i := uint64(0); i < indexedCount; i++ {
				vec := []float32{float32(i), float32(i) + 0.5}
				putTargetedObject(t, bucket, i, i, targetedGatePayload, nil, map[string][]float32{"custom": vec})
				want[i] = vec
				live[i] = true
			}
			// served by the bucket, excluded by both scans: never indexed, tombstoned
			putTargetedObject(t, bucket, unindexedID, unindexedID, targetedGatePayload, nil,
				map[string][]float32{"custom": {5, 5}})
			delete(want, tombstonedID)

			foreignVec := []float32{7, 7}
			putMismatchedRow(t, bucket, foreignKeyID, foreignKeyID+1, foreignDocID, targetedGatePayload,
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

			require.Equal(t, tc.targeted, h.useTargetedPrefillScan(context.Background(), bucket),
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
	large := build(20, 160<<10)

	h := newTargetedTestIndex(nil, nil, "vectors_custom", nil, 0)

	t.Setenv(prefillTargetedReadsEnv, "true")
	require.False(t, h.useTargetedPrefillScan(context.Background(), small))
	require.True(t, h.useTargetedPrefillScan(context.Background(), large))

	t.Setenv(prefillTargetedReadsEnv, "false")
	require.False(t, h.useTargetedPrefillScan(context.Background(), large))
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

// TestTargetedGateAdmitsOnlyBucketsThatTakeTailReads sweeps schema sizes across
// prefillTargetedMinAvgEntrySize and prefillTargetedMinSchemaLen and requires the
// implication to hold: a bucket the gate admits must have rows that resolve a tail.
// See prefillTargetedMinAvgEntrySize for why admitting a bucket that cannot is the
// expensive direction.
func TestTargetedGateAdmitsOnlyBucketsThatTakeTailReads(t *testing.T) {
	t.Setenv(prefillTargetedReadsEnv, "true")

	// otherwise a gate that admits nothing would satisfy the implication vacuously
	admitted := 0
	t.Cleanup(func() {
		require.NotZero(t, admitted, "no payload cleared the gate; the sweep proved nothing")
	})

	for _, payload := range []int{100, 5000, 9000, 16 << 10, 64 << 10, 160 << 10, 256 << 10} {
		t.Run(fmt.Sprintf("payload=%d", payload), func(t *testing.T) {
			store := newTestObjectsStore(t)
			bucket := store.Bucket(helpers.ObjectsBucketLSM)
			const rows = 200
			for i := uint64(0); i < rows; i++ {
				putTargetedObject(t, bucket, i, i, payload, nil,
					map[string][]float32{"custom": {1, 2}})
			}
			require.NoError(t, bucket.FlushAndSwitch())

			h := newTargetedTestIndex(store, nil, "vectors_custom", nil, 0)
			if !h.useTargetedPrefillScan(context.Background(), bucket) {
				return // not admitted: the cursor scan handles it, nothing to prove
			}
			admitted++

			tailReads := 0
			c := bucket.CursorReplaceReusable()
			defer c.Close()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				peek := v[:min(prefillPeekBytes, len(v))]
				_, schemaLen, ok, err := storobj.VectorTailOffsetFromPrefix(peek)
				require.NoError(t, err)
				if ok && schemaLen >= prefillTargetedMinSchemaLen {
					tailReads++
				}
			}

			require.Equal(t, rows, tailReads,
				"bucket admitted by the %d-byte gate, but %d/%d rows fall back to a whole-value read",
				prefillTargetedMinAvgEntrySize, rows-tailReads, rows)
		})
	}
}

// TestTargetedGateRejectsUnreachableTail: a collection carrying a legacy vector next
// to its named ones pushes the schema-length field past the 512-byte peek once the
// legacy vector is around 116 dimensions, so no row can resolve a tail. The targeted
// scan would then read a peek and the whole value for every row — strictly more work
// than the cursor scan, at any row size — so the gate has to refuse it.
func TestTargetedGateRejectsUnreachableTail(t *testing.T) {
	t.Setenv(prefillTargetedReadsEnv, "true")

	build := func(legacyDims int) *lsmkv.Bucket {
		store := newTestObjectsStore(t)
		bucket := store.Bucket(helpers.ObjectsBucketLSM)
		legacy := make([]float32, legacyDims)
		for i := uint64(0); i < 20; i++ {
			putTargetedObject(t, bucket, i, i, targetedGatePayload, legacy,
				map[string][]float32{"custom": {1, 2}})
		}
		require.NoError(t, bucket.FlushAndSwitch())
		return bucket
	}

	h := newTargetedTestIndex(nil, nil, "vectors_custom", nil, 0)
	require.True(t, h.useTargetedPrefillScan(context.Background(), build(0)),
		"named vectors with no legacy vector resolve a tail and should be admitted")
	require.False(t, h.useTargetedPrefillScan(context.Background(), build(200)),
		"a 200-dim legacy vector puts the schema length past the peek; no row can take the tail read")

	// the legacy target itself is unaffected: it reads a bounded front prefix, never a tail
	legacyIdx := newTargetedTestIndex(nil, nil, "main", nil, 0)
	require.True(t, legacyIdx.useTargetedPrefillScan(context.Background(), build(200)))
}
