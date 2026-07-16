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

package lsmkv

// Correctness tests for the batched secondary-key resolver (gh#309). The primary
// gate is a differential-equality oracle: GetBySecondaryBatch(keys) must be
// byte-identical, positionally, to [GetBySecondary(k) for k in keys] over a frozen
// bucket, since the per-key path is the shipped, assumed-correct resolver. Seeded
// fixtures pin the rare newest-wins shapes that random generation would miss
// (tombstone-over-live, flushing-memtable re-add, forced bloom false positive,
// phase-1-elimination-crossed-with-phase-3-recheck). A positional-alignment test
// pins that results stay index-aligned to input keys at the bucket layer (the layer
// that owns permutation restore); a read-op-neutrality test records before/after counts.

import (
	"bytes"
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// secondaryValueMode selects how phase-2 value reads hit the segment: pread (the
// production LTK shape, WithPread(true)) or in-memory/mmap (WithPread(false), which
// sets readFromMemory so value reads come from the mapped region). Both are
// permanent members of the batch differential matrix so a regression in either
// value-read path is caught, not just the one a manual flip happened to exercise.
type secondaryValueMode struct {
	name  string
	pread bool
}

var secondaryValueModes = []secondaryValueMode{
	{name: "pread", pread: true},
	{name: "mmap", pread: false},
}

func newSecondaryBatchTestBucketMode(t testing.TB, useBloom, pread bool, extra ...BucketOption) *Bucket {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	opts := []BucketOption{
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithPread(pread),
		WithMinMMapSize(0), // any non-empty segment maps; pread flag then picks the read path
		WithUseBloomFilter(useBloom),
		WithDisableCompaction(true),
	}
	opts = append(opts, extra...)
	b, err := NewBucketCreator().NewBucket(
		ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		opts...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	return b
}

// newSecondaryBatchTestBucket keeps the pread (production LTK) value path; the mmap
// path is exercised via newSecondaryBatchTestBucketMode in the differential matrix.
func newSecondaryBatchTestBucket(t testing.TB, useBloom bool) *Bucket {
	return newSecondaryBatchTestBucketMode(t, useBloom, true)
}

// forceFlushingMemtable moves the current active memtable into the flushing slot
// (installing a fresh active), so a resolution view observes active + flushing +
// segments at once. Because the test cycle managers are no-ops, nothing drains the
// flushing memtable in the background; this registers a cleanup (LIFO, so it runs
// before the bucket's own Shutdown cleanup) that flushes it to a segment, otherwise
// Shutdown blocks forever waiting for b.flushing to clear. Requires a non-empty
// active memtable (the switch is a no-op on an empty one).
func forceFlushingMemtable(t testing.TB, b *Bucket) {
	t.Helper()
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched, "expected a non-empty active memtable to switch into flushing")
	t.Cleanup(func() {
		if b.flushing == nil {
			return
		}
		b.waitForZeroWriters(b.flushing)
		segmentPath, err := b.flushing.flush()
		require.NoError(t, err)
		seg, err := b.disk.initAndPrecomputeNewSegment(segmentPath)
		require.NoError(t, err)
		require.NoError(t, b.atomicallyAddDiskSegmentAndRemoveFlushing(seg))
	})
}

// resolveViaLoop is the differential oracle: the shipped per-key path, run once per
// input key, positionally aligned. GetBySecondaryWithBuffer already maps
// deleted/not-found to a nil value.
func resolveViaLoop(t testing.TB, b *Bucket, pos int, keys [][]byte) [][]byte {
	t.Helper()
	ctx := context.Background()
	out := make([][]byte, len(keys))
	for i, k := range keys {
		v, _, err := b.GetBySecondaryWithBuffer(ctx, pos, k, nil)
		require.NoError(t, err)
		out[i] = v
	}
	return out
}

func requireBatchEqualsLoop(t testing.TB, b *Bucket, pos int, keys [][]byte) [][]byte {
	t.Helper()
	ctx := context.Background()
	want := resolveViaLoop(t, b, pos, keys)
	got, err := b.GetBySecondaryBatch(ctx, pos, keys)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	for i := range keys {
		require.Equalf(t, want[i], got[i],
			"batch vs per-key-loop divergence at position %d (key %x)", i, keys[i])
	}
	return got
}

// TestBucketGetBySecondaryBatchDifferential is the drift guard: a frozen bucket with
// randomized STATE (multi-segment updates, tombstones, primary keys re-added under a
// NEW secondary key so phase-3 recheck is reached, a flushing memtable, an active
// memtable), resolved through the batch path and the per-key loop, asserting exact
// positional equality over a mixed key set (present, absent, duplicate, interior-nil).
// No writes happen inside the equality window. Run across the full matrix of value
// mode (pread / mmap) x bloom (off / on) so neither the mmap value path nor the
// bloom-on path is left uncovered.
func TestBucketGetBySecondaryBatchDifferential(t *testing.T) {
	for _, mode := range secondaryValueModes {
		for _, useBloom := range []bool{false, true} {
			mode, useBloom := mode, useBloom
			bloomName := "bloom-off"
			if useBloom {
				bloomName = "bloom-on"
			}
			t.Run(mode.name+"/"+bloomName, func(t *testing.T) {
				for _, seed := range []int64{1, 7, 42, 1337, 90210} {
					seed := seed
					t.Run("seed", func(t *testing.T) {
						b, universe := buildRandomizedSecondaryBucket(t, seed, useBloom, mode.pread)
						rng := rand.New(rand.NewSource(seed * 31))

						// Mixed batch: real docIDs (possibly resolving to nil if deleted /
						// updated away / superseded via phase-3), absent docIDs (guaranteed
						// nil), duplicates, in shuffled order.
						keys := make([][]byte, 0, len(universe)+40)
						for _, d := range universe {
							keys = append(keys, encodeDocID(d))
						}
						for i := 0; i < 20; i++ { // absent keys -> interior nils
							keys = append(keys, encodeDocID(1_000_000+uint64(i)))
						}
						for i := 0; i < 20; i++ { // duplicates of real keys
							keys = append(keys, encodeDocID(universe[rng.Intn(len(universe))]))
						}
						rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

						requireBatchEqualsLoop(t, b, secondaryPos, keys)
					})
				}
			})
		}
	}
}

// phase-3 recheck seeding constants: a fixed prefix of the docID universe whose
// primary keys are re-added under a DIFFERENT secondary key in a newer segment, so
// phase-3 SEGMENT recheck is deterministically exercised for every seed (see the
// seeding block below). Kept out of the later memtable rounds so the newer primary
// version stays in a segment, not a memtable.
const (
	p3RecheckSubset = 60
	p3NewSecKeyBase = 9_000_000 // well clear of the differential's absent/dup key ranges
)

// buildRandomizedSecondaryBucket writes `segments` flushed segments of overlapping
// docIDs (so newer segments shadow older ones), sprinkles tombstones, re-adds a fixed
// subset of primary keys under a NEW secondary key in a newer segment (so phase-3
// recheck is reachable), leaves the last batch in the active memtable, and forces a
// flushing memtable to exercise the flushing path. bloom optionally on; pread selects
// the value-read mode.
func buildRandomizedSecondaryBucket(t testing.TB, seed int64, useBloom, pread bool) (*Bucket, []uint64) {
	t.Helper()
	b := newSecondaryBatchTestBucketMode(t, useBloom, pread)
	rng := rand.New(rand.NewSource(seed))

	const universeSize = 300
	universe := make([]uint64, universeSize)
	for i := range universe {
		universe[i] = uint64(i)
	}

	putVersion := func(d uint64, version int) {
		val := make([]byte, 24)
		rng.Read(val)
		val[0] = byte(version) // make the value observably version-specific
		require.NoError(t, b.Put(encodePrimaryKey(d), val, WithSecondaryKey(secondaryPos, encodeDocID(d))))
	}

	const segments = 5
	for s := 0; s < segments; s++ {
		for _, d := range universe {
			switch r := rng.Intn(100); {
			case r < 45:
				putVersion(d, s)
			case r < 55:
				require.NoError(t, b.Delete(encodePrimaryKey(d), WithSecondaryKey(secondaryPos, encodeDocID(d))))
			default:
				// leave untouched this round
			}
		}
		require.NoError(t, b.FlushAndSwitch())
	}

	// Phase-3 recheck seeding. For a fixed subset, first establish a live hit under the
	// doc's ORIGINAL secondary key in one segment, then re-add the SAME primary key
	// under a DIFFERENT secondary key in a newer segment. Querying the original
	// secondary key now finds an older secondary hit whose primary has a newer version
	// (under a different secondary key) in a newer segment, so the batch must run its
	// phase-3 SEGMENT recheck to suppress it (result nil), exactly as the per-key path
	// does. Without this the generator never re-keys a primary and the segment recheck
	// is dead code under the fuzzer.
	for _, d := range universe[:p3RecheckSubset] {
		putVersion(d, segments) // original secondary key, live, newest so far
	}
	require.NoError(t, b.FlushAndSwitch())
	for _, d := range universe[:p3RecheckSubset] {
		val := make([]byte, 24)
		rng.Read(val)
		require.NoError(t, b.Put(encodePrimaryKey(d),
			val, WithSecondaryKey(secondaryPos, encodeDocID(d+p3NewSecKeyBase))))
	}
	require.NoError(t, b.FlushAndSwitch())

	// Write one more round into the active memtable (guaranteeing at least one put so
	// the switch is not a no-op), then force a flushing memtable and write a final
	// round into the new active, so a resolution view sees active + flushing +
	// segments simultaneously. The phase-3 subset is excluded from these rounds so its
	// newest primary version stays in a segment (segment recheck, not memtable recheck).
	putVersion(universe[p3RecheckSubset+rng.Intn(len(universe)-p3RecheckSubset)], segments)
	for _, d := range universe[p3RecheckSubset:] {
		if rng.Intn(100) < 30 {
			putVersion(d, segments)
		}
	}
	forceFlushingMemtable(t, b)
	for _, d := range universe[p3RecheckSubset:] {
		if rng.Intn(100) < 25 {
			putVersion(d, segments+1)
		}
	}

	view := b.GetConsistentView()
	require.NotNil(t, view.Flushing, "test setup must leave a flushing memtable present")
	view.ReleaseView()

	return b, universe
}

// TestBucketGetBySecondaryBatchTombstoneOverLiveOlder pins the single most
// load-bearing newest-wins case: the newest segment holds a TOMBSTONE for a key
// whose live value lives in an older segment. Deleted wins; the older segment is
// never consulted; the result is nil. A co-resident second key (kept live in both
// segments) makes this a genuine n>=2 batch so the batch phases run rather than the
// n==1 single-key delegate.
func TestBucketGetBySecondaryBatchTombstoneOverLiveOlder(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

	d := uint64(7)
	sec := encodeDocID(d)
	pri := encodePrimaryKey(d)

	// co-resident second key: a genuinely-live docID resolved alongside d, so the
	// batch carries >1 key and does not short-circuit to the serial single-key path.
	d2 := uint64(8)
	sec2 := encodeDocID(d2)
	pri2 := encodePrimaryKey(d2)

	// older segment: live values for both
	require.NoError(t, b.Put(pri, []byte("live-old"), WithSecondaryKey(secondaryPos, sec)))
	require.NoError(t, b.Put(pri2, []byte("live2-old"), WithSecondaryKey(secondaryPos, sec2)))
	require.NoError(t, b.FlushAndSwitch())
	// newer segment: tombstone for d, fresh live value for d2
	require.NoError(t, b.Delete(pri, WithSecondaryKey(secondaryPos, sec)))
	require.NoError(t, b.Put(pri2, []byte("live2-new"), WithSecondaryKey(secondaryPos, sec2)))
	require.NoError(t, b.FlushAndSwitch())

	got, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{sec, sec2})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Nil(t, got[0], "tombstone in the newer segment must win over the live older value")
	require.Equal(t, []byte("live2-new"), got[1], "co-resident live key must resolve to its newest value")

	requireBatchEqualsLoop(t, b, secondaryPos, [][]byte{sec, sec2})
}

// TestBucketGetBySecondaryBatchFlushingMemtableReAdd pins all three outcomes of the
// flushing-memtable branch: a secondary hit in the flushing memtable is only
// authoritative if the active memtable has no newer primary-key state.
func TestBucketGetBySecondaryBatchFlushingMemtableReAdd(t *testing.T) {
	ctx := context.Background()

	type outcome struct {
		name        string
		setupActive func(b *Bucket, pri, sec []byte)
		wantNil     bool
	}
	outcomes := []outcome{
		{
			name:        "no-newer-active-version->flushing value wins",
			setupActive: func(b *Bucket, pri, sec []byte) {},
			wantNil:     false,
		},
		{
			name: "newer-active-version-different-secondary->not-found",
			setupActive: func(b *Bucket, pri, sec []byte) {
				// re-add same PRIMARY key with a DIFFERENT secondary key: active hides the
				// old version; active.getBySecondary(sec) misses but active.exists(pri)==nil.
				require.NoError(t, b.Put(pri, []byte("v2"), WithSecondaryKey(secondaryPos, encodeDocID(999999))))
			},
			wantNil: true,
		},
		{
			name: "active-tombstone->deleted",
			setupActive: func(b *Bucket, pri, sec []byte) {
				// tombstone the same PRIMARY key under a DIFFERENT secondary key, so the
				// active getBySecondary(sec) misses at i=0 and the flushing branch's
				// exists(pri) sees the tombstone (the i==1 exists==Deleted outcome).
				require.NoError(t, b.Delete(pri, WithSecondaryKey(secondaryPos, encodeDocID(888888))))
			},
			wantNil: true,
		},
	}

	for _, oc := range outcomes {
		oc := oc
		t.Run(oc.name, func(t *testing.T) {
			b := newSecondaryBatchTestBucket(t, false)
			d := uint64(42)
			sec := encodeDocID(d)
			pri := encodePrimaryKey(d)

			// co-resident second key living in a flushed segment, so the batch carries
			// >1 key (exercising phase 1 + phase 2) and does not short-circuit to the
			// serial single-key delegate while still pinning the flushing-memtable branch.
			d2 := uint64(43)
			sec2 := encodeDocID(d2)
			require.NoError(t, b.Put(encodePrimaryKey(d2), []byte("seg-live-43"), WithSecondaryKey(secondaryPos, sec2)))
			require.NoError(t, b.FlushAndSwitch())

			// put d into active, then switch it into the flushing slot
			require.NoError(t, b.Put(pri, []byte("flushing-val"), WithSecondaryKey(secondaryPos, sec)))
			forceFlushingMemtable(t, b)

			oc.setupActive(b, pri, sec)

			got, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{sec, sec2})
			require.NoError(t, err)
			require.Len(t, got, 2)
			if oc.wantNil {
				require.Nil(t, got[0])
			} else {
				require.Equal(t, []byte("flushing-val"), got[0])
			}
			require.Equal(t, []byte("seg-live-43"), got[1], "co-resident segment key must resolve")
			// and it must match the shipped per-key path exactly
			requireBatchEqualsLoop(t, b, secondaryPos, [][]byte{sec, sec2})
		})
	}
}

// TestBucketGetBySecondaryBatchPhase1BloomFPNoEvict proves the phase-1
// confirmed-hit-only elimination rule: a bloom false positive on the NEWEST segment
// (bloom passes, index Get misses) must NOT remove the key from the phase-1 unresolved
// set; the older segment's live value must still resolve. A "remove on bloom pass" bug
// would silently resolve the key to nil. This exercises the phase-1 index descent
// (getBySecondaryBatchIndexHits), NOT the phase-3 recheck bloom-positive path.
//
// This is the finding-1 mutation proof: it uses a co-resident second key so n>=2 and
// the batch phase-1 descent runs (the single-key form short-circuits to the serial
// delegate, which never touches getBySecondaryBatchIndexHits, so the premature-
// elimination bug (dropping a key on a NotFound instead of keeping it) is invisible
// to it). With that bug injected into getBySecondaryBatchIndexHits, this test goes RED;
// the single-key form stays GREEN, which is exactly the coverage hole being closed.
func TestBucketGetBySecondaryBatchPhase1BloomFPNoEvict(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, true) // bloom ON

	d := uint64(11)
	sec := encodeDocID(d)
	pri := encodePrimaryKey(d)

	// co-resident second key, also genuinely living only in the older segment, so the
	// batch carries >1 key and runs the batch index descent.
	d2 := uint64(13)
	sec2 := encodeDocID(d2)

	// older segment: the ONLY place the keys genuinely live
	require.NoError(t, b.Put(pri, []byte("older-live"), WithSecondaryKey(secondaryPos, sec)))
	require.NoError(t, b.Put(encodePrimaryKey(d2), []byte("older-live-13"), WithSecondaryKey(secondaryPos, sec2)))
	require.NoError(t, b.FlushAndSwitch())
	// newest segment: a different key, so both `sec` and `sec2` are absent from its index
	require.NoError(t, b.Put(encodePrimaryKey(12), []byte("other"), WithSecondaryKey(secondaryPos, encodeDocID(12))))
	require.NoError(t, b.FlushAndSwitch())

	// Force a bloom false positive on the NEWEST segment for `sec`: add `sec` to its
	// secondary bloom filter without it being in the index. Now bloom.Test(sec)==true
	// but the index Get returns NotFound.
	view := b.GetConsistentView()
	require.GreaterOrEqual(t, len(view.Disk), 2)
	newest := view.Disk[len(view.Disk)-1].(*segment) // highest index = newest
	require.True(t, newest.useBloomFilter)
	require.False(t, newest.secondaryBloomFilters[secondaryPos].Test(sec),
		"precondition: sec must not already pass the newest segment's bloom")
	newest.secondaryBloomFilters[secondaryPos].Add(sec)
	require.True(t, newest.secondaryBloomFilters[secondaryPos].Test(sec),
		"forced bloom false positive did not take")
	view.ReleaseView()

	got, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{sec, sec2})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, []byte("older-live"), got[0],
		"a bloom false positive on the newest segment must not shadow the live older value")
	require.Equal(t, []byte("older-live-13"), got[1], "co-resident key must resolve to its older-segment value")

	requireBatchEqualsLoop(t, b, secondaryPos, [][]byte{sec, sec2})
}

// TestBucketGetBySecondaryBatchBloomOnMultiKeyForcedFP is the finding-2 coverage:
// bloom-on batch behavior with a MULTI-key differential and a forced false positive.
// Several co-resident keys live only in an older segment; a bloom false positive is
// forced on the newest segment for each, so the batch's phase-1 sees bloom pass +
// index-Get NotFound and must keep every key unresolved (confirmed-hit-only), letting
// the older segment resolve them. Asserted as batch == per-key loop. Under the
// premature-elimination bug (drop on NotFound) every forced-FP key resolves to nil in
// the batch while the loop resolves the older-live value -> the differential goes RED.
func TestBucketGetBySecondaryBatchBloomOnMultiKeyForcedFP(t *testing.T) {
	b := newSecondaryBatchTestBucket(t, true) // bloom ON

	// keys that live ONLY in the older segment
	fpDocs := []uint64{10, 11, 12, 13, 14}
	for _, d := range fpDocs {
		require.NoError(t, b.Put(encodePrimaryKey(d),
			[]byte{byte(d), 0xAB}, WithSecondaryKey(secondaryPos, encodeDocID(d))))
	}
	require.NoError(t, b.FlushAndSwitch())
	// newest segment: unrelated live keys, so fpDocs are absent from its index
	for _, d := range []uint64{20, 21, 22} {
		require.NoError(t, b.Put(encodePrimaryKey(d),
			[]byte{byte(d), 0xCD}, WithSecondaryKey(secondaryPos, encodeDocID(d))))
	}
	require.NoError(t, b.FlushAndSwitch())

	// Force a bloom false positive on the NEWEST segment for every fpDoc secondary key.
	view := b.GetConsistentView()
	require.GreaterOrEqual(t, len(view.Disk), 2)
	newest := view.Disk[len(view.Disk)-1].(*segment)
	require.True(t, newest.useBloomFilter)
	for _, d := range fpDocs {
		newest.secondaryBloomFilters[secondaryPos].Add(encodeDocID(d))
		require.True(t, newest.secondaryBloomFilters[secondaryPos].Test(encodeDocID(d)),
			"forced bloom false positive did not take for docID %d", d)
	}
	view.ReleaseView()

	// mixed multi-key batch: forced-FP keys, a live newest-segment key, an absent key.
	keys := [][]byte{
		encodeDocID(10), encodeDocID(20), encodeDocID(11),
		encodeDocID(9_999_999 /*absent*/), encodeDocID(12), encodeDocID(13), encodeDocID(14),
	}
	got := requireBatchEqualsLoop(t, b, secondaryPos, keys)
	require.Equal(t, []byte{10, 0xAB}, got[0], "forced-FP key 10 must resolve to its older-live value, not nil")
	require.Equal(t, []byte{20, 0xCD}, got[1])
	require.Nil(t, got[3], "absent key stays nil")
}

// TestBucketGetBySecondaryBatchCrossMechanismPhase1xPhase3 pins the phase-1
// elimination x phase-3 recheck interaction (the loop-nesting inversion bug site):
// key1 secondary-hit at seg 2 with a newer PRIMARY version at seg 5; key2
// secondary-hit at seg 4 with a newer primary at seg 5; key3 hit at seg 5 with no
// newer version. Expect key1 nil, key2 nil, key3 live.
func TestBucketGetBySecondaryBatchCrossMechanismPhase1xPhase3(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

	k1, k2, k3 := uint64(101), uint64(102), uint64(103)
	sec := func(d uint64) []byte { return encodeDocID(d) }
	pri := func(d uint64) []byte { return encodePrimaryKey(d) }

	// Build 6 segments (0..5). Segment index in view.Disk grows newest; here segment N
	// is flushed Nth so higher flush order = newer.
	// seg0: nothing special
	require.NoError(t, b.Put(pri(900), []byte("f0"), WithSecondaryKey(secondaryPos, sec(900))))
	require.NoError(t, b.FlushAndSwitch())
	// seg1
	require.NoError(t, b.Put(pri(901), []byte("f1"), WithSecondaryKey(secondaryPos, sec(901))))
	require.NoError(t, b.FlushAndSwitch())
	// seg2: key1 lives here (secondary hit at seg2)
	require.NoError(t, b.Put(pri(k1), []byte("k1-old"), WithSecondaryKey(secondaryPos, sec(k1))))
	require.NoError(t, b.FlushAndSwitch())
	// seg3
	require.NoError(t, b.Put(pri(902), []byte("f3"), WithSecondaryKey(secondaryPos, sec(902))))
	require.NoError(t, b.FlushAndSwitch())
	// seg4: key2 secondary hit here
	require.NoError(t, b.Put(pri(k2), []byte("k2-old"), WithSecondaryKey(secondaryPos, sec(k2))))
	require.NoError(t, b.FlushAndSwitch())
	// seg5 (newest): key3 lives with no newer version; also newer PRIMARY versions for
	// key1 and key2 under DIFFERENT secondary keys (so their secondary hit stays at the
	// older segment, but the recheck finds the newer primary -> stale).
	require.NoError(t, b.Put(pri(k3), []byte("k3-live"), WithSecondaryKey(secondaryPos, sec(k3))))
	require.NoError(t, b.Put(pri(k1), []byte("k1-new"), WithSecondaryKey(secondaryPos, sec(700001))))
	require.NoError(t, b.Put(pri(k2), []byte("k2-new"), WithSecondaryKey(secondaryPos, sec(700002))))
	require.NoError(t, b.FlushAndSwitch())

	keys := [][]byte{sec(k1), sec(k2), sec(k3)}
	got, err := b.GetBySecondaryBatch(ctx, secondaryPos, keys)
	require.NoError(t, err)
	require.Nil(t, got[0], "key1 superseded by newer primary at seg5")
	require.Nil(t, got[1], "key2 superseded by newer primary at seg5")
	require.Equal(t, []byte("k3-live"), got[2], "key3 has no newer version")

	requireBatchEqualsLoop(t, b, secondaryPos, keys)
}

// TestBucketGetBySecondaryBatchDeletedReAddedAcrossSegmentBoundary covers the
// deleted+re-added-across-a-segment-boundary shape at the segment level. A co-resident
// second key makes this a genuine n>=2 batch (not the serial single-key delegate).
func TestBucketGetBySecondaryBatchDeletedReAddedAcrossSegmentBoundary(t *testing.T) {
	b := newSecondaryBatchTestBucket(t, false)
	d := uint64(55)
	sec := encodeDocID(d)
	pri := encodePrimaryKey(d)

	d2 := uint64(56)
	sec2 := encodeDocID(d2)
	pri2 := encodePrimaryKey(d2)

	require.NoError(t, b.Put(pri, []byte("v1"), WithSecondaryKey(secondaryPos, sec)))
	require.NoError(t, b.Put(pri2, []byte("c56-1"), WithSecondaryKey(secondaryPos, sec2)))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Delete(pri, WithSecondaryKey(secondaryPos, sec)))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Put(pri, []byte("v3"), WithSecondaryKey(secondaryPos, sec)))
	require.NoError(t, b.Put(pri2, []byte("c56-3"), WithSecondaryKey(secondaryPos, sec2)))
	require.NoError(t, b.FlushAndSwitch())

	got := requireBatchEqualsLoop(t, b, secondaryPos, [][]byte{sec, sec2})
	require.Equal(t, []byte("v3"), got[0], "the newest re-add must win")
	require.Equal(t, []byte("c56-3"), got[1], "co-resident key must resolve to its newest value")
}

// TestBucketGetBySecondaryBatchPositionalAlignment pins that results are index-
// aligned to input keys including duplicate keys, interior nils, and a random
// permutation of the internal sort order; and that results remain valid AFTER the
// view is released (copy-under-refcount) and after subsequent compaction-eligible
// writes. The bucket layer owns permutation restore, so this is tested at that layer.
func TestBucketGetBySecondaryBatchPositionalAlignment(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)

	// live keys with distinct values across two segments; some docIDs never written.
	live := map[uint64][]byte{}
	for _, d := range []uint64{3, 1, 4, 1_5, 9, 2, 6} {
		val := []byte{byte(d), byte(d >> 8), 0xAB}
		require.NoError(t, b.Put(encodePrimaryKey(d), val, WithSecondaryKey(secondaryPos, encodeDocID(d))))
		live[d] = val
		if d%2 == 0 {
			require.NoError(t, b.FlushAndSwitch())
		}
	}
	require.NoError(t, b.FlushAndSwitch())

	// Build an input with duplicates, interior missing keys, and deliberately NOT in
	// sorted order so the internal sort must be undone.
	inputDocs := []uint64{9, 999 /*absent*/, 1, 1 /*dup*/, 2, 424242 /*absent*/, 6, 3, 4}
	keys := make([][]byte, len(inputDocs))
	for i, d := range inputDocs {
		keys[i] = encodeDocID(d)
	}

	got, err := b.GetBySecondaryBatch(ctx, secondaryPos, keys)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	for i, d := range inputDocs {
		if want, ok := live[d]; ok {
			require.Equalf(t, want, got[i], "position %d docID %d", i, d)
		} else {
			require.Nilf(t, got[i], "position %d docID %d must be nil (absent)", i, d)
		}
	}

	// results remain valid after view release + subsequent writes/flushes (they were
	// copied out; they must not alias freed/compacted segment memory).
	snapshot := make([][]byte, len(got))
	for i := range got {
		if got[i] != nil {
			snapshot[i] = append([]byte(nil), got[i]...)
		}
	}
	for _, d := range []uint64{3, 1, 4, 9, 2, 6} {
		require.NoError(t, b.Put(encodePrimaryKey(d), []byte("mutated-later"), WithSecondaryKey(secondaryPos, encodeDocID(d))))
	}
	require.NoError(t, b.FlushAndSwitch())
	for i := range got {
		require.Equalf(t, snapshot[i], got[i], "previously returned value at %d changed after later writes", i)
	}

	// Mutating one returned entry must not perturb another (independent backing).
	if len(got[0]) > 0 {
		got[0][0] ^= 0xFF
		for i := 1; i < len(got); i++ {
			require.Equalf(t, snapshot[i], got[i], "mutating result[0] perturbed result[%d]", i)
		}
	}
}

// TestBucketGetBySecondaryBatchEmptyAndSingle covers the len==0 and len==1
// (single-key delegate) edges, including a single missing key and a single deleted
// key.
func TestBucketGetBySecondaryBatchEmptyAndSingle(t *testing.T) {
	ctx := context.Background()
	b := newSecondaryBatchTestBucket(t, false)
	require.NoError(t, b.Put(encodePrimaryKey(1), []byte("one"), WithSecondaryKey(secondaryPos, encodeDocID(1))))
	require.NoError(t, b.Put(encodePrimaryKey(2), []byte("two"), WithSecondaryKey(secondaryPos, encodeDocID(2))))
	require.NoError(t, b.Delete(encodePrimaryKey(2), WithSecondaryKey(secondaryPos, encodeDocID(2))))
	require.NoError(t, b.FlushAndSwitch())

	empty, err := b.GetBySecondaryBatch(ctx, secondaryPos, nil)
	require.NoError(t, err)
	require.Empty(t, empty)

	empty2, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{})
	require.NoError(t, err)
	require.Empty(t, empty2)

	single, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{encodeDocID(1)})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("one")}, single)

	missing, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{encodeDocID(404)})
	require.NoError(t, err)
	require.Len(t, missing, 1)
	require.Nil(t, missing[0])

	deleted, err := b.GetBySecondaryBatch(ctx, secondaryPos, [][]byte{encodeDocID(2)})
	require.NoError(t, err)
	require.Len(t, deleted, 1)
	require.Nil(t, deleted[0])

	// invalid secondary position errors
	_, err = b.GetBySecondaryBatch(ctx, 5, [][]byte{encodeDocID(1)})
	require.Error(t, err)
}

// TestBucketGetBySecondaryBatchSlabChunking exercises the >500-key path: inputs
// above the view-hold cap are chunked into slabs (each with its own view) and the
// results are positionally restored across slab boundaries. Verified against the
// per-key loop over the same >cap key set.
func TestBucketGetBySecondaryBatchSlabChunking(t *testing.T) {
	b := newSecondaryBatchTestBucket(t, false)

	const universe = 1200 // > 2 * cap
	for d := uint64(0); d < universe; d++ {
		val := []byte{byte(d), byte(d >> 8), byte(d >> 16)}
		require.NoError(t, b.Put(encodePrimaryKey(d), val, WithSecondaryKey(secondaryPos, encodeDocID(d))))
		if d%300 == 299 {
			require.NoError(t, b.FlushAndSwitch())
		}
	}
	require.NoError(t, b.FlushAndSwitch())

	// 1300 keys (> 2*cap): a mix spanning the whole universe plus absent keys and
	// duplicates, deliberately unsorted so cross-slab positional restore is exercised.
	rng := rand.New(rand.NewSource(2024))
	keys := make([][]byte, 0, 1300)
	for d := uint64(0); d < universe; d++ {
		keys = append(keys, encodeDocID(d))
	}
	for i := 0; i < 100; i++ {
		keys = append(keys, encodeDocID(uint64(5_000_000+i))) // absent
	}
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	require.Greater(t, len(keys), 2*secondaryBatchViewHoldCap)

	requireBatchEqualsLoop(t, b, secondaryPos, keys)
}

// TestBucketGetBySecondaryBatchReadOpNeutral records the serial-baseline vs
// batch-path per-phase read-op counts over the same 500-key resolution, using the
// read-op-counting harness instrument. The batch path is expected to be read-op
// NEUTRAL (DiskTree.Get has no cursor amortization; the batch win is
// wall-time/concurrency, which the phase-2 concurrency tests gate). It asserts the
// batch mirror matches the real GetBySecondaryBatch values and that batch counts do
// not regress the serial baseline, then logs the before/after numbers for the record.
func TestBucketGetBySecondaryBatchReadOpNeutral(t *testing.T) {
	ctx := context.Background()
	shape := scaledShape()
	bucket, dir, docIDs := buildReadOpsBucket(t, shape)

	rng := rand.New(rand.NewSource(shape.seed + 11))
	targetsIDs := pickRandom(rng, docIDs, shape.numResolve)
	targets := make([][]byte, len(targetsIDs))
	for i, d := range targetsIDs {
		targets[i] = encodeDocID(d)
	}

	segs := reconstructSegments(t, dir)

	// serial baseline (before) via the read-op-counting instrument
	var serial phaseCounters
	for _, k := range targets {
		_, c, err := resolveSerialCounting(t, segs, k)
		require.NoError(t, err)
		serial.add(c)
	}

	// batch counts (after) via a faithful batch-algorithm mirror over the same
	// reconstructed trees; values pinned to the real GetBySecondaryBatch.
	batchVals, batch := resolveBatchCounting(t, segs, targets)
	realVals, err := bucket.GetBySecondaryBatch(ctx, secondaryPos, targets)
	require.NoError(t, err)
	require.Len(t, realVals, len(targets))
	for i := range targets {
		require.Equalf(t, realVals[i], batchVals[i], "batch mirror vs real GetBySecondaryBatch at %d", i)
	}

	// read-op neutrality: batch must not regress the serial per-phase counts.
	require.Equal(t, serial.indexNodeReads, batch.indexNodeReads, "index read-op count must be batch-neutral")
	require.Equal(t, serial.valueReadOps, batch.valueReadOps, "value read-op count must be batch-neutral")
	require.Equal(t, serial.recheckNodeReads, batch.recheckNodeReads, "recheck read-op count must be batch-neutral")

	denom := float64(shape.numResolve)
	t.Logf("read-op before/after (per key): index serial=%.2f batch=%.2f | value serial=%.2f batch=%.2f | recheck serial=%.2f batch=%.2f",
		float64(serial.indexNodeReads)/denom, float64(batch.indexNodeReads)/denom,
		float64(serial.valueReadOps)/denom, float64(batch.valueReadOps)/denom,
		float64(serial.recheckNodeReads)/denom, float64(batch.recheckNodeReads)/denom)
}

// resolveBatchCounting mirrors the batch algorithm's read pattern over the
// reconstructed trees (newest at index 0 in this instrument), counting read-ops per
// phase the same way resolveSerialCounting does, so the two are directly comparable.
func resolveBatchCounting(t testing.TB, segs []reconstructedSegment, keys [][]byte) ([][]byte, phaseCounters) {
	t.Helper()
	var c phaseCounters
	out := make([][]byte, len(keys))

	type pend struct {
		idx int
		key []byte
	}
	type hit struct {
		idx    int
		segIdx int
		node   segmentindex.Node
	}
	type live struct {
		idx    int
		segIdx int
		priKey []byte
		value  []byte
	}

	pending := make([]pend, len(keys))
	for i, k := range keys {
		pending[i] = pend{idx: i, key: k}
	}

	// Phase 1: newest (index 0) -> oldest, confirmed-hit elimination.
	hits := make([]hit, 0, len(pending))
	remaining := pending
	for i := 0; i < len(segs) && len(remaining) > 0; i++ {
		still := remaining[:0]
		for _, pk := range remaining {
			node, nodeReads, err := treeGetCounting(segs[i].secData, pk.key)
			c.indexNodeReads += int64(nodeReads)
			c.indexGetCalls++
			if err != nil {
				still = append(still, pk)
				continue
			}
			hits = append(hits, hit{idx: pk.idx, segIdx: i, node: node})
		}
		remaining = still
	}

	// Phase 2: offset-sorted serial value reads.
	sort.Slice(hits, func(a, b int) bool {
		if hits[a].segIdx != hits[b].segIdx {
			return hits[a].segIdx < hits[b].segIdx
		}
		return hits[a].node.Start < hits[b].node.Start
	})
	lives := make([]live, 0, len(hits))
	for _, h := range hits {
		raw, err := readValueMetered(segs[h.segIdx].contents, h.node, &c)
		require.NoError(t, err)
		priKey, value, perr := parseReplaceValue(raw)
		if perr != nil {
			continue // tombstone -> nil
		}
		lives = append(lives, live{idx: h.idx, segIdx: h.segIdx, priKey: append([]byte(nil), priKey...), value: append([]byte(nil), value...)})
	}

	// Phase 3: recheck newer segments (lower index) for the primary key.
	for _, lh := range lives {
		superseded := false
		for j := 0; j < lh.segIdx; j++ {
			_, nodeReads, err := treeGetCounting(segs[j].priData, lh.priKey)
			c.recheckNodeReads += int64(nodeReads)
			c.recheckGetCalls++
			if err == nil {
				superseded = true
				break
			}
		}
		if !superseded {
			out[lh.idx] = lh.value
		}
	}
	return out, c
}

// ---- phase-2 concurrency, arena, compaction-stress ---------------------------------
//
// The concurrency-effectiveness gate. Read-op count is orthogonal to whether phase 2
// runs 16-wide or accidentally serial, so the headline win (concurrent value issue)
// needs its OWN gate. Both tests drive the REAL production phase-2 function
// readSecondaryBatchValuesConcurrent through a nil-in-production instrumentation hook,
// so an accidentally-serial phase 2 (bound=1, SetLimit bug, or a serializing lock)
// FAILS them.

// buildAllHitSecondaryBucket writes numKeys distinct live docIDs spread across `segments`
// flushed segments, each key written exactly once (no updates, no tombstones), so every
// key resolves to a live value in exactly one segment: a clean all-hit batch.
func buildAllHitSecondaryBucket(t testing.TB, numKeys, segments int, extra ...BucketOption) (*Bucket, [][]byte) {
	t.Helper()
	b := newSecondaryBatchTestBucketMode(t, false, true, extra...)
	keys := make([][]byte, numKeys)
	perSeg := (numKeys + segments - 1) / segments
	written := 0
	for s := 0; s < segments && written < numKeys; s++ {
		for i := 0; i < perSeg && written < numKeys; i++ {
			d := uint64(written)
			val := make([]byte, 48)
			val[0], val[1] = byte(d), byte(d>>8) // observably distinct per key
			require.NoError(t, b.Put(encodePrimaryKey(d), val,
				WithSecondaryKey(secondaryPos, encodeDocID(d))))
			keys[written] = encodeDocID(d)
			written++
		}
		require.NoError(t, b.FlushAndSwitch())
	}
	return b, keys
}

// indexHitsForKeys runs the real phase-1 index descent over keys under view, returning the
// confirmed hits phase 2 consumes. Lets the concurrency gate exercise the real phase-2 fn.
func indexHitsForKeys(t testing.TB, b *Bucket, view BucketConsistentView, keys [][]byte) []secondaryBatchIndexHit {
	t.Helper()
	unresolved := make([]secondaryBatchKey, len(keys))
	for i, k := range keys {
		unresolved[i] = secondaryBatchKey{origIdx: i, key: k}
	}
	hits, err := b.disk.getBySecondaryBatchIndexHits(context.Background(), secondaryPos, unresolved, view.Disk, nil)
	require.NoError(t, err)
	return hits
}

// concurrencyProbe is a deterministic, bounded peak-in-flight counter for the close-blocking
// gate. onReadStart increments in-flight, records the peak, and blocks on a gate that opens
// when in-flight reaches target OR a bounded grace deadline fires (armed on the first read).
// A genuinely-concurrent phase 2 reaches target quickly (gate opens, peak >= target); an
// accidentally-serial one never exceeds in-flight=1, so the gate opens only at the grace
// deadline and peak stays 1 -> the assertion fails fast instead of hanging.
type concurrencyProbe struct {
	mu       sync.Mutex
	inflight int
	peak     int
	target   int
	grace    time.Duration
	gate     chan struct{}
	closeOne sync.Once
	armOne   sync.Once
}

func newConcurrencyProbe(target int, grace time.Duration) *concurrencyProbe {
	return &concurrencyProbe{target: target, grace: grace, gate: make(chan struct{})}
}

func (p *concurrencyProbe) openGate() { p.closeOne.Do(func() { close(p.gate) }) }

func (p *concurrencyProbe) onReadStart() {
	p.armOne.Do(func() { time.AfterFunc(p.grace, p.openGate) })
	// critical section is not the whole body (the gate wait must be lock-free, or
	// onReadDone would deadlock), so scope it in an IIFE with a deferred unlock.
	reached := func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.inflight++
		if p.inflight > p.peak {
			p.peak = p.inflight
		}
		return p.inflight >= p.target
	}()
	if reached {
		p.openGate()
	}
	<-p.gate
}

func (p *concurrencyProbe) onReadDone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight--
}

func (p *concurrencyProbe) peakInflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// TestBucketGetBySecondaryBatchPhase2ConcurrencyPeakInflight is close-blocking gate (i):
// at B=16 over a 500-key all-hit batch, phase 2 must run >= 8 value reads concurrently.
func TestBucketGetBySecondaryBatchPhase2ConcurrencyPeakInflight(t *testing.T) {
	ctx := context.Background()
	b, keys := buildAllHitSecondaryBucket(t, 500, 6)
	view := b.GetConsistentView()
	defer view.ReleaseView()

	hits := indexHitsForKeys(t, b, view, keys)
	require.Len(t, hits, len(keys), "all-hit fixture must confirm every key in phase 1")

	const batchConcurrency = 16
	probe := newConcurrencyProbe(8, 2*time.Second)
	hook := &secondaryBatchReadHook{onReadStart: probe.onReadStart, onReadDone: probe.onReadDone}

	lives, arenaBytes, err := b.disk.readSecondaryBatchValuesConcurrent(ctx, hits, view.Disk, batchConcurrency, hook)
	require.NoError(t, err)
	require.Len(t, lives, len(keys), "every all-hit key must yield a live value")
	require.Positive(t, arenaBytes, "phase 2 must allocate a non-zero arena")

	require.GreaterOrEqualf(t, probe.peakInflight(), 8,
		"phase 2 peaked at %d in-flight reads at B=16; < 8 means it serialized "+
			"(B=1 misconfig, SetLimit bug, or a serializing lock)", probe.peakInflight())
}

// TestBucketGetBySecondaryBatchPhase2WallTimeRatio is close-blocking gate (ii): under a
// fixed injected per-read latency, the concurrent batch (B=16) must complete in <= 0.5x the
// serial (B=1) wall time on the same hits. A serialized phase 2 would be ~1x and fail.
func TestBucketGetBySecondaryBatchPhase2WallTimeRatio(t *testing.T) {
	ctx := context.Background()
	b, keys := buildAllHitSecondaryBucket(t, 500, 6)
	view := b.GetConsistentView()
	defer view.ReleaseView()

	const perRead = 1 * time.Millisecond
	latency := &secondaryBatchReadHook{onReadStart: func() { time.Sleep(perRead) }}

	hitsSerial := indexHitsForKeys(t, b, view, keys)
	startSerial := time.Now()
	livesSerial, _, err := b.disk.readSecondaryBatchValuesConcurrent(ctx, hitsSerial, view.Disk, 1, latency)
	serialWall := time.Since(startSerial)
	require.NoError(t, err)
	require.Len(t, livesSerial, len(keys))

	hitsConc := indexHitsForKeys(t, b, view, keys)
	startConc := time.Now()
	livesConc, _, err := b.disk.readSecondaryBatchValuesConcurrent(ctx, hitsConc, view.Disk, 16, latency)
	concWall := time.Since(startConc)
	require.NoError(t, err)
	require.Len(t, livesConc, len(keys))

	require.Lessf(t, concWall, serialWall/2,
		"concurrent phase 2 (B=16) must be <= 0.5x serial (B=1) under injected %s/read latency; "+
			"serial=%s concurrent=%s (a serialized phase 2 would be ~1x)", perRead, serialWall, concWall)
}

// TestBucketGetBySecondaryBatchArenaNonAliasing pins the disjoint-sub-slice invariant: each
// result owns its own arena range, so mutating one result never perturbs another, and the
// results stay valid after the view is released (GetBySecondaryBatch releases its own view).
func TestBucketGetBySecondaryBatchArenaNonAliasing(t *testing.T) {
	ctx := context.Background()
	b, keys := buildAllHitSecondaryBucket(t, 128, 4)

	got, err := b.GetBySecondaryBatch(ctx, secondaryPos, keys) // view already released on return
	require.NoError(t, err)
	require.Len(t, got, len(keys))

	snapshot := make([][]byte, len(got))
	for i := range got {
		require.NotNilf(t, got[i], "all-hit key %d must resolve", i)
		snapshot[i] = bytes.Clone(got[i])
	}

	// Mutate result[0] in place; every other result must be byte-identical to its snapshot
	// (disjoint arena sub-slices -> no aliasing across results).
	for j := range got[0] {
		got[0][j] ^= 0xFF
	}
	for j := 1; j < len(got); j++ {
		require.Equalf(t, snapshot[j], got[j],
			"mutating result[0] perturbed result[%d]: arena sub-slices are not disjoint", j)
	}
}

// TestBucketGetBySecondaryBatchCtxCancelMidPhase2 confirms clean drain: cancelling ctx
// mid-phase-2 surfaces context.Canceled (the errgroup cancels egctx on the first error and
// pending reads observe it) rather than hanging or leaking goroutines (the -race run would
// flag a leak/deadlock).
func TestBucketGetBySecondaryBatchCtxCancelMidPhase2(t *testing.T) {
	b, keys := buildAllHitSecondaryBucket(t, 500, 6)
	view := b.GetConsistentView()
	defer view.ReleaseView()
	hits := indexHitsForKeys(t, b, view, keys)

	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	hook := &secondaryBatchReadHook{onReadStart: func() { once.Do(cancel) }}

	_, _, err := b.disk.readSecondaryBatchValuesConcurrent(ctx, hits, view.Disk, 16, hook)
	require.ErrorIs(t, err, context.Canceled)
}

// ---- phase-1 descent path (gh#309 pass-3c: serial descents, cancellation, hook seam) ----
//
// pass-3c reverted the within-segment parallel-descent variant: on a QD-capped device the
// parallel descents doubled major-fault events at identical page/byte counts and made cold
// bm25 worse, so phase-1 descents are serial again. The onDescentStart/onDescentDone hook
// seam is retained (nil in production) for the chunked-pipeline prototype A/B and for the
// phase-1 cancellation gate below.

// buildOldestSegmentHitBucket writes all numKeys target keys into the OLDEST segment and
// fills each of the numSegments-1 newer segments with disjoint filler keys. Every target
// key therefore stays unresolved through every newer segment (a descent per segment) and
// only confirms in the oldest: the worst-case cold shape that forces a full descent of
// every segment for every key, maximizing the phase-1 descent fan-out under test.
func buildOldestSegmentHitBucket(t testing.TB, numKeys, numSegments int, extra ...BucketOption) (*Bucket, [][]byte) {
	t.Helper()
	require.GreaterOrEqual(t, numSegments, 2,
		"need >= 2 segments so target keys descend newer segments before confirming in the oldest")
	b := newSecondaryBatchTestBucketMode(t, false, true, extra...)

	keys := make([][]byte, numKeys)
	for d := 0; d < numKeys; d++ {
		val := make([]byte, 48)
		val[0], val[1] = byte(d), byte(d>>8) // observably distinct per key
		require.NoError(t, b.Put(encodePrimaryKey(uint64(d)), val,
			WithSecondaryKey(secondaryPos, encodeDocID(uint64(d)))))
		keys[d] = encodeDocID(uint64(d))
	}
	require.NoError(t, b.FlushAndSwitch()) // oldest segment (lowest index) holds every target key

	filler := uint64(10_000_000) // disjoint from the [0,numKeys) target docID range
	for s := 1; s < numSegments; s++ {
		for i := 0; i < 8; i++ {
			val := make([]byte, 16)
			require.NoError(t, b.Put(encodePrimaryKey(filler), val,
				WithSecondaryKey(secondaryPos, encodeDocID(filler))))
			filler++
		}
		require.NoError(t, b.FlushAndSwitch())
	}
	return b, keys
}

// unresolvedFromKeys builds the phase-1 input set (origIdx-tagged) from a key slice. Each
// call allocates a fresh backing array because getBySecondaryBatchIndexHits consumes it as
// compaction scratch.
func unresolvedFromKeys(keys [][]byte) []secondaryBatchKey {
	u := make([]secondaryBatchKey, len(keys))
	for i, k := range keys {
		u[i] = secondaryBatchKey{origIdx: i, key: k}
	}
	return u
}

// TestBucketGetBySecondaryBatchCtxCancelMidPhase1 confirms a ctx cancelled during phase 1
// aborts the resolve rather than descending every remaining segment: cancelling on the
// first descent surfaces context.Canceled at the next per-segment ctx check. Serial phase 1
// checks ctx once per segment, so cancellation is observed at segment granularity; the
// worst-case oldest-segment fixture guarantees more than one segment is descended. The
// -race run would flag any leak in the unwind.
func TestBucketGetBySecondaryBatchCtxCancelMidPhase1(t *testing.T) {
	b, keys := buildOldestSegmentHitBucket(t, 500, 6)
	view := b.GetConsistentView()
	defer view.ReleaseView()

	ctx, cancel := context.WithCancel(context.Background())
	var once sync.Once
	hook := &secondaryBatchReadHook{onDescentStart: func() { once.Do(cancel) }}

	_, err := b.disk.getBySecondaryBatchIndexHits(
		ctx, secondaryPos, unresolvedFromKeys(keys), view.Disk, hook)
	require.ErrorIs(t, err, context.Canceled)
}

// TestBucketGetBySecondaryBatchSlowLogAnnotationOnRealCtx pins the ctx fix (storage_object.go
// no longer passes context.TODO()): resolving under a real ctx that carries the slow-query
// accumulator must land the lsm_get_by_secondary_batch per-phase timing entry. A context.TODO()
// carries no accumulator, so the annotation is silently dropped, exactly why bm25 never
// showed batch phase timings while the filter path did (pass-3b bonus finding).
func TestBucketGetBySecondaryBatchSlowLogAnnotationOnRealCtx(t *testing.T) {
	b, keys := buildAllHitSecondaryBucket(t, 64, 4)
	ctx := helpers.InitSlowQueryDetails(context.Background())

	_, err := b.GetBySecondaryBatch(ctx, secondaryPos, keys)
	require.NoError(t, err)

	details := helpers.ExtractSlowQueryDetails(ctx)
	require.Contains(t, details, "lsm_get_by_secondary_batch",
		"batch resolver must annotate the slow log under a real ctx; a context.TODO() would "+
			"silently drop the accumulator (the gh#309 bm25 missing-batch-timings bug)")
}

// newCompactableSecondaryBucket is like newSecondaryBatchTestBucket but leaves compaction
// enabled so compactOnce can merge segments while a batch reads them.
func newCompactableSecondaryBucket(t testing.TB) *Bucket {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(
		ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithPread(true),
		WithMinMMapSize(0),
		WithUseBloomFilter(false),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	return b
}

// buildCompactableSecondaryBucket writes several overlapping segments (updates + tombstones,
// so newest-wins across segments is exercised) with all data in segments (no leftover
// active/flushing memtable), returning the docID universe. Compaction is result-preserving
// for a Replace bucket, so a ground-truth resolution taken before compaction stays valid.
func buildCompactableSecondaryBucket(t testing.TB, seed int64) (*Bucket, []uint64) {
	t.Helper()
	b := newCompactableSecondaryBucket(t)
	rng := rand.New(rand.NewSource(seed))

	const universeSize = 300
	universe := make([]uint64, universeSize)
	for i := range universe {
		universe[i] = uint64(i)
	}
	putVersion := func(d uint64, version int) {
		val := make([]byte, 24)
		rng.Read(val)
		val[0] = byte(version)
		require.NoError(t, b.Put(encodePrimaryKey(d), val, WithSecondaryKey(secondaryPos, encodeDocID(d))))
	}

	const segments = 5
	for s := 0; s < segments; s++ {
		for _, d := range universe {
			switch r := rng.Intn(100); {
			case r < 55:
				putVersion(d, s)
			case r < 65:
				require.NoError(t, b.Delete(encodePrimaryKey(d), WithSecondaryKey(secondaryPos, encodeDocID(d))))
			default:
			}
		}
		require.NoError(t, b.FlushAndSwitch())
	}
	return b, universe
}

// TestBucketGetBySecondaryBatchCompactionStressRace runs GetBySecondaryBatch concurrently
// with active compaction under the race detector, asserting equality to a pre-compaction
// ground truth AND no crash. Compaction swaps/drops segments under an in-flight batch, so
// this doubles as the #1837 copy-under-refcount race probe: every returned value must have
// been copied out of the segment into the arena before the view released. Run with -race.
func TestBucketGetBySecondaryBatchCompactionStressRace(t *testing.T) {
	ctx := context.Background()
	b, universe := buildCompactableSecondaryBucket(t, 20260714)
	keys := make([][]byte, len(universe))
	for i, d := range universe {
		keys[i] = encodeDocID(d)
	}

	// Ground truth via the per-key loop; compaction is result-preserving so it stays valid.
	want := resolveViaLoop(t, b, secondaryPos, keys)

	var compactionDone atomic.Bool
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer compactionDone.Store(true)
		for {
			compacted, err := b.disk.compactOnce(ctx)
			if err != nil {
				t.Errorf("compactOnce under stress: %v", err)
				return
			}
			if !compacted {
				return
			}
		}
	}()

	const resolvers = 4
	for g := 0; g < resolvers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; !compactionDone.Load() || iter == 0; iter++ {
				got, err := b.GetBySecondaryBatch(ctx, secondaryPos, keys)
				if err != nil {
					t.Errorf("batch under compaction: %v", err)
					return
				}
				for i := range keys {
					if !bytes.Equal(got[i], want[i]) {
						t.Errorf("compaction-stress divergence at position %d (key %x)", i, keys[i])
						return
					}
				}
			}
		}()
	}
	wg.Wait()
}

// peakInflightHook is a lock-guarded peak-concurrency counter with an injected
// per-read delay, so up to `limit` reads genuinely overlap and the observed peak
// reflects the effective phase-2 concurrency bound.
type peakInflightHook struct {
	mu       sync.Mutex
	inflight int
	peak     int
	delay    time.Duration
}

func (p *peakInflightHook) onStart() {
	p.mu.Lock()
	p.inflight++
	if p.inflight > p.peak {
		p.peak = p.inflight
	}
	p.mu.Unlock()
	time.Sleep(p.delay) // hold the slot so siblings overlap
}

func (p *peakInflightHook) onDone() {
	p.mu.Lock()
	p.inflight--
	p.mu.Unlock()
}

func (p *peakInflightHook) peakValue() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// TestBucketSecondaryBatchReadConcurrencyHotReload proves the phase-2 value-read
// concurrency is a live runtime value: changing the DynamicValue with SetValue re-tunes
// the effective per-batch bound with no bucket restart, and that live value flows into
// the real phase-2 errgroup (readSecondaryBatchValuesConcurrent), bounding how many value
// reads overlap. The same expression the production path uses,
// b.secondaryBatchReadConcurrencyValue(), is read here.
func TestBucketSecondaryBatchReadConcurrencyHotReload(t *testing.T) {
	ctx := context.Background()
	dv := configRuntime.NewDynamicValue(4)
	b, keys := buildAllHitSecondaryBucket(t, 500, 6, WithSecondaryBatchReadConcurrency(dv))
	view := b.GetConsistentView()
	defer view.ReleaseView()

	require.Equal(t, 4, b.secondaryBatchReadConcurrencyValue(),
		"effective bound must reflect the configured DynamicValue")

	measurePeak := func() int {
		hits := indexHitsForKeys(t, b, view, keys)
		hook := &peakInflightHook{delay: 2 * time.Millisecond}
		limit := b.secondaryBatchReadConcurrencyValue() // exactly what the production path passes
		_, _, err := b.disk.readSecondaryBatchValuesConcurrent(ctx, hits, view.Disk, limit,
			&secondaryBatchReadHook{onReadStart: hook.onStart, onReadDone: hook.onDone})
		require.NoError(t, err)
		return hook.peakValue()
	}

	// bound = 4: at most 4 value reads overlap.
	peakLow := measurePeak()
	require.LessOrEqual(t, peakLow, 4, "peak in-flight must not exceed the configured bound of 4")
	require.GreaterOrEqual(t, peakLow, 2, "with an injected delay several reads should overlap at bound 4")

	// Hot-reload: raise the bound at runtime with no bucket restart.
	require.NoError(t, dv.SetValue(24))
	require.Equal(t, 24, b.secondaryBatchReadConcurrencyValue(),
		"runtime SetValue must take effect without restart")

	peakHigh := measurePeak()
	require.Greater(t, peakHigh, peakLow,
		"raising the runtime concurrency must let more value reads overlap (was %d, now %d)", peakLow, peakHigh)
	require.GreaterOrEqual(t, peakHigh, 8,
		"at bound 24 over a 500-read batch, peak in-flight should comfortably exceed the old bound")

	// Non-positive falls back to the compiled default (never zero concurrency).
	require.NoError(t, dv.SetValue(0))
	require.Equal(t, defaultSecondaryBatchReadConcurrency, b.secondaryBatchReadConcurrencyValue(),
		"a non-positive runtime value falls back to the default, not to zero")
}
