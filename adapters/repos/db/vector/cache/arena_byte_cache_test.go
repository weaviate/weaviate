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

package cache

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const testRecordSize = 112 // 16 B metadata + 96 B code: centered 768d rq1

// arenaTestCodeFor returns the deterministic record bytes for an id.
func arenaTestCodeFor(id uint64, size int) []byte {
	code := make([]byte, size)
	for i := range code {
		code[i] = byte(id*31 + uint64(i)*7 + 1)
	}
	return code
}

// arenaTestVecForID backs ids below limit whose id%7 != 3; everything else
// returns a not-found error, so the miss path sees both outcomes.
func arenaTestVecForID(limit uint64, size int) func(ctx context.Context, id uint64) ([]byte, error) {
	return func(_ context.Context, id uint64) ([]byte, error) {
		if id >= limit || id%7 == 3 {
			return nil, storobj.NewErrNotFoundf(id, "not backed")
		}
		return arenaTestCodeFor(id, size), nil
	}
}

func newTestArena(t *testing.T, recordSize int) Cache[byte] {
	t.Helper()
	logger, _ := test.NewNullLogger()
	c, err := NewArenaByteCache(arenaTestVecForID(5000, recordSize), recordSize,
		1e12, 1, logger, 0, nil)
	require.NoError(t, err)
	t.Cleanup(c.Drop)
	return c
}

func newOracle(t *testing.T, recordSize int) Cache[byte] {
	t.Helper()
	logger, _ := test.NewNullLogger()
	c := NewShardedByteLockCache(arenaTestVecForID(5000, recordSize), 1e12, 1, logger, 0, nil)
	t.Cleanup(c.Drop)
	return c
}

// TestArenaByteCacheParity drives a long random operation sequence against
// the arena cache and shardedLockCache side by side and asserts identical
// observable results after every step: returned bytes, errors, Len,
// CountVectors. This is the byte-identical gate: the arena changes layout,
// never semantics.
func TestArenaByteCacheParity(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)
	oracle := newOracle(t, testRecordSize)

	rng := rand.New(rand.NewPCG(0xa1e4a, 0xca11e))
	// id space spans multiple chunks (chunk = 4096 records) and includes a
	// far outlier to exercise growth.
	randomID := func() uint64 {
		if rng.IntN(50) == 0 {
			return uint64(rng.IntN(60000)) // occasionally far out
		}
		return uint64(rng.IntN(10000))
	}

	for step := 0; step < 20000; step++ {
		id := randomID()
		op := rng.IntN(100)
		desc := fmt.Sprintf("step %d op %d id %d", step, op, id)

		switch {
		case op < 35: // Get
			gotVec, gotErr := arena.Get(ctx, id)
			wantVec, wantErr := oracle.Get(ctx, id)
			if wantErr != nil {
				assert.Error(t, gotErr, desc)
			} else {
				require.NoError(t, gotErr, desc)
				assert.Equal(t, wantVec, gotVec, desc)
			}
		case op < 55: // Preload
			code := arenaTestCodeFor(id, testRecordSize)
			// grow both first: the oracle's Preload loops on Grow itself,
			// and so must the arena.
			arena.Preload(id, code)
			oracle.Preload(id, code)
		case op < 60: // PreloadIfAbsent
			code := arenaTestCodeFor(id, testRecordSize)
			gotStored := arena.(IfAbsentPreloader[byte]).PreloadIfAbsent(id, code)
			wantStored := oracle.(IfAbsentPreloader[byte]).PreloadIfAbsent(id, code)
			assert.Equal(t, wantStored, gotStored, desc)
		case op < 70: // Delete
			arena.Delete(ctx, id)
			oracle.Delete(ctx, id)
		case op < 75: // Grow
			arena.Grow(id)
			oracle.Grow(id)
		case op < 85: // MultiGet
			ids := make([]uint64, 1+rng.IntN(8))
			for i := range ids {
				ids[i] = randomID()
			}
			gotVecs, gotErrs := arena.MultiGet(ctx, ids)
			wantVecs, wantErrs := oracle.MultiGet(ctx, ids)
			for i := range ids {
				wantErr := wantErrs != nil && wantErrs[i] != nil
				gotErr := gotErrs != nil && gotErrs[i] != nil
				assert.Equal(t, wantErr, gotErr, "%s multiget idx %d", desc, i)
				if !wantErr {
					assert.Equal(t, wantVecs[i], gotVecs[i], "%s multiget idx %d", desc, i)
				}
			}
		case op < 95: // PrefetchGet + Prefetch
			arena.Prefetch(id)
			oracle.Prefetch(id)
			assert.Equal(t, oracle.PrefetchGet(id), arena.PrefetchGet(id), desc)
		default: // UpdateMaxSize/CopyMaxSize round trip
			size := int64(rng.IntN(1 << 30))
			arena.UpdateMaxSize(size)
			oracle.UpdateMaxSize(size)
			assert.Equal(t, oracle.CopyMaxSize(), arena.CopyMaxSize(), desc)
		}

		assert.Equal(t, oracle.Len(), arena.Len(), desc)
		assert.Equal(t, oracle.CountVectors(), arena.CountVectors(), desc)
	}

	// All() must agree entry by entry (values, not slice identity).
	wantAll := oracle.All()
	gotAll := arena.All()
	require.Equal(t, len(wantAll), len(gotAll), "All() length")
	for i := range wantAll {
		if wantAll[i] == nil {
			assert.Nil(t, gotAll[i], "All() id %d", i)
		} else {
			assert.Equal(t, wantAll[i], gotAll[i], "All() id %d", i)
		}
	}
}

// TestArenaByteCacheAlignment pins the alignment invariant: chunk bases are
// 128-byte aligned (arenaBaseAlign), so every record's address modulo 128
// equals its in-chunk offset (slot*stride) modulo 128, and every record is
// at least 64-byte aligned. Checked for record 0 of several chunks and a
// random sample across record sizes whose strides cover both the
// 128-multiple and the odd-64 cases. A refactor that loses the alignment
// slack breaks this test, not production latency.
func TestArenaByteCacheAlignment(t *testing.T) {
	ctx := context.Background()
	for _, recordSize := range []int{40, 64, 112, 208, 784, 1552} {
		t.Run(fmt.Sprintf("recordSize=%d", recordSize), func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			c, err := NewArenaByteCache(arenaTestVecForID(0, recordSize), recordSize,
				1e12, 1, logger, 0, nil)
			require.NoError(t, err)
			defer c.Drop()

			stride := uintptr((recordSize + arenaAlign - 1) / arenaAlign * arenaAlign)
			rng := rand.New(rand.NewPCG(7, uint64(recordSize)))
			// record 0 of chunks 0..3 (asserts the 128-aligned base directly),
			// chunk-boundary neighbours, and a random sample
			ids := []uint64{0, 1, 4095, 4096, 4097, 8192, 12288}
			for i := 0; i < 50; i++ {
				ids = append(ids, uint64(rng.IntN(20000)))
			}
			for _, id := range ids {
				c.Preload(id, arenaTestCodeFor(id, recordSize))
			}
			for _, id := range ids {
				vec, err := c.Get(ctx, id)
				require.NoError(t, err, "id %d", id)
				require.Equal(t, recordSize, len(vec))
				addr := uintptr(unsafe.Pointer(&vec[0]))
				// floor guarantee: every record on a 64-byte boundary
				assert.Zerof(t, addr%64, "record %d not 64-byte aligned (addr %x)", id, addr)
				// base guarantee: address mod 128 is exactly the in-chunk
				// offset mod 128 — holds only if the chunk base is ≡0 mod 128
				slot := uintptr(id & arenaChunkMask)
				assert.Equalf(t, (slot*stride)%arenaBaseAlign, addr%arenaBaseAlign,
					"record %d: chunk base not %d-byte aligned (addr %x)", id, arenaBaseAlign, addr)
				if slot == 0 {
					assert.Zerof(t, addr%arenaBaseAlign,
						"chunk base for record %d not %d-byte aligned (addr %x)", id, arenaBaseAlign, addr)
				}
			}
		})
	}
}

// TestArenaByteCacheRecordSizeEnforced pins that the fixed-stride arena
// refuses records of the wrong length instead of storing or panicking: the
// preload paths skip them (one bad record must not abort prefill) and the
// miss path returns an error (it must not crash every search reaching the
// id). The sharded cache would silently store any slice.
func TestArenaByteCacheRecordSizeEnforced(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	short := func(_ context.Context, id uint64) ([]byte, error) {
		return make([]byte, testRecordSize-1), nil
	}
	ac, err := NewArenaByteCache(short, testRecordSize, 1e12, 1, logger, 0, nil)
	require.NoError(t, err)
	t.Cleanup(ac.Drop)
	c := ac.(*arenaByteCache)

	assert.NotPanics(t, func() {
		c.Preload(1, make([]byte, testRecordSize-1))
		c.Preload(1, make([]byte, testRecordSize+1))
		c.Preload(1, nil)
		c.SetSizeAndGrowNoLock(10)
		c.PreloadNoLock(2, make([]byte, testRecordSize+1))
	})
	assert.Equal(t, int64(0), c.CountVectors())

	// PreloadIfAbsent refuses with false, for an empty record too
	assert.False(t, c.PreloadIfAbsent(1, make([]byte, testRecordSize-1)))
	assert.False(t, c.PreloadIfAbsent(1, make([]byte, testRecordSize+1)))
	assert.False(t, c.PreloadIfAbsent(1, nil))

	// a wrong-length fetch on the miss path is an error, and not cached
	vec, err := c.Get(ctx, 3)
	require.Error(t, err)
	assert.Nil(t, vec)
	vecs, errs := c.MultiGet(ctx, []uint64{3, 4})
	require.Len(t, errs, 2)
	assert.Error(t, errs[0])
	assert.Error(t, errs[1])
	assert.Nil(t, vecs[0])
	assert.Equal(t, int64(0), c.CountVectors())
}

// badRecordWarnings classifies hook's warn entries, in log order, into the
// number of detailed first-refusal lines and the per-flush summary counts.
// Any other warning fails the test: the rate limit's contract is that a
// refused record either is the one detailed line or joins a summary count,
// never a line of its own. A summary line for an empty flush also fails.
// Must be called from the test goroutine (it can FailNow).
func badRecordWarnings(t *testing.T, hook *test.Hook) (details int, skips []int64) {
	t.Helper()
	for _, e := range hook.AllEntries() {
		if e.Level != logrus.WarnLevel {
			continue
		}
		if strings.Contains(e.Message, "further size mismatches") {
			details++
			continue
		}
		var n, size int64
		_, err := fmt.Sscanf(e.Message, "skipped %d more records whose size is not %d bytes", &n, &size)
		require.NoErrorf(t, err, "warning is neither the detailed first refusal nor a summary: %q", e.Message)
		require.Positive(t, n, "a flush with nothing pending must stay quiet")
		skips = append(skips, n)
	}
	return details, skips
}

// summarizedSkips is the total refusal count the summary lines account for
// so far, tolerant of every other entry so that an Eventually condition can
// poll it from its own goroutine without failing the test.
func summarizedSkips(hook *test.Hook) int64 {
	var total int64
	for _, e := range hook.AllEntries() {
		if e.Level != logrus.WarnLevel {
			continue
		}
		var n, size int64
		if _, err := fmt.Sscanf(e.Message, "skipped %d more records whose size is not %d bytes", &n, &size); err == nil {
			total += n
		}
	}
	return total
}

func sumInt64(vals []int64) int64 {
	var total int64
	for _, v := range vals {
		total += v
	}
	return total
}

// TestArenaByteCacheBadRecordLogRateLimited: a systematic size mismatch
// refuses every record of a prefill. Only the first refusal is logged in
// full; the rest are counted and come out as summary lines with the counts,
// on the deletion ticker or on Drop, instead of one line per record.
func TestArenaByteCacheBadRecordLogRateLimited(t *testing.T) {
	warnings := func(hook *test.Hook) []string {
		var out []string
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.WarnLevel {
				out = append(out, e.Message)
			}
		}
		return out
	}
	bad := make([]byte, testRecordSize+1)

	t.Run("summary on drop", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		ac, err := NewArenaByteCache(nil, testRecordSize, 1e12, 1, logger, 0, nil)
		require.NoError(t, err)
		c := ac.(*arenaByteCache)
		c.SetSizeAndGrowNoLock(2000)
		for id := uint64(0); id < 1000; id++ {
			c.Preload(id, bad)
			c.PreloadIfAbsent(id, bad)
			c.PreloadNoLock(id, bad)
		}
		require.Len(t, warnings(hook), 1, "only the first refusal is logged per record")
		assert.Contains(t, warnings(hook)[0], "further size mismatches")

		c.Drop()
		got := warnings(hook)
		require.Len(t, got, 2)
		assert.Contains(t, got[1], "skipped 2999 more records")
	})

	t.Run("summary on tick", func(t *testing.T) {
		logger, hook := test.NewNullLogger()
		ac, err := NewArenaByteCache(nil, testRecordSize, 1e12, 1, logger, 10*time.Millisecond, nil)
		require.NoError(t, err)
		t.Cleanup(ac.Drop)
		for id := uint64(0); id < 500; id++ {
			ac.Preload(id, bad)
		}

		// The ticker races the preload loop: a tick that fires mid-loop
		// splits the 499 counted refusals over several summary lines, and on
		// a slow runner it does. The schedule is not the contract, so assert
		// the invariants instead: the summaries eventually account for every
		// refusal past the first, in exactly one detailed line plus one line
		// per flush that had anything to report.
		require.Eventually(t, func() bool { return summarizedSkips(hook) == 499 },
			5*time.Second, 5*time.Millisecond)

		got := warnings(hook)
		require.NotEmpty(t, got)
		assert.Contains(t, got[0], "further size mismatches",
			"the detailed first refusal precedes every summary")
		details, skips := badRecordWarnings(t, hook)
		assert.Equal(t, 1, details, "exactly one detailed line, however many flushes ran")
		assert.EqualValues(t, 499, sumInt64(skips))

		// nothing left to flush: later ticks stay quiet
		time.Sleep(50 * time.Millisecond)
		assert.Len(t, warnings(hook), len(got))
	})

	t.Run("split summaries account for every refusal", func(t *testing.T) {
		// the mid-loop flush of the tick subtest, pinned deterministically:
		// no ticker (interval 0), the flush is driven by the test
		logger, hook := test.NewNullLogger()
		ac, err := NewArenaByteCache(nil, testRecordSize, 1e12, 1, logger, 0, nil)
		require.NoError(t, err)
		t.Cleanup(ac.Drop)
		c := ac.(*arenaByteCache)
		for id := uint64(0); id < 300; id++ {
			c.Preload(id, bad)
		}
		c.flushBadRecords()
		c.flushBadRecords() // empty flush: must log nothing
		for id := uint64(300); id < 500; id++ {
			c.Preload(id, bad)
		}
		c.flushBadRecords()

		details, skips := badRecordWarnings(t, hook)
		assert.Equal(t, 1, details)
		assert.Equal(t, []int64{299, 200}, skips)
	})
}

// TestArenaByteCacheNoLockPaths covers the two NoLock entry points against
// the oracle, pinning the occupied-slot counting they share with every
// other write path: SetSizeAndGrowNoLock only grows coverage and never
// touches the count, and PreloadNoLock bumps the count exactly when it
// fills an empty slot (and not on overwrites).
func TestArenaByteCacheNoLockPaths(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)
	oracle := newOracle(t, testRecordSize)

	arena.SetSizeAndGrowNoLock(1234)
	oracle.SetSizeAndGrowNoLock(1234)
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
	assert.EqualValues(t, 0, arena.CountVectors(),
		"SetSizeAndGrowNoLock must not touch the count")
	assert.Equal(t, oracle.Len(), arena.Len())

	for id := uint64(0); id < 100; id++ {
		code := arenaTestCodeFor(id, testRecordSize)
		arena.PreloadNoLock(id, code)
		oracle.PreloadNoLock(id, code)
	}
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
	assert.EqualValues(t, 100, arena.CountVectors(),
		"PreloadNoLock must count newly occupied slots")

	// overwriting the same ids must not move the count
	for id := uint64(0); id < 100; id++ {
		code := arenaTestCodeFor(id+1, testRecordSize)
		arena.PreloadNoLock(id, code)
		oracle.PreloadNoLock(id, code)
	}
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
	assert.EqualValues(t, 100, arena.CountVectors(),
		"PreloadNoLock overwrites must not change the count")

	for id := uint64(0); id < 100; id++ {
		want, wantErr := oracle.Get(ctx, id)
		got, gotErr := arena.Get(ctx, id)
		require.NoError(t, wantErr)
		require.NoError(t, gotErr)
		assert.Equal(t, want, got, "id %d", id)
	}
}

// TestArenaByteCachePreloadIfAbsent pins PreloadIfAbsent against the
// oracle: it stores into an empty slot (reporting true), refuses to clobber
// an occupied slot (reporting false), refuses an empty vector without
// growing the cache, and — like the oracle — grows to cover an id beyond
// the current length and stores there.
func TestArenaByteCachePreloadIfAbsent(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)
	arenaIA := arena.(IfAbsentPreloader[byte])
	oracle := newOracle(t, testRecordSize)
	oracleIA := oracle.(IfAbsentPreloader[byte])

	// store into an empty slot
	code := arenaTestCodeFor(7, testRecordSize)
	assert.True(t, arenaIA.PreloadIfAbsent(7, code))
	assert.True(t, oracleIA.PreloadIfAbsent(7, code))
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())

	// refuse on an occupied slot: the old bytes survive
	other := arenaTestCodeFor(8, testRecordSize)
	assert.False(t, arenaIA.PreloadIfAbsent(7, other))
	assert.False(t, oracleIA.PreloadIfAbsent(7, other))
	got, err := arena.Get(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, code, got, "occupied slot must keep its bytes")
	want, err := oracle.Get(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())

	// a deleted slot is empty again
	arena.Delete(ctx, 7)
	oracle.Delete(ctx, 7)
	assert.True(t, arenaIA.PreloadIfAbsent(7, other))
	assert.True(t, oracleIA.PreloadIfAbsent(7, other))
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())

	// an empty vector is refused without growing the cache to cover its id
	lenBefore := arena.Len()
	assert.False(t, arenaIA.PreloadIfAbsent(1<<20, nil))
	assert.False(t, oracleIA.PreloadIfAbsent(1<<20, nil))
	assert.Equal(t, lenBefore, arena.Len(), "empty vec must not grow the cache")
	assert.Equal(t, oracle.Len(), arena.Len())
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())

	// an id beyond the current length grows the cache and stores, like the
	// oracle's preload slow path
	farID := uint64(arena.Len()) + 5000
	farCode := arenaTestCodeFor(farID, testRecordSize)
	assert.True(t, arenaIA.PreloadIfAbsent(farID, farCode))
	assert.True(t, oracleIA.PreloadIfAbsent(farID, farCode))
	got, err = arena.Get(ctx, farID)
	require.NoError(t, err)
	assert.Equal(t, farCode, got)
	assert.Equal(t, oracle.Len(), arena.Len())
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
}

// TestArenaByteCacheMultiVectorPanics pins that the single-vector arena
// cache panics on the multi-vector interface methods, exactly like
// shardedLockCache.
func TestArenaByteCacheMultiVectorPanics(t *testing.T) {
	c := newTestArena(t, testRecordSize)
	assert.Panics(t, func() { c.GetKeys(0) })
	assert.Panics(t, func() { c.SetKeys(0, 0, 0) })
	assert.Panics(t, func() { c.PreloadMulti(0, nil, nil) })
	assert.Panics(t, func() { c.PreloadPassage(0, 0, 0, nil) })
}

// TestArenaByteCacheGetAllInCurrentLock compares the page-read path against
// the oracle at the byte cache's production page size (1).
func TestArenaByteCacheGetAllInCurrentLock(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)
	oracle := newOracle(t, testRecordSize)
	for id := uint64(0); id < 50; id += 2 {
		code := arenaTestCodeFor(id, testRecordSize)
		arena.Preload(id, code)
		oracle.Preload(id, code)
	}
	require.Equal(t, oracle.PageSize(), arena.PageSize())
	out := make([][]byte, 1)
	errs := make([]error, 1)
	wantOut := make([][]byte, 1)
	wantErrs := make([]error, 1)
	for id := uint64(0); id < 60; id++ {
		got, _, gs, ge := arena.GetAllInCurrentLock(ctx, id, out, errs)
		want, _, ws, we := oracle.GetAllInCurrentLock(ctx, id, wantOut, wantErrs)
		assert.Equal(t, ws, gs, "start id %d", id)
		assert.Equal(t, we, ge, "end id %d", id)
		assert.Equal(t, want, got, "page id %d", id)
	}
}

// TestArenaByteCacheDropAndReuse ensures Drop mirrors the map-backed cache's
// dropped state: count reset, coverage released, reads passing through to
// vectorForID without reallocating anything, writes and growth ignored, and a
// repeated Drop returning early instead of notifying the watcher twice.
func TestArenaByteCacheDropAndReuse(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)
	oracle := newOracle(t, testRecordSize)

	for id := uint64(0); id < 300; id++ {
		code := arenaTestCodeFor(id, testRecordSize)
		arena.Preload(id, code)
		oracle.Preload(id, code)
	}
	arena.Drop()
	oracle.Drop()
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
	assert.Equal(t, oracle.Len(), arena.Len())

	// ids beyond the backing limit are gone for good (vecForID errors), ids
	// under it read through the miss path in both caches — without being
	// stored again. id 12 is backed (12 % 7 != 3, see arenaTestVecForID).
	got, gotErr := arena.Get(ctx, 12)
	want, wantErr := oracle.Get(ctx, 12)
	require.NoError(t, wantErr)
	require.NoError(t, gotErr)
	assert.Equal(t, want, got)
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
	assert.Equal(t, oracle.Len(), arena.Len(), "a read-through must not regrow a dropped cache")

	// writes and growth are ignored on a dropped cache instead of
	// resurrecting it (or spinning on a Grow that never covers the id)
	code := arenaTestCodeFor(7, testRecordSize)
	arena.Preload(7, code)
	oracle.Preload(7, code)
	assert.False(t, arena.(IfAbsentPreloader[byte]).PreloadIfAbsent(7, code))
	assert.False(t, oracle.(IfAbsentPreloader[byte]).PreloadIfAbsent(7, code))
	arena.Grow(5000)
	oracle.Grow(5000)
	arena.SetSizeAndGrowNoLock(5000)
	oracle.SetSizeAndGrowNoLock(5000)
	arena.PreloadNoLock(7, code)
	oracle.PreloadNoLock(7, code)
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
	assert.Equal(t, oracle.Len(), arena.Len())

	// a second Drop returns early (the watcher already exited; a second
	// notification would block forever)
	arena.Drop()
	oracle.Drop()
}

// TestArenaByteCacheConcurrency runs readers, writers, deleters and growth
// against disjoint and overlapping id ranges under -race. Writers own
// disjoint id ranges (matching production: an id is preloaded by exactly one
// inserter); readers roam everywhere.
func TestArenaByteCacheConcurrency(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)

	const (
		writers        = 4
		readers        = 4
		idsPerWriter   = 3000
		rounds         = 3
		totalIDs       = writers * idsPerWriter
		deleteEveryNth = 5
	)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := uint64(w * idsPerWriter)
			for r := 0; r < rounds; r++ {
				for i := uint64(0); i < idsPerWriter; i++ {
					id := base + i
					arena.Preload(id, arenaTestCodeFor(id, testRecordSize))
				}
				for i := uint64(0); i < idsPerWriter; i += deleteEveryNth {
					arena.Delete(ctx, base+i)
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(seed), 99))
			for i := 0; i < 30000; i++ {
				id := uint64(rng.IntN(totalIDs + 2000))
				switch i % 4 {
				case 0:
					// read the bytes, not just the length, so -race sees
					// any write into a published record
					vec, err := arena.Get(ctx, id)
					if err == nil && vec != nil && !bytes.Equal(vec, arenaTestCodeFor(id, testRecordSize)) {
						t.Errorf("id %d: torn or foreign record", id)
						return
					}
				case 1:
					arena.Prefetch(id)
				case 2:
					_ = arena.PrefetchGet(id)
				case 3:
					arena.Grow(id)
				}
			}
		}(r)
	}
	wg.Wait()

	// Every id that survived its writer's final delete pass must read back
	// with the expected bytes.
	for id := uint64(0); id < totalIDs; id++ {
		if id%uint64(idsPerWriter)%deleteEveryNth == 0 {
			continue
		}
		vec, err := arena.Get(ctx, id)
		require.NoError(t, err, "id %d", id)
		assert.Equal(t, arenaTestCodeFor(id, testRecordSize), vec, "id %d", id)
	}
}

// TestArenaByteCacheOverlappingFillsRaceFree is the -race regression for
// fills that overlap readers of an already published view: concurrent
// misses on the same ids, a prefill-style Preload of the same codes, and
// Delete followed by a re-fill. Readers read every byte of the views they
// get, so any write into a published record is reported by the race
// detector, and they compare the bytes so a torn record fails the test.
func TestArenaByteCacheOverlappingFillsRaceFree(t *testing.T) {
	ctx := context.Background()
	arena := newTestArena(t, testRecordSize)
	const ids = 64 // a handful of records, so goroutines collide constantly

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for i := 0; i < 4000; i++ {
				id := uint64(rng.IntN(ids))
				switch {
				case g == 0 && i%3 == 0:
					arena.Preload(id, arenaTestCodeFor(id, testRecordSize))
				case g == 1 && i%5 == 0:
					arena.Delete(ctx, id)
				default:
					var vec []byte
					var err error
					if i%2 == 0 {
						vec, err = arena.Get(ctx, id)
					} else {
						vec = arena.PrefetchGet(id)
					}
					if err != nil || vec == nil {
						continue
					}
					if !bytes.Equal(vec, arenaTestCodeFor(id, testRecordSize)) {
						t.Errorf("id %d: torn or foreign record", id)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestArenaByteCacheAllConcurrentWithDropAndWipe pins that All can run
// while the table is grown, wiped and dropped (no out-of-range panic on a
// released table), and that All under a caller-held LockAll doesn't
// deadlock against a concurrent Grow — the LockAll → All pattern the
// sharded cache's tests use.
func TestArenaByteCacheAllConcurrentWithDropAndWipe(t *testing.T) {
	arena := newTestArena(t, testRecordSize).(*arenaByteCache)
	for id := uint64(0); id < 3000; id++ {
		arena.Preload(id, arenaTestCodeFor(id, testRecordSize))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		arena.LockAll()
		growDone := make(chan struct{})
		go func() {
			arena.Grow(100_000) // waits for the stripes held above
			close(growDone)
		}()
		_ = arena.All()
		arena.UnlockAll()
		<-growDone
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("All under LockAll deadlocked against a concurrent Grow")
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for id, vec := range arena.All() {
					if vec != nil && len(vec) != testRecordSize {
						t.Errorf("id %d: %d bytes", id, len(vec))
					}
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		arena.Grow(uint64(200_000 + i*10_000))
		arena.deleteAllVectors()
		arena.Preload(uint64(i), arenaTestCodeFor(uint64(i), testRecordSize))
	}
	arena.Drop()
	time.Sleep(10 * time.Millisecond) // let the readers traverse the dropped table
	close(stop)
	wg.Wait()
	assert.Empty(t, arena.All())
}

// rejectAboveChecker is an AllocChecker that refuses any single
// allocation larger than limit bytes.
type rejectAboveChecker struct {
	limit  int64
	called []int64
	mu     sync.Mutex
}

func (c *rejectAboveChecker) CheckAlloc(size int64) error {
	c.mu.Lock()
	c.called = append(c.called, size)
	c.mu.Unlock()
	if size > c.limit {
		return fmt.Errorf("refused %d bytes", size)
	}
	return nil
}

func (c *rejectAboveChecker) CheckMappingAndReserve(int64, int) error { return nil }
func (c *rejectAboveChecker) Refresh(bool)                            {}

// TestArenaByteCacheMissChargesChunkFootprint pins that a miss into a chunk
// that isn't allocated yet is charged the whole chunk, not the per-record
// estimate, so the memory guard stops chunk allocation under pressure; a
// miss into an allocated chunk keeps the small estimate.
func TestArenaByteCacheMissChargesChunkFootprint(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	checker := &rejectAboveChecker{limit: 64 * 1024} // far below one chunk
	ac, err := NewArenaByteCache(arenaTestVecForID(5000, testRecordSize), testRecordSize,
		1e12, 1, logger, 0, checker)
	require.NoError(t, err)
	t.Cleanup(ac.Drop)
	arena := ac.(*arenaByteCache)

	vec, err := arena.Get(ctx, 1)
	require.Error(t, err, "a miss into a missing chunk must be charged the full chunk")
	assert.Nil(t, vec)
	assert.Nil(t, arena.chunkAt(0), "the refused miss must not allocate the chunk")
	assert.Equal(t, arena.chunkFootprint(), checker.called[len(checker.called)-1])

	// once the chunk exists (preloads aren't alloc-checked, as in the
	// sharded cache), misses into it pass with the per-record estimate
	arena.Preload(0, arenaTestCodeFor(0, testRecordSize))
	vec, err = arena.Get(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, arenaTestCodeFor(1, testRecordSize), vec)
	assert.Equal(t, int64(1024), checker.called[len(checker.called)-1])
}

// TestArenaByteCacheOccupancy pins the occupancy accounting that makes the
// missing chunk reclamation observable: slab bytes follow installed chunks,
// live vectors follow live records, deletes lower only the live count (the
// documented caveat), and a wipe or Drop gives everything back. The
// node-level gauges move by exactly the same deltas.
func TestArenaByteCacheOccupancy(t *testing.T) {
	ctx := context.Background()
	m := monitoring.GetMetrics()
	require.NotNil(t, m)
	slabBase := testutil.ToFloat64(m.VectorCacheArenaSlabBytes)
	liveBase := testutil.ToFloat64(m.VectorCacheArenaLiveVectors)

	arena := newTestArena(t, testRecordSize).(*arenaByteCache)
	chunk := arena.chunkFootprint()
	assertOcc := func(slab, live int64, msg string) {
		t.Helper()
		assert.Equal(t, ArenaOccupancy{SlabBytes: slab, LiveVectors: live}, arena.Occupancy(), msg)
		assert.Equal(t, slabBase+float64(slab), testutil.ToFloat64(m.VectorCacheArenaSlabBytes), msg)
		assert.Equal(t, liveBase+float64(live), testutil.ToFloat64(m.VectorCacheArenaLiveVectors), msg)
	}
	assertOcc(0, 0, "fresh cache holds nothing")

	// ten records in chunk 0, one in chunk 1, one more via the miss path
	for id := uint64(0); id < 10; id++ {
		arena.Preload(id, arenaTestCodeFor(id, testRecordSize))
	}
	arena.Preload(arenaChunkSize+1, arenaTestCodeFor(arenaChunkSize+1, testRecordSize))
	_, err := arena.Get(ctx, 11)
	require.NoError(t, err)
	assertOcc(2*chunk, 12, "two chunks, twelve live records")

	// re-preloading a live record moves nothing
	arena.Preload(3, arenaTestCodeFor(3, testRecordSize))
	assertOcc(2*chunk, 12, "overwrite of a live record")

	// deletes free records but not chunks: the update-churn caveat
	for _, id := range []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, arenaChunkSize + 1} {
		arena.Delete(ctx, id)
	}
	assertOcc(2*chunk, 0, "chunks outlive their records")

	arena.Preload(5, arenaTestCodeFor(5, testRecordSize))
	arena.deleteAllVectors()
	assertOcc(0, 0, "a wipe releases every chunk")

	arena.Preload(5, arenaTestCodeFor(5, testRecordSize))
	assertOcc(chunk, 1, "refilled after the wipe")
	arena.Drop()
	assertOcc(0, 0, "Drop releases everything")
}
