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
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/storobj"
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

// TestArenaByteCacheRecordSizeEnforced pins the one deliberate divergence
// from shardedLockCache: a fixed-stride arena cannot store records of the
// wrong length, so it panics loudly instead of accepting them. The sharded
// cache would silently store any slice; every production caller stores
// codes of one fixed width, so this can only fire on a programming error.
func TestArenaByteCacheRecordSizeEnforced(t *testing.T) {
	c := newTestArena(t, testRecordSize)
	assert.Panics(t, func() { c.Preload(1, make([]byte, testRecordSize-1)) })
	assert.Panics(t, func() { c.Preload(1, make([]byte, testRecordSize+1)) })
	assert.Panics(t, func() { c.Preload(1, nil) })

	// PreloadIfAbsent shares the size check for non-empty records, but an
	// empty record is refused (false) rather than fatal, mirroring the
	// oracle's early return.
	ia := c.(IfAbsentPreloader[byte])
	assert.Panics(t, func() { ia.PreloadIfAbsent(1, make([]byte, testRecordSize-1)) })
	assert.Panics(t, func() { ia.PreloadIfAbsent(1, make([]byte, testRecordSize+1)) })
	assert.NotPanics(t, func() { assert.False(t, ia.PreloadIfAbsent(1, nil)) })
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

// TestArenaByteCacheDropAndReuse ensures Drop clears content and the cache
// remains usable, mirroring deleteAllVectors semantics (count reset, entries
// gone, coverage retained).
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
	// under it come back through the miss path in both caches. id 12 is
	// backed (12 % 7 != 3, see arenaTestVecForID).
	got, gotErr := arena.Get(ctx, 12)
	want, wantErr := oracle.Get(ctx, 12)
	require.NoError(t, wantErr)
	require.NoError(t, gotErr)
	assert.Equal(t, want, got)
	assert.Equal(t, oracle.CountVectors(), arena.CountVectors())
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
					vec, err := arena.Get(ctx, id)
					if err == nil && vec != nil && len(vec) != testRecordSize {
						t.Errorf("torn read: %d bytes", len(vec))
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
