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

package roaringsetrange

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

var cacheTestOperators = []filters.Operator{
	filters.OperatorEqual,
	filters.OperatorNotEqual,
	filters.OperatorLessThan,
	filters.OperatorLessThanEqual,
	filters.OperatorGreaterThan,
	filters.OperatorGreaterThanEqual,
}

// newCachedSegment builds a segment whose leaf cache is independent of the
// process-wide budget, so a test never depends on the operator's environment.
func newCachedSegment(logger logrus.FieldLogger, maxBytes int) *SegmentInMemory {
	s := NewSegmentInMemory(logger)
	s.leafCache = newLeafCache(maxBytes)
	return s
}

// query exercises the real CombinedReader read path, not a shortcut.
func query(t *testing.T, s *SegmentInMemory, value uint64, operator filters.Operator) []uint64 {
	t.Helper()

	readers, release := s.Readers(roaringset.NewBitmapBufPoolNoop())
	creader := NewCombinedReader(readers, release, 1, logrus.New())
	defer creader.Close()

	bm, releaseBm, err := creader.Read(context.Background(), value, operator)
	require.NoError(t, err)
	defer releaseBm()

	return bm.ToArray()
}

func cachedEntries(s *SegmentInMemory) int {
	if s.leafCache == nil {
		return 0
	}
	s.leafCache.lock.Lock()
	defer s.leafCache.lock.Unlock()
	return len(s.leafCache.entries)
}

// TestLeafCacheIsOnTheReadPath poisons a cached entry with a sentinel bitmap;
// if a read still returns the true result, the cache is not on the read path.
func TestLeafCacheIsOnTheReadPath(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	seg := newCachedSegment(logger, 1<<20)
	seg.MergeMemtableEventually(mt1)
	seg.MergeMemtableEventually(mt2)
	seg.MergeMemtableEventually(mt3)
	waitUntilMemtablesMerged(t, seg)

	expected := []uint64{113, 213, 117, 217, 119, 219}

	// first is first sight, second admits and stores, third hits the cache
	assert.ElementsMatch(t, expected, query(t, seg, 13, filters.OperatorGreaterThanEqual))
	assert.ElementsMatch(t, expected, query(t, seg, 13, filters.OperatorGreaterThanEqual))
	assert.ElementsMatch(t, expected, query(t, seg, 13, filters.OperatorGreaterThanEqual))
	require.Equal(t, 1, cachedEntries(seg))

	sentinel := roaringset.NewBitmap(777)
	seg.leafCache.lock.Lock()
	seg.leafCache.entries[0].bm = sentinel
	seg.leafCache.lock.Unlock()

	require.Equal(t, []uint64{777}, query(t, seg, 13, filters.OperatorGreaterThanEqual),
		"cache is not on the read path")

	// and a write must throw the poisoned entry away
	mt4 := NewMemtable(logger)
	mt4.Insert(31, []uint64{131})
	seg.MergeMemtableEventually(mt4)
	waitUntilMemtablesMerged(t, seg)

	assert.ElementsMatch(t, append(expected, 131), query(t, seg, 13, filters.OperatorGreaterThanEqual))
}

// TestLeafCacheInvalidation runs the write -> read -> write -> read gate for
// both writers that mutate the planes, with additions and deletions.
func TestLeafCacheInvalidation(t *testing.T) {
	writers := []struct {
		name  string
		apply func(t *testing.T, s *SegmentInMemory, mt *Memtable)
	}{
		{
			name: "mergeMemtables",
			apply: func(t *testing.T, s *SegmentInMemory, mt *Memtable) {
				s.MergeMemtableEventually(mt)
				waitUntilMemtablesMerged(t, s)
			},
		},
		{
			name: "MergeSegmentByCursor",
			apply: func(t *testing.T, s *SegmentInMemory, mt *Memtable) {
				require.NoError(t, s.MergeSegmentByCursor(newFakeSegmentCursor(mt)))
			},
		},
	}

	for _, w := range writers {
		t.Run(w.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			seg := newCachedSegment(logger, 1<<20)

			mt1 := NewMemtable(logger)
			mt1.Insert(10, []uint64{100})
			mt1.Insert(20, []uint64{200})
			w.apply(t, seg, mt1)

			// warm every operator past the second-sight admission
			for round := 0; round < 3; round++ {
				for _, op := range cacheTestOperators {
					query(t, seg, 15, op)
				}
			}
			require.NotZero(t, cachedEntries(seg), "nothing was cached, the gate would be vacuous")

			assert.ElementsMatch(t, []uint64{200},
				query(t, seg, 15, filters.OperatorGreaterThanEqual))

			t.Run("addition becomes visible", func(t *testing.T) {
				mt2 := NewMemtable(logger)
				mt2.Insert(30, []uint64{300})
				w.apply(t, seg, mt2)

				assert.ElementsMatch(t, []uint64{200, 300},
					query(t, seg, 15, filters.OperatorGreaterThanEqual))
				assert.ElementsMatch(t, []uint64{100},
					query(t, seg, 15, filters.OperatorLessThan))
			})

			t.Run("deletion becomes visible", func(t *testing.T) {
				// warm again against the post-addition state
				for round := 0; round < 3; round++ {
					query(t, seg, 15, filters.OperatorGreaterThanEqual)
				}

				mt3 := NewMemtable(logger)
				mt3.Delete(30, []uint64{300})
				w.apply(t, seg, mt3)

				assert.ElementsMatch(t, []uint64{200},
					query(t, seg, 15, filters.OperatorGreaterThanEqual))
			})

			t.Run("overwrite becomes visible", func(t *testing.T) {
				for round := 0; round < 3; round++ {
					query(t, seg, 15, filters.OperatorGreaterThanEqual)
				}

				mt4 := NewMemtable(logger)
				mt4.Delete(20, []uint64{200})
				mt4.Insert(1, []uint64{200})
				w.apply(t, seg, mt4)

				assert.ElementsMatch(t, []uint64{},
					query(t, seg, 15, filters.OperatorGreaterThanEqual))
				assert.ElementsMatch(t, []uint64{100, 200},
					query(t, seg, 15, filters.OperatorLessThan))
			})
		})
	}
}

// TestLeafCacheDifferential runs a cached and uncached segment through
// identical randomized mutations and queries; any divergence means the cache
// served something stale.
func TestLeafCacheDifferential(t *testing.T) {
	const (
		seeds     = 64
		steps     = 12
		maxValue  = 40
		maxDocIDs = 60
	)

	logger, _ := test.NewNullLogger()

	for seed := int64(0); seed < seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))

			cached := newCachedSegment(logger, 1<<20)
			uncached := newCachedSegment(logger, 0)
			require.Nil(t, uncached.leafCache)

			live := map[uint64]uint64{} // docID -> value, the oracle's view

			for step := 0; step < steps; step++ {
				mt := NewMemtable(logger)
				for i := 0; i < 1+rnd.Intn(6); i++ {
					docID := uint64(rnd.Intn(maxDocIDs))
					if _, ok := live[docID]; ok && rnd.Intn(3) == 0 {
						mt.Delete(live[docID], []uint64{docID})
						delete(live, docID)
						continue
					}
					value := uint64(rnd.Intn(maxValue))
					mt.Insert(value, []uint64{docID})
					live[docID] = value
				}

				if rnd.Intn(2) == 0 {
					// flush path
					cached.MergeMemtableEventually(mt.Clone())
					uncached.MergeMemtableEventually(mt.Clone())
					waitUntilMemtablesMerged(t, cached)
					waitUntilMemtablesMerged(t, uncached)
				} else {
					// compaction path
					require.NoError(t, cached.MergeSegmentByCursor(newFakeSegmentCursor(mt.Clone())))
					require.NoError(t, uncached.MergeSegmentByCursor(newFakeSegmentCursor(mt.Clone())))
				}

				for q := 0; q < 24; q++ {
					value := uint64(rnd.Intn(maxValue + 2))
					op := cacheTestOperators[rnd.Intn(len(cacheTestOperators))]

					// repeat so the same query crosses the admission threshold
					// and is served from the cache on later rounds
					for round := 0; round < 3; round++ {
						want := readBytes(t, uncached, value, op)
						got := readBytes(t, cached, value, op)
						if !assert.Equal(t, want, got,
							"seed=%d step=%d value=%d op=%s round=%d",
							seed, step, value, op.Name(), round) {
							t.FailNow()
						}
					}
				}
			}

			require.NotZero(t, cachedEntries(cached),
				"seed=%d cached nothing, the comparison would be vacuous", seed)
		})
	}
}

// readBytes returns the serialized bitmap so the comparison is byte-identical
// rather than set-equal: a representation difference would also be a bug.
func readBytes(t *testing.T, s *SegmentInMemory, value uint64, operator filters.Operator) []byte {
	t.Helper()

	readers, release := s.Readers(roaringset.NewBitmapBufPoolNoop())
	creader := NewCombinedReader(readers, release, 1, logrus.New())
	defer creader.Close()

	bm, releaseBm, err := creader.Read(context.Background(), value, operator)
	require.NoError(t, err)
	defer releaseBm()

	buf := bm.ToBuffer()
	out := make([]byte, len(buf))
	copy(out, buf)
	return out
}

// TestLeafCacheConcurrentReadersAndWriter runs N readers against 1 writer
// under -race, asserting a reader never sees a bitmap that contradicts a
// write it can already observe.
func TestLeafCacheConcurrentReadersAndWriter(t *testing.T) {
	logger, _ := test.NewNullLogger()
	seg := newCachedSegment(logger, 1<<20)

	// docIDs 0..99 all start at value 0
	mt0 := NewMemtable(logger)
	for i := uint64(0); i < 100; i++ {
		mt0.Insert(0, []uint64{i})
	}
	require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(mt0)))

	var (
		stop   atomic.Bool
		wg     sync.WaitGroup
		reads  atomic.Int64
		writes atomic.Int64
	)

	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				// every doc has a value in [0, 63], so >= 0 must always be all
				// 100 docs no matter which write won the race
				got := query(t, seg, 0, filters.OperatorGreaterThanEqual)
				assert.Len(t, got, 100)
				reads.Add(1)
				// bitmapsLock is read-preferring, so a reader loop with no gap
				// starves the writer indefinitely
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			mt := NewMemtable(logger)
			for d := uint64(0); d < 100; d++ {
				mt.Insert(uint64((i+int(d))%64), []uint64{d})
			}
			if i%2 == 0 {
				assert.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(mt)))
			} else {
				seg.MergeMemtableEventually(mt)
			}
			writes.Add(1)
		}
	}()

	time.Sleep(3 * time.Second)
	stop.Store(true)
	wg.Wait()

	// floors rather than exact counts: the point is that both sides made real
	// progress against each other, not how much
	require.Greater(t, reads.Load(), int64(100))
	require.Greater(t, writes.Load(), int64(20))
}

// TestLeafCacheBudgetConstrainedConcurrentReads runs concurrent readers
// against a cache whose budget fits only a fraction of the values queried.
func TestLeafCacheBudgetConstrainedConcurrentReads(t *testing.T) {
	logger, _ := test.NewNullLogger()

	probe := newCachedSegment(logger, 1<<30)
	mt := NewMemtable(logger)
	for i := uint64(0); i < 200; i++ {
		mt.Insert(i%64, []uint64{i})
	}
	require.NoError(t, probe.MergeSegmentByCursor(newFakeSegmentCursor(mt)))
	entrySize := len(readBytes(t, probe, 5, filters.OperatorGreaterThanEqual))

	// budget for roughly two leaves; once full, new values are declined rather
	// than evicting an existing entry
	seg := newCachedSegment(logger, 2*entrySize)
	require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(mt)))

	reference := map[uint64][]uint64{}
	for v := uint64(0); v < 64; v++ {
		reference[v] = query(t, seg, v, filters.OperatorGreaterThanEqual)
	}

	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(r)))
			for i := 0; i < 400; i++ {
				v := uint64(rnd.Intn(64))
				assert.ElementsMatch(t, reference[v],
					query(t, seg, v, filters.OperatorGreaterThanEqual))
			}
		}(r)
	}
	wg.Wait()

	seg.leafCache.lock.Lock()
	defer seg.leafCache.lock.Unlock()
	assert.LessOrEqual(t, seg.leafCache.bytes, 2*entrySize)
}

// TestLeafCacheEntryIsNotAliased pins why the cache must clone: the reader's
// bitmap is mutated in place by CombinedReader's memtable merge.
func TestLeafCacheEntryIsNotAliased(t *testing.T) {
	logger, _ := test.NewNullLogger()
	seg := newCachedSegment(logger, 1<<20)

	base := NewMemtable(logger)
	base.Insert(20, []uint64{200})
	require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(base)))

	// pending, never merged into the planes: CombinedReader has to OR it into
	// whatever the segment reader hands back
	pending := NewMemtable(logger)
	pending.Insert(30, []uint64{300})
	seg.memtablesLock.Lock()
	seg.memtables = append(seg.memtables, pending)
	seg.memtablesLock.Unlock()

	var last []uint64
	for round := 0; round < 5; round++ {
		last = query(t, seg, 15, filters.OperatorGreaterThanEqual)
		require.ElementsMatch(t, []uint64{200, 300}, last,
			"round %d: the cached leaf was mutated by an earlier read", round)
	}

	seg.leafCache.lock.Lock()
	defer seg.leafCache.lock.Unlock()
	require.Len(t, seg.leafCache.entries, 1)
	assert.ElementsMatch(t, []uint64{200}, seg.leafCache.entries[0].bm.ToArray(),
		"the cache entry must hold the segment leaf only, without memtable deltas")
}

// TestLeafCacheHitReturnsPooledBuffers pins that a cache hit borrows and
// releases exactly one buffer, same as the uncached path.
func TestLeafCacheHitReturnsPooledBuffers(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	seg := newCachedSegment(logger, 1<<20)
	seg.MergeMemtableEventually(mt1)
	seg.MergeMemtableEventually(mt2)
	seg.MergeMemtableEventually(mt3)
	waitUntilMemtablesMerged(t, seg)

	bufPool := newBitmapBufPoolWithCounter()
	readers, release := seg.Readers(bufPool)
	defer release()
	require.Len(t, readers, 1)

	for _, op := range cacheTestOperators {
		t.Run(op.Name(), func(t *testing.T) {
			for round := 0; round < 3; round++ {
				_, releaseBm, err := readers[0].Read(context.Background(), 13, op)
				require.NoError(t, err)
				assert.GreaterOrEqual(t, 1, bufPool.InUseCounter(), "round %d", round)
				releaseBm()
				assert.Equal(t, 0, bufPool.InUseCounter(), "round %d", round)
			}
		})
	}
	require.NotZero(t, cachedEntries(seg))
}

// TestLeafCacheLongTailRetainsNothing is the end-to-end check that a
// no-repeat workload leaves the cache empty.
func TestLeafCacheLongTailRetainsNothing(t *testing.T) {
	logger, _ := test.NewNullLogger()
	seg := newCachedSegment(logger, 1<<20)

	mt := NewMemtable(logger)
	for i := uint64(0); i < 500; i++ {
		mt.Insert(i, []uint64{i})
	}
	require.NoError(t, seg.MergeSegmentByCursor(newFakeSegmentCursor(mt)))

	for i := uint64(0); i < 500; i++ {
		query(t, seg, i, filters.OperatorGreaterThanEqual)
	}

	assert.Zero(t, cachedEntries(seg))
	seg.leafCache.lock.Lock()
	defer seg.leafCache.lock.Unlock()
	assert.Zero(t, seg.leafCache.bytes)
}

func TestLeafCacheDisabledMatchesShippedPath(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	seg := newCachedSegment(logger, 0)
	require.Nil(t, seg.leafCache)

	seg.MergeMemtableEventually(mt1)
	seg.MergeMemtableEventually(mt2)
	seg.MergeMemtableEventually(mt3)
	waitUntilMemtablesMerged(t, seg)

	for round := 0; round < 3; round++ {
		assert.ElementsMatch(t, []uint64{113, 213, 117, 217, 119, 219},
			query(t, seg, 13, filters.OperatorGreaterThanEqual))
	}
}
