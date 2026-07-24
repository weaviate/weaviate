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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// TestInvertedLazyPropertyLengths covers the stats-at-open / load-on-demand /
// free-keeps-stats lifecycle and that the flag toggles eager vs lazy.
func TestInvertedLazyPropertyLengths(t *testing.T) {
	ctx := context.Background()
	const size = 200
	key := []byte("my-key")

	for _, tt := range []struct {
		name string
		lazy bool
	}{
		{name: "eager", lazy: false},
		{name: "lazy", lazy: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lazy := configRuntime.NewDynamicValue(tt.lazy)

			dirName := t.TempDir()
			b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted), WithForceCompaction(true), WithLazyPropertyLengths(lazy))
			require.Nil(t, err)
			b.SetMemtableThreshold(1e9)

			for i := 0; i < size; i++ {
				pair := NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1), false)
				require.Nil(t, b.MapSet(key, pair))
			}
			require.Nil(t, b.FlushAndSwitch())

			// re-open so the segment is loaded from disk via newSegment
			require.Nil(t, b.Shutdown(ctx))
			b, err = NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
			require.Nil(t, err)
			b.SetMemtableThreshold(1e9)
			defer b.Shutdown(ctx)

			// stats are always resident at open, independent of lazy mode
			avg, count := b.GetAveragePropertyLength()
			require.Equal(t, 1.0, avg)
			require.Equal(t, uint64(size), count)

			require.GreaterOrEqual(t, len(b.disk.segments), 1)
			seg := b.disk.segments[0]

			require.Equal(t, !tt.lazy, seg.isPropertyLengthsLoaded(),
				"lazy mode must not load the full map at open; eager must")

			pl, err := seg.getPropertyLengths()
			require.NoError(t, err)
			require.Len(t, pl, size)
			require.True(t, seg.isPropertyLengthsLoaded())

			// freeing releases the per-document arrays but keeps the stats
			seg.freePropertyLengths()
			require.False(t, seg.isPropertyLengthsLoaded())
			require.Nil(t, seg.getInvertedData().propLengthsDense)
			require.Nil(t, seg.getInvertedData().propLengthsPairIds)
			require.Nil(t, seg.getInvertedData().propLengthsPairIds32)
			avgAfter, countAfter := b.GetAveragePropertyLength()
			require.Equal(t, avg, avgAfter)
			require.Equal(t, count, countAfter)

			// reloads on demand after a free
			pl, err = seg.getPropertyLengths()
			require.NoError(t, err)
			require.Len(t, pl, size)

			kvs, err := b.MapList(ctx, key)
			require.NoError(t, err)
			require.Len(t, kvs, size)
		})
	}
}

// TestInvertedFreePropertyLengthsClearsNarrowPairs pins that freePropertyLengths
// clears the uint32 pairs layout, not just dense. TestInvertedLazyPropertyLengths
// above uses contiguous docIDs (the dense path), so its post-free nil check on
// propLengthsPairIds32 is vacuous — that field is never populated there. Strided
// docIDs with a max under uint32 force the narrow-pairs layout, exercising the
// field for real.
func TestInvertedFreePropertyLengthsClearsNarrowPairs(t *testing.T) {
	ctx := context.Background()
	const size = 100
	const stride = 1_000_000 // max docID 99_000_000 < MaxUint32; span/3 >> size -> pairs, not dense
	key := []byte("my-key")

	dirName := t.TempDir()
	mk := func() *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyInverted))
		require.Nil(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	b := mk()
	for i := 0; i < size; i++ {
		docID := uint64(i) * stride
		require.Nil(t, b.MapSet(key, NewMapPairFromDocIdAndTf(docID, float32(1), float32(1+i%5), false)))
	}
	require.Nil(t, b.FlushAndSwitch())

	// reopen so the segment is loaded from disk via newSegment
	require.Nil(t, b.Shutdown(ctx))
	b = mk()
	defer b.Shutdown(ctx)

	require.GreaterOrEqual(t, len(b.disk.segments), 1)
	seg := b.disk.segments[0]

	_, err := seg.getPropertyLengths()
	require.NoError(t, err)
	inv := seg.getInvertedData()
	require.NotNil(t, inv.propLengthsPairIds32, "strided max under uint32 must select the narrow pairs layout")
	require.Nil(t, inv.propLengthsDense, "sparse span must not select dense")
	require.Nil(t, inv.propLengthsPairIds, "max under uint32 must not use wide pairs")

	seg.freePropertyLengths()
	require.False(t, seg.isPropertyLengthsLoaded())
	require.Nil(t, seg.getInvertedData().propLengthsPairIds32, "free must clear the uint32 pairs layout")
	require.Nil(t, seg.getInvertedData().propLengthsPairLens, "free must clear the pairs lengths")
}

// TestInvertedCompactionFreesLazyPropertyLengths verifies that an inverted
// compaction releases a property length map it loaded itself, while leaving a
// map that was already loaded before the compaction untouched. The aborted
// compaction keeps both inputs alive so the effect is observable.
func TestInvertedCompactionFreesLazyPropertyLengths(t *testing.T) {
	ctx := context.Background()
	const perSegment = 100
	key := []byte("my-key")

	dirName := t.TempDir()
	b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted), WithLazyPropertyLengths(configRuntime.NewDynamicValue(true)))
	require.Nil(t, err)
	b.SetMemtableThreshold(1e9)
	defer b.Shutdown(ctx)

	// two flushes -> two same-level disk segments, each loaded stats-only
	for seg := 0; seg < 2; seg++ {
		for i := 0; i < perSegment; i++ {
			docID := uint64(seg*perSegment + i)
			pair := NewMapPairFromDocIdAndTf(docID, float32(1), float32(1), false)
			require.Nil(t, b.MapSet(key, pair))
		}
		require.Nil(t, b.FlushAndSwitch())
	}

	candidates, _ := b.disk.findCompactionCandidates()
	require.NotNil(t, candidates, "expected two same-level segments to be compaction candidates")

	segKept := b.disk.segments[candidates[0]]
	segFreed := b.disk.segments[candidates[1]]
	require.False(t, segKept.isPropertyLengthsLoaded())
	require.False(t, segFreed.isPropertyLengthsLoaded())

	// pre-load one input's map: a load that predates the compaction
	_, err = segKept.getPropertyLengths()
	require.NoError(t, err)
	require.True(t, segKept.isPropertyLengthsLoaded())

	// abort the compaction so both inputs survive; the compaction still loads
	// both maps when it opens its cursors
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	compacted, err := b.disk.compactOnce(cancelledCtx)
	require.NoError(t, err)
	require.False(t, compacted)
	require.GreaterOrEqual(t, len(b.disk.segments), 2)

	require.True(t, segKept.isPropertyLengthsLoaded(),
		"a map loaded before the compaction must not be freed")
	require.False(t, segFreed.isPropertyLengthsLoaded(),
		"a map loaded only for the compaction must be freed afterwards")
}

// TestInvertedLazyPropertyLengthsStatsNoRace exercises the unlocked stat read
// (as done by combinePropertyLengths) concurrently with the on-demand full
// load. The full load must not re-write avg/count, or -race flags it.
func TestInvertedLazyPropertyLengthsStatsNoRace(t *testing.T) {
	ctx := context.Background()
	const size = 200
	key := []byte("my-key")
	lazy := configRuntime.NewDynamicValue(true)

	dirName := t.TempDir()
	mk := func() *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
		require.Nil(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	b := mk()
	for i := 0; i < size; i++ {
		require.Nil(t, b.MapSet(key, NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1), false)))
	}
	require.Nil(t, b.FlushAndSwitch())
	require.Nil(t, b.Shutdown(ctx))

	b = mk()
	defer b.Shutdown(ctx)

	require.GreaterOrEqual(t, len(b.disk.segments), 1)
	seg := b.disk.segments[0]

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = seg.getInvertedData().avgPropertyLengthsAvg
				_ = seg.getInvertedData().avgPropertyLengthsCount
			}
		}
	}, nullLogger())

	var loadErr error
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 300; i++ {
			seg.freePropertyLengths()
			if _, err := seg.getPropertyLengths(); err != nil {
				loadErr = err
				return
			}
		}
	}, nullLogger())

	wg.Wait()
	require.NoError(t, loadErr)
}

// TestInvertedPropertyLengthsViewNoEmptyUnderFree pins that the load-on-demand
// slow path never hands a reader an empty view because a concurrent
// freePropertyLengths niled the arrays mid-load (which would silently score
// every doc with propLength 0). Freer and readers race in separate goroutines;
// every stored length is >= 1, so any 0 a reader sees means the view was lost.
func TestInvertedPropertyLengthsViewNoEmptyUnderFree(t *testing.T) {
	ctx := context.Background()
	const size = 500
	key := []byte("my-key")
	lazy := configRuntime.NewDynamicValue(true)

	dirName := t.TempDir()
	mk := func() *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
		require.Nil(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	want := make([]uint32, size)
	b := mk()
	for i := 0; i < size; i++ {
		propLen := float32(1 + i%50)
		want[i] = uint32(propLen)
		require.Nil(t, b.MapSet(key, NewMapPairFromDocIdAndTf(uint64(i), float32(1), propLen, false)))
	}
	require.Nil(t, b.FlushAndSwitch())
	require.Nil(t, b.Shutdown(ctx))

	b = mk()
	defer b.Shutdown(ctx)
	require.GreaterOrEqual(t, len(b.disk.segments), 1)
	seg := b.disk.segments[0]

	// hammer free so readers keep falling into the load-on-demand slow path
	var freerWg sync.WaitGroup
	stop := make(chan struct{})
	freerWg.Add(1)
	enterrors.GoWrapper(func() {
		defer freerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				seg.freePropertyLengths()
			}
		}
	}, nullLogger())

	// each reader builds its own view (own cursor) and scans ascending
	var mismatches atomic.Int64
	var readersWg sync.WaitGroup
	const readers = 16
	for r := 0; r < readers; r++ {
		readersWg.Add(1)
		enterrors.GoWrapper(func() {
			defer readersWg.Done()
			for iter := 0; iter < 3000; iter++ {
				view, err := seg.propLengthsView()
				if err != nil {
					mismatches.Add(1)
					continue
				}
				for id := uint64(0); id < size; id++ {
					if view.get(id) != want[id] {
						mismatches.Add(1)
					}
				}
			}
		}, nullLogger())
	}

	readersWg.Wait()
	close(stop)
	freerWg.Wait()

	require.Zero(t, mismatches.Load(),
		"property-length view returned wrong/zero lengths under concurrent free")
}

// TestInvertedLazyPropertyLengthsBlockMaxQuery runs a BM25 query in lazy mode
// and asserts it drives the on-demand load and returns the same results as eager.
// Distinct per-doc lengths mean a nil map would surface as PropLength==0.
func TestInvertedLazyPropertyLengthsBlockMaxQuery(t *testing.T) {
	ctx := context.Background()
	const size = 200
	key := []byte("my-key")

	type expectedDoc struct{ freq, propLen float32 }
	expected := make(map[uint64]expectedDoc, size)
	for i := 0; i < size; i++ {
		expected[uint64(i)] = expectedDoc{freq: float32(1 + i%3), propLen: float32(1 + i%5)}
	}

	query := func(t *testing.T, b *Bucket) map[uint64]*terms.DocPointerWithScore {
		t.Helper()
		view := b.GetConsistentView()
		defer view.ReleaseView()

		bm25config := schema.BM25Config{K1: 1.2, B: 0.75}
		avgPropLen, _ := b.GetAveragePropertyLength()
		N := size * 10
		diskTerms, _, _, err := b.createDiskTermFromCV(ctx, view, float64(N), nil,
			[]string{string(key)}, "", 1, []int{1}, bm25config)
		require.NoError(t, err)

		got := make(map[uint64]*terms.DocPointerWithScore, size)
		for _, diskTerm := range diskTerms {
			topKHeap, err := DoBlockMaxWand(ctx, N, diskTerm, avgPropLen, true, 1, 1, b.logger)
			require.NoError(t, err)
			for topKHeap.Len() > 0 {
				item := topKHeap.Pop()
				got[item.ID] = item.Value[0]
			}
		}
		return got
	}

	for _, tt := range []struct {
		name string
		lazy bool
	}{
		{name: "eager", lazy: false},
		{name: "lazy", lazy: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lazy := configRuntime.NewDynamicValue(tt.lazy)
			dirName := t.TempDir()

			b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
			require.Nil(t, err)
			b.SetMemtableThreshold(1e9)
			for id, exp := range expected {
				require.Nil(t, b.MapSet(key, NewMapPairFromDocIdAndTf(id, exp.freq, exp.propLen, false)))
			}
			require.Nil(t, b.FlushAndSwitch())

			// reopen so the segment is loaded from disk via newSegment
			require.Nil(t, b.Shutdown(ctx))
			b, err = NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
			require.Nil(t, err)
			b.SetMemtableThreshold(1e9)
			defer b.Shutdown(ctx)

			require.GreaterOrEqual(t, len(b.disk.segments), 1)
			seg := b.disk.segments[0]

			// lazy mode defers the per-doc map; the query must be what loads it
			require.Equal(t, !tt.lazy, seg.isPropertyLengthsLoaded())

			got := query(t, b)

			require.True(t, seg.isPropertyLengthsLoaded(),
				"a BM25 query must drive the on-demand property-length load")
			require.Len(t, got, size)
			for id, exp := range expected {
				doc, ok := got[id]
				require.True(t, ok, "docId %d missing from blockmax results", id)
				require.Equal(t, exp.freq, doc.Frequency, "docId %d frequency", id)
				require.Equal(t, exp.propLen, doc.PropLength,
					"docId %d property length (0 would mean the lazy load returned a nil map)", id)
			}
		})
	}
}

// TestInvertedLazyPropertyLengthsFilterPathDoesNotLoad asserts key-only reads
// (map cursors, MapListSkipPropertyLengths) leave the property-length map
// unloaded, while a plain MapList loads it.
func TestInvertedLazyPropertyLengthsFilterPathDoesNotLoad(t *testing.T) {
	ctx := context.Background()
	const size = 200
	key := []byte("my-key")

	dirName := t.TempDir()
	lazy := configRuntime.NewDynamicValue(true)
	b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
	require.Nil(t, err)
	b.SetMemtableThreshold(1e9)
	for i := 0; i < size; i++ {
		require.Nil(t, b.MapSet(key, NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1+i%5), false)))
	}
	require.Nil(t, b.FlushAndSwitch())

	// reopen so the segment is loaded from disk via newSegment (stats-only)
	require.Nil(t, b.Shutdown(ctx))
	b, err = NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyInverted), WithLazyPropertyLengths(lazy))
	require.Nil(t, err)
	b.SetMemtableThreshold(1e9)
	defer b.Shutdown(ctx)

	require.GreaterOrEqual(t, len(b.disk.segments), 1)
	seg := b.disk.segments[0]
	require.False(t, seg.isPropertyLengthsLoaded())

	// greaterThan/lessThan/like filters go through a map cursor
	cur, err := b.MapCursor()
	require.NoError(t, err)
	rows := 0
	for k, _ := cur.First(ctx); k != nil; k, _ = cur.Next(ctx) {
		rows++
	}
	cur.Close()
	require.Equal(t, 1, rows, "single term expected")
	require.False(t, seg.isPropertyLengthsLoaded(),
		"a map cursor must not load the per-doc property length map")

	// equal/notEqual filters go through MapList with the skip option
	kvs, err := b.MapList(ctx, key, MapListSkipPropertyLengths())
	require.NoError(t, err)
	require.Len(t, kvs, size)
	require.False(t, seg.isPropertyLengthsLoaded(),
		"MapListSkipPropertyLengths must not load the per-doc property length map")

	// BM25 and reindexing use a plain MapList
	kvs, err = b.MapList(ctx, key)
	require.NoError(t, err)
	require.Len(t, kvs, size)
	require.True(t, seg.isPropertyLengthsLoaded(),
		"a plain MapList must drive the on-demand load")
}
