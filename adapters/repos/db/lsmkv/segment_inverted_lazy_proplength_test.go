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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

			// on-demand load populates the full map
			pl, err := seg.getPropertyLengths()
			require.NoError(t, err)
			require.Len(t, pl, size)
			require.True(t, seg.isPropertyLengthsLoaded())

			// freeing releases the map but keeps the stats
			seg.freePropertyLengths()
			require.False(t, seg.isPropertyLengthsLoaded())
			require.Nil(t, seg.getInvertedData().propertyLengths)
			avgAfter, countAfter := b.GetAveragePropertyLength()
			require.Equal(t, avg, avgAfter)
			require.Equal(t, count, countAfter)

			// the map reloads on demand after a free
			pl, err = seg.getPropertyLengths()
			require.NoError(t, err)
			require.Len(t, pl, size)

			// data is still readable end-to-end
			kvs, err := b.MapList(ctx, key)
			require.NoError(t, err)
			require.Len(t, kvs, size)
		})
	}
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
