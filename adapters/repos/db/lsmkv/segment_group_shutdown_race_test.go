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

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// mustSegmentView unwraps getConsistentViewOfSegments for tests that run
// against a live (non-shutting-down) segment group.
func mustSegmentView(t *testing.T, sg *SegmentGroup) ([]Segment, func()) {
	t.Helper()
	segments, release, err := sg.getConsistentViewOfSegments()
	require.NoError(t, err)
	return segments, release
}

func newShutdownTestBucket(t *testing.T, ctx context.Context, opts ...BucketOption) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	return b
}

// TestShutdownRefusesViewsInRefWaitWindow reproduces weaviate/0-weaviate-issues#285:
// shutdown waits for segment refcounts to reach zero and then munmaps, but a
// reader could take a NEW view between the wait's last zero-observation and the
// close (use-after-munmap → SIGBUS). The test hook makes that window
// deterministic: a read issued inside it must be refused, not served.
func TestShutdownRefusesViewsInRefWaitWindow(t *testing.T) {
	ctx := context.Background()
	b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))

	require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
	require.NoError(t, b.FlushAndSwitch())

	var hookRan bool
	var windowVal []byte
	var windowErr error
	b.disk.testHookShutdownAfterRefWait = func() {
		hookRan = true
		windowVal, windowErr = b.Get([]byte("key-1"))
	}

	require.NoError(t, b.Shutdown(ctx))
	require.True(t, hookRan)

	require.ErrorIs(t, windowErr, ErrShuttingDown,
		"a consistent view acquired between shutdown's refcount wait and segment "+
			"close reads memory that is about to be munmapped")
	assert.Nil(t, windowVal)
}

// TestReadAfterShutdownErrs covers the second half of #285: a reader arriving
// after sg.segments = nil must get an error, not a silently empty result.
func TestReadAfterShutdownErrs(t *testing.T) {
	ctx := context.Background()
	b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))

	require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	val, err := b.Get([]byte("key-1"))
	require.ErrorIs(t, err, ErrShuttingDown)
	assert.Nil(t, val)
}

// TestCursorsAfterShutdownRefuse: the no-error cursor constructors are the
// unpinned read path of aggregations and filters on displaced reindex buckets.
// They must refuse loudly (panic carries ErrShuttingDown, recovered by the
// enterrors/HTTP layers), never serve a silently empty iteration.
func TestCursorsAfterShutdownRefuse(t *testing.T) {
	ctx := context.Background()

	t.Run("replace cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))
		require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		requirePanicsWithShuttingDown(t, func() { b.Cursor() })
	})

	t.Run("roaringset cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyRoaringSet))
		require.NoError(t, b.RoaringSetAddOne([]byte("key-1"), 1))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		requirePanicsWithShuttingDown(t, func() { b.CursorRoaringSet() })
	})

	t.Run("set cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategySetCollection))
		require.NoError(t, b.SetAdd([]byte("key-1"), [][]byte{[]byte("v1")}))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		requirePanicsWithShuttingDown(t, func() { b.SetCursor() })
	})

	t.Run("map cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyMapCollection))
		require.NoError(t, b.MapSet([]byte("key-1"), MapPair{Key: []byte("k"), Value: []byte("v")}))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		// MapCursor has an error channel, so it must refuse via error, not panic
		_, err := b.MapCursor()
		require.ErrorIs(t, err, ErrShuttingDown)
	})
}

func requirePanicsWithShuttingDown(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		require.NotNil(t, r, "cursor constructor on a shut-down bucket must refuse, "+
			"not serve a silently empty iteration")
		err, ok := r.(error)
		require.True(t, ok, "panic value should be an error, got %T", r)
		require.ErrorIs(t, err, ErrShuttingDown)
	}()
	f()
}

// TestShutdownVsReadersStress races real readers against shutdown. Every read
// must either fully succeed or fail with ErrShuttingDown; on unfixed code this
// can dereference munmapped memory (SIGBUS). Run with -race.
func TestShutdownVsReadersStress(t *testing.T) {
	ctx := context.Background()
	b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))

	require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
	require.NoError(t, b.FlushAndSwitch())

	const readers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				val, err := b.Get([]byte("key-1"))
				if err != nil {
					if !errors.Is(err, ErrShuttingDown) {
						errCh <- err
					}
					return
				}
				if string(val) != "value-1" {
					errCh <- errors.New("read returned wrong or empty value")
					return
				}
			}
		}()
	}

	close(start)
	require.NoError(t, b.Shutdown(ctx))
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("reader failed: %v", err)
	}
}
