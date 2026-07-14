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

// mustSegmentView unwraps getConsistentViewOfSegments for tests on a live segment group.
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

// A view taken between shutdown's refcount wait and close would read munmapped memory (weaviate/0-weaviate-issues#285).
func TestShutdownRefusesViewsInRefWaitWindow(t *testing.T) {
	ctx := context.Background()
	b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))

	require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
	require.NoError(t, b.FlushAndSwitch())

	var hookRan bool
	var windowVal []byte
	var windowErr error

	// Drive the segment group's shutdown directly, passing the window hook as a
	// parameter. This keeps the test seam off the production struct while still
	// exercising the exact refcount-wait-to-close window.
	require.NoError(t, b.disk.shutdown(ctx, func() {
		hookRan = true
		windowVal, windowErr = b.Get([]byte("key-1"))
	}))
	require.True(t, hookRan)

	require.ErrorIs(t, windowErr, ErrShuttingDown,
		"a consistent view acquired between shutdown's refcount wait and segment "+
			"close reads memory that is about to be munmapped")
	assert.Nil(t, windowVal)
}

// A read after shutdown must error, not return a silently empty result (weaviate/0-weaviate-issues#285).
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

// Cursor constructors on a shut-down bucket must refuse with ErrShuttingDown,
// never serve a silently empty iteration and never crash the node.
func TestCursorsAfterShutdownRefuse(t *testing.T) {
	ctx := context.Background()

	t.Run("replace cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))
		require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		_, err := b.Cursor()
		require.ErrorIs(t, err, ErrShuttingDown)
	})

	t.Run("replace cursor on disk", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyReplace))
		require.NoError(t, b.Put([]byte("key-1"), []byte("value-1")))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		_, err := b.CursorOnDisk()
		require.ErrorIs(t, err, ErrShuttingDown)
	})

	t.Run("roaringset cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyRoaringSet))
		require.NoError(t, b.RoaringSetAddOne([]byte("key-1"), 1))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		_, err := b.CursorRoaringSet()
		require.ErrorIs(t, err, ErrShuttingDown)
	})

	t.Run("set cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategySetCollection))
		require.NoError(t, b.SetAdd([]byte("key-1"), [][]byte{[]byte("v1")}))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		_, err := b.SetCursor()
		require.ErrorIs(t, err, ErrShuttingDown)
	})

	t.Run("map cursor", func(t *testing.T) {
		b := newShutdownTestBucket(t, ctx, WithStrategy(StrategyMapCollection))
		require.NoError(t, b.MapSet([]byte("key-1"), MapPair{Key: []byte("k"), Value: []byte("v")}))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.Shutdown(ctx))

		_, err := b.MapCursor()
		require.ErrorIs(t, err, ErrShuttingDown)
	})
}

// Races readers against shutdown: every read must fully succeed or fail with ErrShuttingDown.
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
