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

package aggregator

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// Ensure at least one seeded range exceeds contextCheckInterval on every machine.
var iteratorCancelKeyCount = (2*runtime.GOMAXPROCS(0) + 1) * (contextCheckInterval + 1)

// iteratorStrategy pairs a bucket strategy that reaches iteratorConcurrently
// with the cursor its call sites use. No cursor aborts on a cancelled context
// itself: the Ctx constructors only cap merge concurrency.
type iteratorStrategy struct {
	name     string
	strategy string
	put      func(tb testing.TB, b *lsmkv.Bucket, i int)
	newCurs  func(ctx context.Context, b *lsmkv.Bucket) func() Cursor
}

func iteratorStrategies() []iteratorStrategy {
	return []iteratorStrategy{
		{
			name:     "replace",
			strategy: lsmkv.StrategyReplace,
			put: func(tb testing.TB, b *lsmkv.Bucket, i int) {
				require.NoError(tb, b.Put(iteratorKey(i), []byte(fmt.Sprintf("value-%06d", i))))
			},
			newCurs: func(_ context.Context, b *lsmkv.Bucket) func() Cursor {
				return func() Cursor { return ReplaceCursor{b.Cursor()} }
			},
		},
		{
			name:     "set collection",
			strategy: lsmkv.StrategySetCollection,
			put: func(tb testing.TB, b *lsmkv.Bucket, i int) {
				require.NoError(tb, b.SetAdd(iteratorKey(i), [][]byte{[]byte(fmt.Sprintf("value-%06d", i))}))
			},
			newCurs: func(_ context.Context, b *lsmkv.Bucket) func() Cursor {
				return func() Cursor { return SetCursor{b.SetCursor()} }
			},
		},
		{
			name:     "roaring set",
			strategy: lsmkv.StrategyRoaringSet,
			put: func(tb testing.TB, b *lsmkv.Bucket, i int) {
				require.NoError(tb, b.RoaringSetAddOne(iteratorKey(i), uint64(i)))
			},
			newCurs: func(ctx context.Context, b *lsmkv.Bucket) func() Cursor {
				// the call sites hand the query's context to the cursor, so do the same
				return func() Cursor { return RoaringCursor{b.CursorRoaringSetCtx(ctx)} }
			},
		},
	}
}

func iteratorKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%06d", i))
}

// TestIteratorConcurrentlyStopsWhenTheCallerGivesUp pins that an aggregation
// whose client has hung up stops scanning, in every strategy and both branches.
func TestIteratorConcurrentlyStopsWhenTheCallerGivesUp(t *testing.T) {
	for _, bs := range iteratorStrategies() {
		// A flushed bucket has disk segments, so QuantileKeys seeds the parallel
		// branches; an unflushed one has none and takes the single-cursor branch.
		for _, flush := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/flushed=%v", bs.name, flush), func(t *testing.T) {
				ctx := context.Background()
				logger, _ := test.NewNullLogger()
				b := bucketFixture(t, ctx, bs, iteratorCancelKeyCount, flush)

				seeds := b.QuantileKeys(2 * runtime.GOMAXPROCS(0))
				if flush {
					require.NotEmpty(t, seeds, "flushed keys must seed the parallel branches")
				} else {
					require.Empty(t, seeds, "unflushed keys must take the single-cursor branch")
				}

				scanCtx, cancel := context.WithCancel(ctx)
				defer cancel()

				var seen atomic.Int64
				err := iteratorConcurrently(scanCtx, b, bs.newCurs(scanCtx, b),
					func(k, v []byte, vv [][]byte, bi *sroar.Bitmap) error {
						if seen.Add(1) == 1 {
							cancel()
						}
						return nil
					}, logger)

				require.ErrorIs(t, err, context.Canceled)
				require.Less(t, seen.Load(), int64(iteratorCancelKeyCount),
					"the scan must stop early, not run to the end")
			})
		}
	}
}

// TestIteratorConcurrentlyScansEverythingForACallerThatStays is the control:
// the check must not cut a live scan short.
func TestIteratorConcurrentlyScansEverythingForACallerThatStays(t *testing.T) {
	for _, bs := range iteratorStrategies() {
		for _, flush := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/flushed=%v", bs.name, flush), func(t *testing.T) {
				ctx := context.Background()
				logger, _ := test.NewNullLogger()
				b := bucketFixture(t, ctx, bs, iteratorCancelKeyCount, flush)

				var seen atomic.Int64
				err := iteratorConcurrently(ctx, b, bs.newCurs(ctx, b),
					func(k, v []byte, vv [][]byte, bi *sroar.Bitmap) error {
						seen.Add(1)
						return nil
					}, logger)

				require.NoError(t, err)
				require.Equal(t, int64(iteratorCancelKeyCount), seen.Load())
			})
		}
	}
}

// bucketFixture builds a bucket holding count keys. The memtable threshold is
// raised so only an explicit FlushAndSwitch produces a segment.
func bucketFixture(tb testing.TB, ctx context.Context, bs iteratorStrategy,
	count int, flush bool,
) *lsmkv.Bucket {
	tb.Helper()

	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(ctx) })

	const bucketName = "iterator_cancel"
	require.NoError(tb, store.CreateOrLoadBucket(ctx, bucketName,
		lsmkv.WithStrategy(bs.strategy)))

	b := store.Bucket(bucketName)
	require.NotNil(tb, b)
	b.SetMemtableThreshold(1 << 40)

	for i := 0; i < count; i++ {
		bs.put(tb, b, i)
	}

	if flush {
		require.NoError(tb, b.FlushAndSwitch())
	}

	return b
}
