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

package inverted

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// spyContainsBatchBucket wraps a real roaringset *lsmkv.Bucket, recording
// every key passed to RoaringSetGetFromView and counting how many of the
// returned release funcs get called, so tests can assert both which keys
// docBitmapContainsBatch actually read and that it never leaks a bitmap.
type spyContainsBatchBucket struct {
	*lsmkv.Bucket
	reads        []string
	releaseCalls int
}

func (s *spyContainsBatchBucket) RoaringSetGetFromView(
	ctx context.Context, view lsmkv.BucketConsistentView, key []byte,
) (*sroar.Bitmap, func(), error) {
	s.reads = append(s.reads, string(key))
	bm, release, err := s.Bucket.RoaringSetGetFromView(ctx, view, key)
	return bm, func() {
		s.releaseCalls++
		release()
	}, err
}

func buildContainsBatchBucket(t *testing.T, ctx context.Context, rows map[string][]uint64) *spyContainsBatchBucket {
	t.Helper()

	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	b.SetMemtableThreshold(1e9) // no auto-flush; keep the fixture deterministic

	for key, values := range rows {
		require.NoError(t, b.RoaringSetAddList([]byte(key), values))
	}
	require.NoError(t, b.FlushAndSwitch())

	return &spyContainsBatchBucket{Bucket: b}
}

// expireAfterNDoneCtx reports ctx.Done() as closed starting with the (n+1)th
// call to Done(), letting a test deterministically cancel a synchronous
// per-key loop after exactly n keys have been checked, without goroutines or
// wall-clock timing.
type expireAfterNDoneCtx struct {
	context.Context
	n     int
	calls int
	done  chan struct{}
}

func newExpireAfterNDoneCtx(parent context.Context, n int) *expireAfterNDoneCtx {
	return &expireAfterNDoneCtx{Context: parent, n: n, done: make(chan struct{})}
}

func (c *expireAfterNDoneCtx) Done() <-chan struct{} {
	c.calls++
	if c.calls > c.n {
		select {
		case <-c.done:
		default:
			close(c.done)
		}
	}
	return c.done
}

func (c *expireAfterNDoneCtx) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// TestMergeAllowlistBitmaps_UnsupportedOperator pins the defensive default
// arm: an operator that is neither ContainsAny nor ContainsAll must error
// and release both operands, so the backstop cannot rot into a silent
// nil return or a buffer leak.
func TestMergeAllowlistBitmaps_UnsupportedOperator(t *testing.T) {
	var aReleased, bReleased bool
	res, release, err := mergeAllowlistBitmaps(filters.OperatorEqual, 1,
		sroar.NewBitmap(), func() { aReleased = true },
		sroar.NewBitmap(), func() { bReleased = true })
	require.Error(t, err)
	require.Nil(t, res)
	require.Nil(t, release)
	require.True(t, aReleased, "unsupported operator must release the accumulator")
	require.True(t, bReleased, "unsupported operator must release the fetched operand")
}

func TestDocBitmapContainsBatch_ContainsAnyFold(t *testing.T) {
	ctx := context.Background()
	spy := buildContainsBatchBucket(t, ctx, map[string][]uint64{
		"present-a": {1, 2, 3},
		"present-b": {3, 4, 5},
	})

	pv := &propValuePair{
		operator:       filters.ContainsAny,
		containsValues: [][]byte{[]byte("present-a"), []byte("missing"), []byte("present-b")},
	}

	s := &Searcher{}
	dbm, err := s.docBitmapContainsBatch(ctx, spy, pv)
	require.NoError(t, err)
	defer dbm.release()

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, dbm.docIDs.ToArray())
	require.False(t, dbm.IsDenyList())
	require.Equal(t, []string{"present-a", "missing", "present-b"}, spy.reads,
		"every key must be read for ContainsAny, absent key included")
}

func TestDocBitmapContainsBatch_ContainsAllFold(t *testing.T) {
	ctx := context.Background()

	t.Run("non-empty intersection", func(t *testing.T) {
		spy := buildContainsBatchBucket(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"b": {2, 3, 4},
			"c": {2, 3, 5},
		})

		pv := &propValuePair{
			operator:       filters.ContainsAll,
			containsValues: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		}

		s := &Searcher{}
		dbm, err := s.docBitmapContainsBatch(ctx, spy, pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Equal(t, []uint64{2, 3}, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "b", "c"}, spy.reads)
	})

	t.Run("folds to empty and stops reading remaining keys", func(t *testing.T) {
		spy := buildContainsBatchBucket(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"b": {4, 5, 6}, // disjoint from "a" -> accumulator becomes empty here
			"c": {1, 2, 3}, // must never be read: the AND result can't change
		})

		pv := &propValuePair{
			operator:       filters.ContainsAll,
			containsValues: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		}

		s := &Searcher{}
		dbm, err := s.docBitmapContainsBatch(ctx, spy, pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Empty(t, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "b"}, spy.reads,
			"key c must not be read once the AND accumulator is provably empty")
	})

	t.Run("absent key empties the intersection and stops reading", func(t *testing.T) {
		spy := buildContainsBatchBucket(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"c": {1, 2, 3}, // must never be read: the absent key already emptied the AND
		})

		pv := &propValuePair{
			operator:       filters.ContainsAll,
			containsValues: [][]byte{[]byte("a"), []byte("missing"), []byte("c")},
		}

		s := &Searcher{}
		dbm, err := s.docBitmapContainsBatch(ctx, spy, pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Empty(t, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "missing"}, spy.reads,
			"key c must not be read once the absent key emptied the accumulator")
	})
}

func TestDocBitmapContainsBatch_ContextCancelledMidLoop(t *testing.T) {
	spy := buildContainsBatchBucket(t, context.Background(), map[string][]uint64{
		"a": {1, 2, 3},
		"b": {4, 5, 6},
		"c": {7, 8, 9},
	})

	pv := &propValuePair{
		operator:       filters.ContainsAny,
		containsValues: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
	}

	// expires right after key "a" is read, before "b" is checked
	ctx := newExpireAfterNDoneCtx(context.Background(), 1)

	s := &Searcher{}
	dbm, err := s.docBitmapContainsBatch(ctx, spy, pv)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, docBitmap{}, dbm)

	require.Equal(t, []string{"a"}, spy.reads, "loop must stop reading once ctx is expired")
	require.Equal(t, 1, spy.releaseCalls, "accumulator built from key \"a\" must be released, not leaked")
}
