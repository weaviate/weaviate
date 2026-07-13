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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/concurrency/testinghelpers"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestCombinedReader(t *testing.T) {
	logger, _ := test.NewNullLogger()
	mt1, mt2, mt3 := createTestMemtables(logger)

	testCases := []struct {
		name     string
		value    uint64
		operator filters.Operator
		expected []uint64
	}{
		{
			name:     "greater than equal 0",
			value:    0,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 0",
			value:    0,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than equal 0",
			value:    0,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20},
		},
		{
			name:     "less than 0",
			value:    0,
			operator: filters.OperatorLessThan,
			expected: []uint64{},
		},
		{
			name:     "equal 0",
			value:    0,
			operator: filters.OperatorEqual,
			expected: []uint64{10, 20},
		},
		{
			name:     "not equal 0",
			value:    0,
			operator: filters.OperatorNotEqual,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 4",
			value:    4,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 4",
			value:    4,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than equal 4",
			value:    4,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24},
		},
		{
			name:     "less than 4",
			value:    4,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20},
		},
		{
			name:     "equal 4",
			value:    4,
			operator: filters.OperatorEqual,
			expected: []uint64{14, 24},
		},
		{
			name:     "not equal 4",
			value:    4,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 15, 25, 113, 213, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 5",
			value:    5,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 5",
			value:    5,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than equal 5",
			value:    5,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25},
		},
		{
			name:     "less than 5",
			value:    5,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24},
		},
		{
			name:     "equal 5",
			value:    5,
			operator: filters.OperatorEqual,
			expected: []uint64{15, 25},
		},
		{
			name:     "not equal 5",
			value:    5,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 113, 213, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 13",
			value:    13,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{113, 213, 117, 217, 119, 219},
		},
		{
			name:     "greater than 13",
			value:    13,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{117, 217, 119, 219},
		},
		{
			name:     "less than equal 13",
			value:    13,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
		},
		{
			name:     "less than 13",
			value:    13,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25},
		},
		{
			name:     "equal 13",
			value:    13,
			operator: filters.OperatorEqual,
			expected: []uint64{113, 213},
		},
		{
			name:     "not equal 13",
			value:    13,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 117, 217, 119, 219},
		},

		{
			name:     "greater than equal 17",
			value:    17,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{117, 217, 119, 219},
		},
		{
			name:     "greater than 17",
			value:    17,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{119, 219},
		},
		{
			name:     "less than equal 17",
			value:    17,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
		{
			name:     "less than 17",
			value:    17,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213},
		},
		{
			name:     "equal 17",
			value:    17,
			operator: filters.OperatorEqual,
			expected: []uint64{117, 217},
		},
		{
			name:     "not equal 17",
			value:    17,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 119, 219},
		},

		{
			name:     "greater than equal 19",
			value:    19,
			operator: filters.OperatorGreaterThanEqual,
			expected: []uint64{119, 219},
		},
		{
			name:     "greater than 19",
			value:    19,
			operator: filters.OperatorGreaterThan,
			expected: []uint64{},
		},
		{
			name:     "less than equal 19",
			value:    19,
			operator: filters.OperatorLessThanEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217, 119, 219},
		},
		{
			name:     "less than 19",
			value:    19,
			operator: filters.OperatorLessThan,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
		{
			name:     "equal 19",
			value:    19,
			operator: filters.OperatorEqual,
			expected: []uint64{119, 219},
		},
		{
			name:     "not equal 19",
			value:    19,
			operator: filters.OperatorNotEqual,
			expected: []uint64{10, 20, 14, 24, 15, 25, 113, 213, 117, 217},
		},
	}

	t.Run("segments + memtable readers", func(t *testing.T) {
		seg1Reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(mt1)))
		seg2Reader := NewSegmentReader(NewGaplessSegmentCursor(newFakeSegmentCursor(mt2)))
		mtReader := NewMemtableReader(mt3)

		reader := NewCombinedReader([]InnerReader{seg1Reader, seg2Reader, mtReader}, func() {}, 4, logger)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				assert.NoError(t, err)
				defer release()

				assert.NotNil(t, bm)
				assert.ElementsMatch(t, bm.ToArray(), tc.expected)
			})
		}
	})

	t.Run("segment-in-memory + memtable readers", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		s := NewSegmentInMemory(logger)
		s.MergeMemtableEventually(mt1)
		s.MergeMemtableEventually(mt2)

		readers, release := s.Readers(roaringset.NewBitmapBufPoolNoop())
		mtReader := NewMemtableReader(mt3)

		reader := NewCombinedReader(append(readers, mtReader), release, 4, logger)

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				bm, release, err := reader.Read(context.Background(), tc.value, tc.operator)
				assert.NoError(t, err)
				defer release()

				assert.NotNil(t, bm)
				assert.ElementsMatch(t, bm.ToArray(), tc.expected)
			})
		}
	})
}

func TestCombinedReaderInnerReaders(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("all but 1st inner readers' results are released", func(t *testing.T) {
		expected := []uint64{1, 3, 5, 6}

		innerReader1 := newFakeInnerReader(roaringset.NewBitmap(1, 2), nil, nil)
		innerReader2 := newFakeInnerReader(roaringset.NewBitmap(3, 4), roaringset.NewBitmap(2), nil)
		innerReader3 := newFakeInnerReader(roaringset.NewBitmap(5, 6), roaringset.NewBitmap(4), nil)
		reader := NewCombinedReader([]InnerReader{innerReader1, innerReader2, innerReader3}, func() {}, 4, logger)

		bm, release, err := reader.Read(context.Background(), 0, filters.OperatorGreaterThanEqual)
		require.NoError(t, err)

		assert.ElementsMatch(t, expected, bm.ToArray())
		assert.Equal(t, 1, innerReader1.InUseCounter())
		assert.Equal(t, 0, innerReader2.InUseCounter())
		assert.Equal(t, 0, innerReader3.InUseCounter())
		release()
		assert.Equal(t, 0, innerReader1.InUseCounter())
	})

	t.Run("all inner readers' results are released on error", func(t *testing.T) {
		innerReader1 := newFakeInnerReader(nil, nil, fmt.Errorf("error1"))
		innerReader2 := newFakeInnerReader(roaringset.NewBitmap(3, 4), roaringset.NewBitmap(2), nil)
		innerReader3 := newFakeInnerReader(nil, nil, fmt.Errorf("error3"))
		reader := NewCombinedReader([]InnerReader{innerReader1, innerReader2, innerReader3}, func() {}, 4, logger)

		bm, _, err := reader.Read(context.Background(), 0, filters.OperatorGreaterThanEqual)
		require.Error(t, err)

		assert.Nil(t, bm)
		assert.ErrorContains(t, err, "error1")
		assert.ErrorContains(t, err, "error3")
		assert.Equal(t, 0, innerReader1.InUseCounter())
		assert.Equal(t, 0, innerReader2.InUseCounter())
		assert.Equal(t, 0, innerReader3.InUseCounter())
	})
}

// cloningInnerReader returns a fresh clone of its additions on every Read, so
// concurrent CombinedReader.Read calls never mutate a shared bitmap.
type cloningInnerReader struct {
	additions *sroar.Bitmap
	// readDelay keeps each Read alive across a few goroutine-sampler ticks, so
	// a disabled budget (kill switch) overshoots the ceiling decisively
	// instead of its short-lived workers being missed by the sampler.
	readDelay time.Duration
}

func (r *cloningInnerReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	if r.readDelay > 0 {
		time.Sleep(r.readDelay)
	}
	return roaringset.BitmapLayer{
		Additions: r.additions.Clone(),
		Deletions: sroar.NewBitmap(),
	}, noopRelease, nil
}

// TestCombinedReader_RespectsConcurrencyBudget pins the outer layer merge to
// the per-query budget without inflating the live goroutine count.
func TestCombinedReader_RespectsConcurrencyBudget(t *testing.T) {
	// Kill switch is this bound's red control, but no CI job sets it: CI
	// only ever runs the green (budget-enforced) path.
	if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
		t.Skip("budget cap disabled via kill switch")
	}
	// Merge fan-out only exists at SROAR_MERGE>=2 (GOMAXPROCS>=4); skipping
	// here silently would hide the guard, so CI fails loudly instead.
	if concurrency.SROAR_MERGE < 2 {
		if os.Getenv("CI") != "" {
			t.Fatalf("bounding tests require GOMAXPROCS>=4, refusing to skip silently on CI (SROAR_MERGE=%d)",
				concurrency.SROAR_MERGE)
		}
		t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
			concurrency.SROAR_MERGE)
	}

	logger, _ := test.NewNullLogger()

	// one value per sroar container (stride 2^16); enough real work per
	// *Conc op that the sampler can observe an ignored budget
	const numContainers = 8192
	values := make([]uint64, numContainers)
	for i := range values {
		values[i] = uint64(i) << 16
	}

	const numReaders = 8                   // several outer merges per Read
	const readDelay = 2 * time.Millisecond // see cloningInnerReader.readDelay
	// fixed segment parallelism keeps the goroutine ceiling machine-independent;
	// the budget only governs merge fan-out since the two axes were split
	const segmentParallelism = 2
	newReader := func() *CombinedReader {
		readers := make([]InnerReader, numReaders)
		for i := range readers {
			readers[i] = &cloningInnerReader{additions: roaringset.NewBitmap(values...), readDelay: readDelay}
		}
		return NewCombinedReader(readers, noopRelease, segmentParallelism, logger)
	}

	ctx := context.Background()
	budget1 := concurrency.CtxWithBudget(ctx, 1)

	// correctness: a budget of 1 returns the same set as an unconstrained query
	got1, release1, err := newReader().Read(budget1, 0, filters.OperatorGreaterThanEqual)
	require.NoError(t, err)
	arr1 := got1.ToArray()
	release1()

	gotDefault, releaseDefault, err := newReader().Read(ctx, 0, filters.OperatorGreaterThanEqual)
	require.NoError(t, err)
	arrDefault := gotDefault.ToArray()
	releaseDefault()

	require.Equal(t, values, arr1)
	require.Equal(t, arrDefault, arr1)

	// each in-flight Read holds <=4 goroutines: its own worker, the error
	// group driver, and the segmentParallelism=2 eg.Go workers; budget=1 keeps
	// sroar merges from adding any. Sharing one reader is safe since
	// cloningInnerReader clones per Read. readDelay pins that peak so slack=8
	// still lets the kill-switch red control overshoot decisively.
	shared := newReader()
	testinghelpers.AssertGoroutineCeiling(t, 16, 4, 8, 200*time.Millisecond, func() error {
		bm, release, err := shared.Read(budget1, 0, filters.OperatorGreaterThanEqual)
		if err != nil {
			return err
		}
		_ = bm
		release()
		return nil
	})
}

// budgetRecordingReader records the merge budget its Read observed in ctx
// (-1 when ctx carried none).
type budgetRecordingReader struct {
	seenBudget int
}

func (r *budgetRecordingReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	r.seenBudget = concurrency.BudgetFromCtx(ctx, -1)
	return roaringset.BitmapLayer{
		Additions: sroar.NewBitmap(),
		Deletions: sroar.NewBitmap(),
	}, noopRelease, nil
}

// TestCombinedReader_SplitsBudgetAcrossReaders pins the inner budget
// derivation: the per-query budget is divided by the number of readers that
// can run at once (segment parallelism + the current goroutine, at most
// count), floored at 1, and every reader sees the same share.
func TestCombinedReader_SplitsBudgetAcrossReaders(t *testing.T) {
	if entcfg.Enabled(os.Getenv("DISABLE_SROAR_MERGE_BUDGET")) {
		t.Skip("budget cap disabled via kill switch")
	}
	logger, _ := test.NewNullLogger()

	// expected share per reader; outer budget via the production helper, the
	// split formula spelled out so a regression in reader.go fails here
	expectedInner := func(numReaders, segmentParallelism, requestedBudget int) int {
		ctx := concurrency.CtxWithBudget(context.Background(), requestedBudget)
		outer := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
		return max(1, outer/min(numReaders, segmentParallelism+1))
	}

	tests := []struct {
		name               string
		numReaders         int
		segmentParallelism int
		requestedBudget    int
		expected           int
	}{
		{
			name:               "budget of 1 always yields share of 1",
			numReaders:         3,
			segmentParallelism: 4,
			requestedBudget:    1,
			expected:           1,
		},
		{
			name:               "share is outer budget over effective concurrency",
			numReaders:         3,
			segmentParallelism: 2,
			requestedBudget:    6,
			expected:           expectedInner(3, 2, 6),
		},
		{
			name:               "effective concurrency capped by reader count",
			numReaders:         2,
			segmentParallelism: 8,
			requestedBudget:    6,
			expected:           expectedInner(2, 8, 6),
		},
		{
			name:               "share floored at 1 when readers outnumber budget",
			numReaders:         8,
			segmentParallelism: 8,
			requestedBudget:    2,
			expected:           1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readers := make([]InnerReader, tt.numReaders)
			recorders := make([]*budgetRecordingReader, tt.numReaders)
			for i := range readers {
				recorders[i] = &budgetRecordingReader{}
				readers[i] = recorders[i]
			}
			reader := NewCombinedReader(readers, noopRelease, tt.segmentParallelism, logger)

			ctx := concurrency.CtxWithBudget(context.Background(), tt.requestedBudget)
			_, release, err := reader.Read(ctx, 0, filters.OperatorGreaterThanEqual)
			require.NoError(t, err)
			release()

			for i, rec := range recorders {
				assert.Equalf(t, tt.expected, rec.seenBudget, "reader %d saw wrong budget share", i)
			}
		})
	}

	t.Run("single reader keeps the caller's full budget", func(t *testing.T) {
		rec := &budgetRecordingReader{}
		reader := NewCombinedReader([]InnerReader{rec}, noopRelease, 2, logger)

		ctx := concurrency.CtxWithBudget(context.Background(), 5)
		_, release, err := reader.Read(ctx, 0, filters.OperatorGreaterThanEqual)
		require.NoError(t, err)
		release()

		assert.Equal(t, 5, rec.seenBudget)
	})
}

// concurrencyTrackingReader tracks how many Reads run at once across all
// instances sharing the same counters.
type concurrencyTrackingReader struct {
	current *atomic.Int32
	peak    *atomic.Int32
}

func (r *concurrencyTrackingReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	cur := r.current.Add(1)
	for {
		peak := r.peak.Load()
		if cur <= peak || r.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	// keep the read alive long enough for parallel reads to overlap
	time.Sleep(20 * time.Millisecond)
	r.current.Add(-1)
	return roaringset.BitmapLayer{
		Additions: sroar.NewBitmap(),
		Deletions: sroar.NewBitmap(),
	}, noopRelease, nil
}

// TestCombinedReader_SegmentFanoutBoundByConcurrency pins that segment-level
// fan-out is bound by the constructor's concurrency (not by the merge budget):
// at most segmentParallelism readers in the error group plus the current
// goroutine run at once, even with a merge budget of 1.
func TestCombinedReader_SegmentFanoutBoundByConcurrency(t *testing.T) {
	logger, _ := test.NewNullLogger()

	const numReaders = 8
	const segmentParallelism = 2

	var current, peak atomic.Int32
	readers := make([]InnerReader, numReaders)
	for i := range readers {
		readers[i] = &concurrencyTrackingReader{current: &current, peak: &peak}
	}
	reader := NewCombinedReader(readers, noopRelease, segmentParallelism, logger)

	ctx := concurrency.CtxWithBudget(context.Background(), 1)
	_, release, err := reader.Read(ctx, 0, filters.OperatorGreaterThanEqual)
	require.NoError(t, err)
	release()

	assert.LessOrEqual(t, peak.Load(), int32(segmentParallelism+1),
		"segment fan-out must be bound by constructor concurrency + current goroutine")
	assert.GreaterOrEqual(t, peak.Load(), int32(2),
		"a merge budget of 1 must not serialize segment reads")
}

func createTestMemtables(logger logrus.FieldLogger) (*Memtable, *Memtable, *Memtable) {
	mt1 := NewMemtable(logger)
	mt1.Insert(6, []uint64{16, 26})    // deleted
	mt1.Insert(19, []uint64{119, 219}) // 010011
	mt1.Insert(25, []uint64{113, 213}) // overwriten
	mt1.Delete(8, []uint64{10, 20})

	mt2 := NewMemtable(logger)
	mt2.Insert(4, []uint64{14, 24})    // 000100
	mt2.Insert(17, []uint64{117, 217}) // 010001
	mt2.Insert(22, []uint64{15, 25})   // overwritten
	mt2.Delete(1, []uint64{16, 26})

	mt3 := NewMemtable(logger)
	mt3.Insert(0, []uint64{10, 20})    // 000000
	mt3.Insert(5, []uint64{15, 25})    // 000101
	mt3.Insert(13, []uint64{113, 213}) // 001101
	mt3.Delete(21, []uint64{121, 221})

	// 0 -> 10, 20
	// 4 -> 14, 24
	// 5 -> 15, 25
	// 13 -> 113, 213
	// 17 -> 117, 217
	// 19 -> 119, 219

	return mt1, mt2, mt3
}

type fakeInnerReader struct {
	inUseCounter int
	additions    *sroar.Bitmap
	deletions    *sroar.Bitmap
	err          error
}

func newFakeInnerReader(additions, deletions *sroar.Bitmap, err error) *fakeInnerReader {
	return &fakeInnerReader{
		inUseCounter: 0,
		additions:    additions,
		deletions:    deletions,
		err:          err,
	}
}

func (r *fakeInnerReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (layer roaringset.BitmapLayer, release func(), err error) {
	r.inUseCounter++
	return roaringset.BitmapLayer{Additions: r.additions, Deletions: r.deletions},
		func() { r.inUseCounter-- }, r.err
}

func (r *fakeInnerReader) InUseCounter() int {
	return r.inUseCounter
}
