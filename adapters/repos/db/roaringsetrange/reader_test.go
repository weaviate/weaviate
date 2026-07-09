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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
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
}

func (r *cloningInnerReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, func(), error) {
	return roaringset.BitmapLayer{
		Additions: r.additions.Clone(),
		Deletions: sroar.NewBitmap(),
	}, noopRelease, nil
}

// TestCombinedReader_RespectsConcurrencyBudget proves the per-query budget
// threaded into CombinedReader.Read caps sroar's merge fan-out. With a budget
// of 1 the outer layer merges run single-threaded, so hammering the reader
// from many callers cannot inflate the live goroutine count. Without the
// budget each of the (readers-1) merges would fan out ~SROAR_MERGE workers.
func TestCombinedReader_RespectsConcurrencyBudget(t *testing.T) {
	if concurrency.SROAR_MERGE < 2 {
		t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
			concurrency.SROAR_MERGE)
	}

	logger, _ := test.NewNullLogger()

	// one value per sroar container (stride 2^16). Many containers so each
	// *Conc op spreads real work across min(n/24, SROAR_MERGE) workers that
	// live long enough for the sampler to observe when the budget is ignored.
	const numContainers = 8192
	values := make([]uint64, numContainers)
	for i := range values {
		values[i] = uint64(i) << 16
	}

	const numReaders = 8 // several outer merges per Read
	newReader := func() *CombinedReader {
		readers := make([]InnerReader, numReaders)
		for i := range readers {
			readers[i] = &cloningInnerReader{additions: roaringset.NewBitmap(values...)}
		}
		return NewCombinedReader(readers, noopRelease, concurrency.SROAR_MERGE, logger)
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

	// bounding leg
	const (
		numWorkers = 16
		runFor     = 200 * time.Millisecond
		// CombinedReader always drives its inner readers through an errgroup, so
		// each in-flight Read keeps at most 3 goroutines alive: its own worker,
		// the errgroup driver, and (because budget=1 forces eg.SetLimit(1)) a
		// single limited eg.Go worker. The merge ops spawn zero workers at conc=1.
		// Without the budget, conc=SROAR_MERGE lifts the eg limit and every Conc
		// op fans out ~SROAR_MERGE-1 merge workers, blowing past this ceiling.
		maxGoroutinesPerRead = 3
		// absorbs the sampler goroutine and transient runtime/GC workers
		noiseSlack = 12
	)

	stop := make(chan struct{})
	samplerDone := make(chan struct{})
	var maxSeen int // written only by the sampler, read after samplerDone closes

	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if n := runtime.NumGoroutine(); n > maxSeen {
					maxSeen = n
				}
			}
		}
	}()

	time.Sleep(5 * time.Millisecond)
	base := runtime.NumGoroutine()

	firstErr := make(chan error, 1)
	var wg sync.WaitGroup
	deadline := time.Now().Add(runFor)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := newReader()
			for time.Now().Before(deadline) {
				bm, release, err := r.Read(budget1, 0, filters.OperatorGreaterThanEqual)
				if err != nil {
					select {
					case firstErr <- err:
					default:
					}
					return
				}
				_ = bm
				release()
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-samplerDone

	select {
	case err := <-firstErr:
		require.NoError(t, err)
	default:
	}

	ceiling := base + numWorkers*maxGoroutinesPerRead + noiseSlack
	assert.LessOrEqualf(t, maxSeen, ceiling,
		"live goroutines peaked at %d, above base(%d)+workers(%d)*perRead(%d)+noise(%d)=%d; "+
			"a budget of 1 must not fan out sroar merge workers",
		maxSeen, base, numWorkers, maxGoroutinesPerRead, noiseSlack, ceiling)
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
