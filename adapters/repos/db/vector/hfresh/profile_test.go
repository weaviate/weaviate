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

package hfresh

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestLogHist(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		var h logHist
		assert.EqualValues(t, 0, h.Count())
		assert.EqualValues(t, 0, h.Mean())
		assert.EqualValues(t, 0, h.Quantile(0.99))
		assert.EqualValues(t, 0, h.Max())
	})

	t.Run("mean is exact, quantiles are upper bounds", func(t *testing.T) {
		tests := []struct {
			name   string
			values []uint64
		}{
			{"uniform small", []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			{"single value", []uint64{100}},
			{"zeros", []uint64{0, 0, 0}},
			{"wide range", []uint64{1, 1000, 1_000_000, 100_000_000}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var h logHist
				var sum, max uint64
				for _, v := range tt.values {
					h.Record(v)
					sum += v
					if v > max {
						max = v
					}
				}

				assert.EqualValues(t, len(tt.values), h.Count())
				assert.EqualValues(t, float64(sum)/float64(len(tt.values)), h.Mean())
				assert.EqualValues(t, max, h.Max())

				// quantiles are approximations rounded up to a power-of-two
				// bucket bound: they must be >= the true quantile and <= 2x max
				p50, p99 := h.Quantile(0.5), h.Quantile(0.99)
				assert.LessOrEqual(t, p50, p99)
				assert.LessOrEqual(t, p99, 2*max+1)
				assert.GreaterOrEqual(t, p99, max/2)
			})
		}
	})

	t.Run("p50 separates low and high populations", func(t *testing.T) {
		var h logHist
		// 90 values around 10, 10 values around 100k
		for range 90 {
			h.Record(10)
		}
		for range 10 {
			h.Record(100_000)
		}
		assert.Less(t, h.Quantile(0.5), uint64(100))
		assert.Greater(t, h.Quantile(0.95), uint64(50_000))
	})
}

func TestSearchProfilerRecord(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	p := newSearchProfiler(logger, 1000)

	// two queries with known phase durations and counters
	p.record(&searchStats{
		Centroid:          1 * time.Millisecond,
		Read:              8 * time.Millisecond,
		Scan:              2 * time.Millisecond,
		Rescore:           5 * time.Millisecond,
		PostingsRequested: 256,
		PostingsRead:      250,
		PostingsEmpty:     6,
		SegmentReads:      300,
		MemtableReads:     20,
		PostingBytes:      12 << 20,
		Candidates:        100_000,
		Duplicates:        30_000,
		Deleted:           1_000,
		RescoreFetched:    350,
		RescoreNotFound:   2,
		Results:           10,
	})
	p.record(&searchStats{
		Centroid:          3 * time.Millisecond,
		Read:              12 * time.Millisecond,
		Scan:              4 * time.Millisecond,
		Rescore:           7 * time.Millisecond,
		PostingsRequested: 256,
		PostingsRead:      256,
		SegmentReads:      256,
		PostingBytes:      10 << 20,
		Candidates:        80_000,
		Duplicates:        20_000,
		RescoreFetched:    350,
		Results:           10,
	})

	s := p.snapshot()
	require.EqualValues(t, 2, s.Queries)

	// exact means
	assert.EqualValues(t, 2*time.Millisecond, time.Duration(s.Centroid.Mean())*time.Microsecond)
	assert.EqualValues(t, 10*time.Millisecond, time.Duration(s.Read.Mean())*time.Microsecond)
	assert.EqualValues(t, 3*time.Millisecond, time.Duration(s.Scan.Mean())*time.Microsecond)
	assert.EqualValues(t, 6*time.Millisecond, time.Duration(s.Rescore.Mean())*time.Microsecond)

	assert.EqualValues(t, 256, s.PostingsRequested.Mean())
	assert.EqualValues(t, 253, s.PostingsRead.Mean())
	assert.EqualValues(t, 3, s.PostingsEmpty.Mean())
	assert.EqualValues(t, 278, s.SegmentReads.Mean())
	assert.EqualValues(t, 10, s.MemtableReads.Mean())
	assert.EqualValues(t, 11<<20, s.PostingBytes.Mean())
	assert.EqualValues(t, 90_000, s.Candidates.Mean())
	assert.EqualValues(t, 25_000, s.Duplicates.Mean())
	assert.EqualValues(t, 500, s.Deleted.Mean())
	assert.EqualValues(t, 350, s.RescoreFetched.Mean())
	assert.EqualValues(t, 1, s.RescoreNotFound.Mean())
	assert.EqualValues(t, 10, s.Results.Mean())

	// derived ratios
	assert.InDelta(t, 10.0/21.0*100, s.ReadPct, 0.5)                           // read share of summed phases
	assert.InDelta(t, (300.0+256.0)/(250.0+256.0), s.SegmentsPerPosting, 0.01) // fragmentation multiplier
	assert.InDelta(t, (30_000.0+20_000.0)/(180_000.0), s.DuplicatePct/100, 0.01)
}

func TestSearchProfilerPeriodicLog(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	p := newSearchProfiler(logger, 2) // log every 2 queries

	p.record(&searchStats{Read: time.Millisecond})
	require.Empty(t, hook.Entries)
	p.record(&searchStats{Read: time.Millisecond})
	require.Len(t, hook.Entries, 1)

	entry := hook.LastEntry()
	assert.Equal(t, "hfresh_search_profile", entry.Data["action"])
	assert.EqualValues(t, 2, entry.Data["queries"])
	assert.EqualValues(t, 2, entry.Data["window"])

	// logging resets the window: the next line must describe only the
	// queries recorded after the previous one, not the cumulative history
	require.EqualValues(t, 0, p.snapshot().Read.Count())

	p.record(&searchStats{Read: 3 * time.Millisecond})
	p.record(&searchStats{Read: 3 * time.Millisecond})
	require.Len(t, hook.Entries, 2)

	entry = hook.LastEntry()
	assert.EqualValues(t, 4, entry.Data["queries"]) // cumulative counter
	assert.Equal(t, "3ms", entry.Data["read_mean"]) // window-local mean
}

func TestSearchProfilerDisabled(t *testing.T) {
	// a nil profiler must be safe to use from the hot path
	var p *searchProfiler
	assert.NotPanics(t, func() {
		p.record(&searchStats{Read: time.Millisecond})
	})
	assert.False(t, p.enabled())
}

func TestNewSearchProfilerFromEnv(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	tests := []struct {
		value   string
		enabled bool
	}{
		{"", false},
		{"0", false},
		{"false", false},
		{"1", true},
		{"true", true},
		{"on", true},
		{"yes", true},
	}

	for _, tt := range tests {
		t.Run("value="+tt.value, func(t *testing.T) {
			t.Setenv("HFRESH_SEARCH_PROFILE", tt.value)
			p := newSearchProfilerFromEnv(logger)
			assert.Equal(t, tt.enabled, p.enabled())
		})
	}
}

// the profiler must not blow up under concurrent recording (it is shared
// across all search goroutines)
func TestSearchProfilerConcurrency(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	p := newSearchProfiler(logger, 1_000_000)

	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := range 1000 {
				p.record(&searchStats{
					Read:         time.Duration(j) * time.Microsecond,
					PostingsRead: 10,
					Candidates:   100,
				})
			}
		}()
	}
	for range 8 {
		<-done
	}

	s := p.snapshot()
	assert.EqualValues(t, 8000, s.Queries)
	assert.EqualValues(t, 10, s.PostingsRead.Mean())
}

var _ = logrus.Fields{} // keep logrus import if assertions change

// end-to-end: a profiled search must account phases and counters for the
// whole path (centroid search, posting reads, scan, rescore)
func TestSearchProfilerWiring(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectorsSize := 500
	k := 10
	vectors, _ := testinghelpers.RandomVecsFixedSeed(vectorsSize, 0, 32)

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	logger, _ := logrustest.NewNullLogger()
	index.profiler = newSearchProfiler(logger, 1_000_000)

	for i := range vectorsSize {
		require.NoError(t, index.Add(t.Context(), uint64(i), vectors[i]))
	}
	for index.taskQueue.Size() > 0 {
		time.Sleep(50 * time.Millisecond)
	}

	ids, _, err := index.SearchByVector(t.Context(), vectors[42], k, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	s := index.profiler.snapshot()
	require.EqualValues(t, 1, s.Queries)

	// counters must reflect a real search over a populated index
	assert.Greater(t, s.PostingsRequested.Mean(), 0.0)
	assert.Greater(t, s.PostingsRead.Mean(), 0.0)
	assert.Greater(t, s.Candidates.Mean(), 0.0)
	assert.Greater(t, s.RescoreFetched.Mean(), 0.0)
	assert.EqualValues(t, k, s.Results.Mean())
	// the dummy store serves postings from the memtable
	assert.Greater(t, s.MemtableReads.Mean()+s.SegmentReads.Mean(), 0.0)
	assert.Greater(t, s.PostingBytes.Mean(), 0.0)

	// all phases were timed (they may be fast, but each was recorded once)
	assert.EqualValues(t, 1, s.Centroid.Count())
	assert.EqualValues(t, 1, s.Read.Count())
	assert.EqualValues(t, 1, s.Scan.Count())
	assert.EqualValues(t, 1, s.Rescore.Count())
}

// a search with the profiler disabled (nil) must not record anything and not
// panic anywhere on the path
func TestSearchProfilerNilSafe(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectors, _ := testinghelpers.RandomVecsFixedSeed(50, 0, 32)
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)
	require.Nil(t, index.profiler)

	for i := range vectors {
		require.NoError(t, index.Add(t.Context(), uint64(i), vectors[i]))
	}

	ids, _, err := index.SearchByVector(t.Context(), vectors[7], 5, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
}
