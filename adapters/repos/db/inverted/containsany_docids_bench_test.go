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

package inverted

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// GH-12242 Go-level A/B instrument for ContainsAny/ContainsAll fan-out cost.
//
// Benchmarks Searcher.DocIDs (the full extract + resolve + merge path, no
// HNSW) on a filterable roaringset text property with strictly-unique values
// (1 value == 1 docID), the reported pathological shape. The primary metric is
// allocs/op + B/op via -benchmem: deterministic and thermal-independent, unlike
// server-level throughput. The optimization target ("cost F") is the O(N)
// per-value filters.Clause + propValuePair construction at extraction, so the
// benchmark deliberately includes extraction rather than only resolution.
//
// Run:
//   go test -tags integrationTest -run '^$' -bench 'DocIDs_Contains' \
//       -benchmem -benchtime 20x -count 6 ./adapters/repos/db/inverted/ | tee baseline.txt
// then compare A/B with: benchstat baseline.txt optimized.txt

const benchPropName = "inverted-text-roaringset"

// containsFixture is a shared, deterministic corpus reused across every
// sub-benchmark size so the (expensive) 300K-entry bucket build happens once.
type containsFixture struct {
	searcher *Searcher
	store    *lsmkv.Store
	numDocs  int
}

func newContainsFixture(tb testing.TB, numDocs int) *containsFixture {
	tb.Helper()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	// Use the production pooled buffer pool (NewBitmapBufPoolDefault with the
	// server's default 32MB/128MB sizing), matching what the real server wires
	// in configure_api.go, so allocation/GC numbers reflect production behaviour
	// (pooled+reused read buffers) rather than the noop pool's per-read
	// allocations, which overstate cost.
	bufPool, bufPoolClose := roaringset.NewBitmapBufPoolDefault(logger, nil,
		config.DefaultQueryBitmapBufsMaxBufSize, config.DefaultQueryBitmapBufsMaxMemory)
	tb.Cleanup(bufPoolClose)

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(context.Background()) })

	bucketName := helpers.BucketFromPropNameLSM(benchPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(bufPool),
	))
	bucket := store.Bucket(bucketName)

	// value i ("val_%08d") maps to exactly docID i: strictly unique, 1:1.
	for i := 0; i < numDocs; i++ {
		require.NoError(tb, bucket.RoaringSetAddList([]byte(benchValue(i)), []uint64{uint64(i)}))
	}
	require.NoError(tb, bucket.FlushAndSwitch())

	maxDocID := uint64(numDocs + 1)
	bitmapFactory := roaringset.NewBitmapFactory(bufPool, newFakeMaxIDGetter(maxDocID))
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false },
		func(string) bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory)

	return &containsFixture{searcher: searcher, store: store, numDocs: numDocs}
}

func benchValue(i int) string { return fmt.Sprintf("val_%08d", i) }

// sampleValues picks `size` values spread evenly across the corpus (strided),
// so the selection touches the whole keyspace. Deterministic and identical
// across A/B runs. Returns the values and the docIDs they resolve to.
func (f *containsFixture) sampleValues(size int) (values []string, docIDs []uint64) {
	stride := f.numDocs / size
	if stride < 1 {
		stride = 1
	}
	values = make([]string, 0, size)
	docIDs = make([]uint64, 0, size)
	for i := 0; i < f.numDocs && len(values) < size; i += stride {
		values = append(values, benchValue(i))
		docIDs = append(docIDs, uint64(i))
	}
	return values, docIDs
}

func containsFilter(op filters.Operator, values []string) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: op,
			On:       &filters.Path{Class: className, Property: schema.PropertyName(benchPropName)},
			Value:    &filters.Value{Value: values, Type: schema.DataTypeText},
		},
	}
}

const benchCorpusSize = 300_000

var benchSizes = []int{100, 1_000, 10_000, 100_000}

func BenchmarkDocIDs_ContainsAny(b *testing.B) {
	f := newContainsFixture(b, benchCorpusSize)
	ctx := context.Background()
	for _, size := range benchSizes {
		values, _ := f.sampleValues(size)
		filter := containsFilter(filters.ContainsAny, values)
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
				if err != nil {
					b.Fatal(err)
				}
				al.Close()
			}
		})
	}
}

func BenchmarkDocIDs_ContainsAll(b *testing.B) {
	f := newContainsFixture(b, benchCorpusSize)
	ctx := context.Background()
	for _, size := range benchSizes {
		values, _ := f.sampleValues(size)
		filter := containsFilter(filters.ContainsAll, values)
		b.Run(fmt.Sprintf("N=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
				if err != nil {
					b.Fatal(err)
				}
				al.Close()
			}
		})
	}
}

// TestDocIDs_ContainsCorrectness is the correctness gate for the benchmark
// fixture: it pins that DocIDs returns exactly the expected doc-ID set on the
// same corpus the benchmark measures, so an optimization cannot "win" by
// returning wrong results. ContainsAny(sample) == the sampled docIDs;
// ContainsAll(sample) over strictly-unique values == empty (no doc holds >1
// value), which still fully exercises the AND-fold extraction/merge path.
func TestDocIDs_ContainsCorrectness(t *testing.T) {
	f := newContainsFixture(t, 20_000)
	ctx := context.Background()

	for _, size := range []int{1, 100, 1_000, 10_000} {
		values, wantAny := f.sampleValues(size)
		t.Run(fmt.Sprintf("ContainsAny_N=%d", size), func(t *testing.T) {
			al, err := f.searcher.DocIDs(ctx, containsFilter(filters.ContainsAny, values),
				additional.Properties{}, className)
			require.NoError(t, err)
			defer al.Close()
			got := al.Slice()
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			require.Equal(t, wantAny, got)
		})
		t.Run(fmt.Sprintf("ContainsAll_N=%d", size), func(t *testing.T) {
			al, err := f.searcher.DocIDs(ctx, containsFilter(filters.ContainsAll, values),
				additional.Properties{}, className)
			require.NoError(t, err)
			defer al.Close()
			if size == 1 {
				require.Equal(t, wantAny, al.Slice()) // single value: AND == that value's docID
			} else {
				require.True(t, al.IsEmpty(),
					"ContainsAll over %d strictly-unique values must be empty", size)
			}
		})
	}
}

// TestDocIDs_GoroutinePeak measures peak live goroutines during concurrent
// DocIDs(100K) resolution — the structural signal for the fan-out fix. It does
// not hard-assert a low bound (the baseline is expected to explode); it records
// the peak via t.Log so baseline vs optimized can be compared. A generous
// sanity ceiling guards against a runaway only.
func TestDocIDs_GoroutinePeak(t *testing.T) {
	f := newContainsFixture(t, benchCorpusSize)
	ctx := context.Background()
	values, _ := f.sampleValues(100_000)
	filter := containsFilter(filters.ContainsAny, values)

	const concurrentCallers = 8

	stop := make(chan struct{})
	var peak int64
	var samplerWg sync.WaitGroup
	samplerWg.Add(1)
	go func() {
		defer samplerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if n := int64(runtime.NumGoroutine()); n > peak {
					peak = n
				}
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()

	baseline := runtime.NumGoroutine()
	var wg sync.WaitGroup
	for c := 0; c < concurrentCallers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			al, err := f.searcher.DocIDs(ctx, filter, additional.Properties{}, className)
			require.NoError(t, err)
			al.Close()
		}()
	}
	wg.Wait()
	close(stop)
	samplerWg.Wait()

	t.Logf("goroutine peak: baseline=%d peak=%d (delta=%d) during %d concurrent DocIDs(100K), GOMAXPROCS=%d",
		baseline, peak, peak-int64(baseline), concurrentCallers, runtime.GOMAXPROCS(0))
	require.Less(t, peak, int64(2_000_000), "runaway goroutine growth")
}
