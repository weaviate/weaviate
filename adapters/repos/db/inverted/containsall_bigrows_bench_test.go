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
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// What splitting a ContainsAll across workers is worth on rows big enough for
// the split to pay for itself, on the one shape where the early exit cannot
// rescue a bad plan.
//
// gh12242's own corpus is one docID per row, where the per-key overhead of
// reaching a row dominates and the worker count barely shows. Here every row
// holds ten thousand docs, so a worker's share is real work, and every row
// shares a core so the intersection never empties: no key is skipped.
//
// Run:
//
//	go test -tags integrationTest -v -run '^$' -bench 'ContainsAllBigRows' \
//	    -benchmem -count 6 ./adapters/repos/db/inverted/ | tee bigrows.txt
//
// -v because the clamp figure the workers legs are read against is logged,
// and b.Logf output is dropped without it.
//
// then read the workers legs against each other with benchstat. The `default`
// leg is the planner's own choice, which is the number the others are asking
// whether to change.
const (
	// bigRowsDocs is how many doc IDs the shard has allocated, which is what the
	// memory clamp prices a worker against — at this size it affords seven.
	bigRowsDocs = 500_000
	// bigRowsKeys is how many distinct values the corpus holds. It bounds the
	// largest batch the sweep can ask for.
	bigRowsKeys = 512
	// bigRowsRowSize is how many docs one row holds. Big enough that merging a
	// row costs more than finding it, which is the regime this file exists for.
	bigRowsRowSize = 10_000
	// bigRowsCore is the docs every row shares, so an intersection over any
	// subset of the corpus is non-empty however many keys it names.
	bigRowsCore = 1_000
	// bigRowsUnflushedEvery leaves every nth key in the active memtable. A fully
	// flushed corpus has an empty one, the reader drops it from the view, and
	// the windowed memtable read — the fold's dominant memory term and the one
	// per-worker cost this sweep is meant to price — never runs at all.
	bigRowsUnflushedEvery = 4
)

// bigRowsBatchSizes brackets the point where splitting starts to pay. The small
// end is not padding: a batch can be too small to be worth splitting, and the
// sweep is only useful if it covers both sides of that.
var bigRowsBatchSizes = []int{8, 16, 32, 64, 128, 512}

// bigRowsWorkerCounts is swept by the benchmark and pinned by the corpus test
// below, which stops being a check on the benchmark if the two drift.
var bigRowsWorkerCounts = []int{0, 1, 2, 4, 7, 8}

// roaringRowsFixture is a roaringset bucket over a known set of rows, wrapped in
// a batched-Contains Searcher.
type roaringRowsFixture struct {
	searcher *Searcher
	store    *lsmkv.Store
	// rows is what was written, key order, so a test can say what a fold over
	// them owes without reaching back into the bucket for it
	rows [][]uint64
}

// newRoaringRowsFixture writes rows under benchValue(k), leaving every
// bigRowsUnflushedEvery-th key in the active memtable so a fold reads both
// tiers. The flush threshold is set out of reach, so which keys land in a
// memtable is a property of this fixture rather than of how much got written
// before it ran.
func newRoaringRowsFixture(tb testing.TB, rows [][]uint64, docIDCount uint64) *roaringRowsFixture {
	tb.Helper()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	// the production pooled buffer pool, so allocation numbers reflect pooled
	// reads rather than the noop pool's per-read allocations
	bufPool, closePool := roaringset.NewBitmapBufPoolDefault(logger, nil,
		config.DefaultQueryBitmapBufsMaxBufSize, config.DefaultQueryBitmapBufsMaxMemory)
	tb.Cleanup(closePool)

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Shutdown(context.Background())) })

	name := helpers.BucketFromPropNameLSM(benchPropName)
	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), name,
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet), lsmkv.WithBitmapBufPool(bufPool)))
	bucket := store.Bucket(name)
	bucket.SetMemtableThreshold(1 << 30)

	for k, row := range rows {
		if k%bigRowsUnflushedEvery == 0 {
			continue // written below, after the switch, so it stays in the memtable
		}
		require.NoError(tb, bucket.RoaringSetAddList([]byte(benchValue(k)), row))
	}
	require.NoError(tb, bucket.FlushAndSwitch())
	for k, row := range rows {
		if k%bigRowsUnflushedEvery == 0 {
			require.NoError(tb, bucket.RoaringSetAddList([]byte(benchValue(k)), row))
		}
	}

	bitmapFactory := roaringset.NewBitmapFactory(bufPool, func() uint64 { return docIDCount })
	searcher := NewSearcher(logger, store, createSchema().GetClass, nil, nil,
		stopwords.NewProvider(fakeStopwordDetector{}, nil), 2, func() bool { return false },
		func(string) bool { return false }, "",
		config.DefaultQueryNestedCrossReferenceLimit, bitmapFactory).
		WithBatchedContainsEnabled(configRuntime.NewDynamicValue(true))

	return &roaringRowsFixture{searcher: searcher, store: store, rows: rows}
}

// resolveDocIDs runs DocIDs with filter and returns the sorted doc-ID slice.
func resolveDocIDs(t *testing.T, ctx context.Context, s *Searcher,
	filter *filters.LocalFilter,
) []uint64 {
	t.Helper()
	al, err := s.DocIDs(ctx, filter, additional.Properties{}, className)
	require.NoError(t, err)
	defer al.Close()

	out := al.Slice()
	slices.Sort(out)
	return out
}

// benchDocIDs is the body every DocIDs benchmark leg runs.
func benchDocIDs(b *testing.B, s *Searcher, ctx context.Context, filter *filters.LocalFilter) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		al, err := s.DocIDs(ctx, filter, additional.Properties{}, className)
		if err != nil {
			b.Fatal(err)
		}
		al.Close()
	}
}

func (f *roaringRowsFixture) wantIntersection(size int) []uint64 {
	counts := map[uint64]int{}
	for _, row := range f.rows[:size] {
		for _, d := range row {
			counts[d]++
		}
	}
	var out []uint64
	for d, n := range counts {
		if n == size {
			out = append(out, d)
		}
	}
	slices.Sort(out)
	return out
}

// newBigRowsFixture builds the corpus described above: every row holds the
// shared core, plus its own overlapping stretch beyond it.
func newBigRowsFixture(tb testing.TB) *roaringRowsFixture {
	tb.Helper()

	rows := make([][]uint64, bigRowsKeys)
	for k := range rows {
		row := make([]uint64, 0, bigRowsRowSize)
		for d := 0; d < bigRowsCore; d++ {
			row = append(row, uint64(d))
		}
		// each key's own docs overlap its neighbours', so the rows are neither
		// disjoint nor identical outside the core
		start := bigRowsCore + k*900
		for d := 0; d < bigRowsRowSize-bigRowsCore; d++ {
			row = append(row, uint64(start+d))
		}
		rows[k] = row
	}

	return newRoaringRowsFixture(tb, rows, bigRowsDocs)
}

// bigRowsValues names the first size keys of the corpus, so every batch the
// sweep runs is a prefix of the same corpus and the legs differ only in how
// many keys they name.
func bigRowsValues(size int) []string {
	values := make([]string, size)
	for i := range values {
		values[i] = benchValue(i)
	}
	return values
}

// BenchmarkDocIDs_ContainsAllBigRows sweeps the fetch worker count over batches
// of big, non-emptying rows. The `default` leg leaves the planner alone and is
// the baseline the forced legs argue with; the forced legs run from one worker
// up past what the clamp affords, so the sweep shows both what parallelism buys
// and where the clamp is cutting off something that was still paying.
func BenchmarkDocIDs_ContainsAllBigRows(b *testing.B) {
	f := newBigRowsFixture(b)
	ctx := context.Background()

	planner := containsFoldPlanner{docIDCount: bigRowsDocs}
	b.Logf("corpus: %d keys x %d docs/row, %d doc IDs; clamp affords %d workers, GOMAXPROCS %d",
		bigRowsKeys, bigRowsRowSize, bigRowsDocs,
		planner.clampWorkers(concurrency.GOMAXPROCS), concurrency.GOMAXPROCS)

	// 0 means "leave the planner alone"; the rest bracket the clamp, which
	// affords seven workers at this shard size
	workerCounts := bigRowsWorkerCounts

	for _, size := range bigRowsBatchSizes {
		filter := containsFilter(filters.ContainsAll, bigRowsValues(size))
		for _, workers := range workerCounts {
			name := fmt.Sprintf("N=%03d/workers=default", size)
			if workers > 0 {
				name = fmt.Sprintf("N=%03d/workers=%d", size, workers)
			}
			b.Run(name, func(b *testing.B) {
				forceContainsWorkers(b, workers)

				benchDocIDs(b, f.searcher, ctx, filter)
			})
		}
	}
}

// TestDocIDs_ContainsAllBigRowsCorpus pins what the sweep is measuring, so a
// benchmark whose corpus quietly stopped having the shape it claims cannot go
// on reporting numbers for it: the intersection is the shared core at every
// batch size, and it is the same whatever worker count folded it.
//
// Without this the sweep's own premise is unchecked — an early exit that fired
// because the rows had drifted apart would make every forced leg look faster
// for a reason that has nothing to do with the split.
func TestDocIDs_ContainsAllBigRowsCorpus(t *testing.T) {
	f := newBigRowsFixture(t)
	ctx := context.Background()

	for _, size := range bigRowsBatchSizes {
		filter := containsFilter(filters.ContainsAll, bigRowsValues(size))
		want := f.wantIntersection(size)

		for _, workers := range bigRowsWorkerCounts {
			t.Run(fmt.Sprintf("N=%03d/workers=%d", size, workers), func(t *testing.T) {
				forceContainsWorkers(t, workers)

				got := resolveDocIDs(t, ctx, f.searcher, filter)
				require.Equal(t, want, got,
					"the worker count must not change what the intersection is")
				// the premise of the sweep: no batch empties, so no leg of it
				// can be fast because the fold stopped early. Asked of what the
				// bucket answered, since wantIntersection counts the rows the
				// fixture meant to write and cannot see them drift.
				require.GreaterOrEqual(t, len(got), bigRowsCore,
					"N=%d must intersect to at least the shared core", size)
			})
		}
	}
}
