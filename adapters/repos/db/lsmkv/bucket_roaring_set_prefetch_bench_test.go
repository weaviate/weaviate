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
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/usecases/config"
)

// What BatchReaderWindowKeys trades, measured on a bucket rather than on
// stand-ins. Gotchas: the fixture is built inside each sub-benchmark, not
// shared across a sweep, so a writer growing it doesn't drive the conditions;
// the writer only touches keys already present, so it adds bytes but never
// nodes; and the disk is populated, since an empty one makes cloning the
// reader's whole cost, unlike a flushed bucket. TestBenchWindowFixtureIsStable
// checks the first two.
//
// Reader cost is timed in the reader rather than read off ns/op, which with
// several readers sharing b.N divides by the reader count. Writer throughput
// is writes/s rather than a raw count over varying wall time; writer latency
// is bimodal, so the sample count is reported alongside the percentiles.
//
//	go test -run '^$' -bench BenchmarkRoaringSetWindow -benchtime 300x -count 5 \
//	    ./adapters/repos/db/lsmkv/
const (
	benchWindowBatch = 20_000
	// how many of the batch's keys the memtable holds; a memtable is a delta,
	// so a filter asks it about far more keys than it has
	benchWindowMemPct = 10
)

var benchWindows = []int{64, 128, 256, 512, 1024, 4096}

// writeLatCap bounds the writer's latency samples so appending them doesn't
// reallocate inside the timed loop. The ring is sized to fit the documented
// run; a longer one wraps and keeps the most recent writes over the warm-up.
const writeLatCap = 1 << 20

// benchWindowReaderCounts straddles GOMAXPROCS rather than hardcoding reader
// counts, so the contention conclusions hold across machines with different
// core counts.
func benchWindowReaderCounts() []int {
	procs := runtime.GOMAXPROCS(0)
	// Deduped: a small host's procs/2 and procs can collide.
	counts := []int{max(2, procs/2), procs, 2 * procs}
	slices.Sort(counts)
	return slices.Compact(counts)
}

func benchWindowKey(i int) string { return fmt.Sprintf("key_%08d", i) }

// benchWindowDocs spreads a row's documents stride apart. A clone copies
// whole containers, so cost follows how many containers the documents touch,
// not how many documents there are: at benchWindowBatch, a thousand documents
// ingested in document order (stride N) touch ~305 of the 65,536-id
// containers and cost ~47KB; the same thousand grouped by value (stride 1)
// share one container and cost ~2KB. Neither is worst case — one container
// per document (a full-container stride) costs ~141KB.
func benchWindowDocs(key, docsPerRow, stride int) []uint64 {
	docs := make([]uint64, docsPerRow)
	for j := range docs {
		docs[j] = uint64(key + j*stride)
	}
	return docs
}

// benchWindowFixture builds a bucket holding diskHitPct of the batch on disk
// and benchWindowMemPct of it in the active memtable, and returns the batch
// along with the memtable's own keys, which is all the writer may touch.
//
// docsPerRow and docsStride are kept separate from the key count because they
// move independently: the batch decides how many rows are cloned, these
// decide what each clone costs (see benchWindowDocs).
func benchWindowFixture(tb testing.TB, diskHitPct, docsPerRow, docsStride int) (*Bucket, inverted.SortedKeys, []string) {
	tb.Helper()
	ctx := context.Background()

	// The pooled buffers the server wires in, so the disk read costs what it
	// costs in production rather than allocating per row.
	pool, closePool := roaringset.NewBitmapBufPoolDefault(nullLogger(), nil,
		config.DefaultQueryBitmapBufsMaxBufSize, config.DefaultQueryBitmapBufsMaxMemory)
	tb.Cleanup(closePool)

	b, err := NewBucketCreator().NewBucket(ctx, tb.TempDir(), "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet), WithBitmapBufPool(pool))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, b.Shutdown(context.Background())) })
	b.SetMemtableThreshold(1e9) // no auto-flush; the split is the fixture

	names := make([]string, benchWindowBatch)
	for i := range names {
		names[i] = benchWindowKey(i)
	}
	for i, name := range names {
		if i%100 < diskHitPct {
			require.NoError(tb, b.RoaringSetAddList([]byte(name), benchWindowDocs(i, docsPerRow, docsStride)))
		}
	}
	require.NoError(tb, b.FlushAndSwitch())

	var memKeys []string
	for i, name := range names {
		if i%100 < benchWindowMemPct {
			require.NoError(tb, b.RoaringSetAddList([]byte(name), benchWindowDocs(i+benchWindowBatch, docsPerRow, docsStride)))
			memKeys = append(memKeys, name)
		}
	}
	return b, sortedKeysOf(tb, names), memKeys
}

// benchWindowSplitAcrossMemtables leaves the fixture with a flush in flight, so
// a fold reads two memtables and splits each fill's budget between them. Every
// other fixture here switches its memtable away before writing.
func benchWindowSplitAcrossMemtables(tb testing.TB, b *Bucket, memKeys []string,
	docsPerRow, docsStride int,
) {
	tb.Helper()

	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(tb, err)
	require.True(tb, switched, "the fixture wrote nothing, so there is nothing to flush")

	// Nothing here will ever flush it: these fixtures run a noop cycle manager,
	// and Shutdown polls for a nil flushing memtable on a context that never
	// expires. Registered after the fixture's own Shutdown cleanup so it runs
	// before it.
	tb.Cleanup(func() {
		b.flushLock.Lock()
		defer b.flushLock.Unlock()
		b.flushing = nil
	})

	// The new active memtable has to hold rows of its own: a reader drops an
	// empty active one, which would put the fold back on a single memtable.
	// Seeded past the flushing memtable's documents so the two do not coincide.
	for i, name := range memKeys {
		docs := benchWindowDocs(i+2*benchWindowBatch, docsPerRow, docsStride)
		require.NoError(tb, b.RoaringSetAddList([]byte(name), docs))
	}
}

// benchWindowFold reads the whole batch through one reader the way the
// contains fold does: in order, once each, releasing as it goes. Runs at the
// production byte budget, so the timings carry whatever narrowing it imposes.
func benchWindowFold(tb testing.TB, b *Bucket, keys inverted.SortedKeys, window int) {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	r, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, window, BatchReaderWindowBytes)
	if err != nil {
		tb.Error(err)
		return
	}
	for i := 0; i < keys.Len(); i++ {
		_, release, err := r.Next(concurrency.SROAR_MERGE)
		if err != nil {
			tb.Error(err)
			return
		}
		release()
	}
}

// benchWindowCloneBytes is what one filled window holds at once — live bytes,
// which allocs/op cannot report since it counts allocation events. It fills
// through the reader rather than serving through it, since a served slot
// releases its clone.
func benchWindowCloneBytes(tb testing.TB, b *Bucket, keys inverted.SortedKeys, window int) int {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	// Uncapped: a real budget would clamp every window size above it to the
	// same figure, flattening the curve this measures. Production holds this
	// or the budget, whichever is smaller.
	r, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, window, math.MaxInt)
	require.NoError(tb, err)
	require.NoError(tb, r.fillWindow())

	total := 0
	for mt := 0; mt < r.mtCount; mt++ {
		for _, layer := range r.windows[mt].layer {
			if layer.Additions != nil {
				total += len(layer.Additions.ToBuffer())
			}
			if layer.Deletions != nil {
				total += len(layer.Deletions.ToBuffer())
			}
		}
	}
	return total
}

// TestBenchWindowFixtureIsStable is the harness checking itself: a fixture
// must come out the same every time it's built (in nodes and in clone bytes),
// and running the writer must leave the node count where it found it —
// otherwise the sweep measures the window against a moving target.
func TestBenchWindowFixtureIsStable(t *testing.T) {
	// Nodes and clone bytes move for different reasons: nodes are what a
	// tree-growing writer would disturb, bytes are what clone-B reports.
	shape := func(b *Bucket) (nodes, bytes int) {
		c := b.active.newRoaringSetCursor()
		for k, layer, err := c.First(); k != nil && err == nil; k, layer, err = c.Next() {
			nodes++
			bytes += len(layer.Additions.ToBuffer()) + len(layer.Deletions.ToBuffer())
		}
		return nodes, bytes
	}

	first, _, memKeys := benchWindowFixture(t, 50, 1, benchWindowBatch)
	second, _, _ := benchWindowFixture(t, 50, 1, benchWindowBatch)
	firstNodes, firstBytes := shape(first)
	secondNodes, secondBytes := shape(second)
	require.NotZero(t, firstNodes)
	require.Equal(t, firstNodes, secondNodes, "two builds of one fixture must match")
	require.Equal(t, firstBytes, secondBytes,
		"two builds must cost the same to clone, or clone-B is not a comparison")

	for n := 0; n < 5*len(memKeys); n++ {
		require.NoError(t, first.RoaringSetAddList([]byte(memKeys[n%len(memKeys)]), []uint64{uint64(n)}))
	}
	afterNodes, afterBytes := shape(first)
	require.Equal(t, firstNodes, afterNodes, "the writer must add bytes, never nodes")
	// A loose bound, not a claim the encoding never grows: catches a fixture
	// change that made a row several times costlier to clone.
	require.LessOrEqual(t, afterBytes, 4*firstBytes,
		"a row grew far more than this writer explains; clone-B moved with it")
}

// TestBenchWindowSplitLeavesTwoMemtables is the harness checking itself again.
// A switch leaving the new active memtable empty would have the reader drop it,
// and the flushing row of [BenchmarkRoaringSetWindowRead] would quietly re-time
// the single-memtable case beside it under a different name.
func TestBenchWindowSplitLeavesTwoMemtables(t *testing.T) {
	bucket, keys, memKeys := benchWindowFixture(t, 50, 1, benchWindowBatch)

	view := bucket.GetConsistentView()
	defer view.ReleaseView()
	before, err := newRoaringSetBatchReaderWithBounds(view.WithoutEmptyActiveMemtable(), keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
	require.NoError(t, err)
	require.Equal(t, 1, before.Stats().Memtables,
		"the fixture must start on one memtable, or the split below proves nothing")

	benchWindowSplitAcrossMemtables(t, bucket, memKeys, 1, benchWindowBatch)

	split := bucket.GetConsistentView()
	defer split.ReleaseView()
	after, err := newRoaringSetBatchReaderWithBounds(split.WithoutEmptyActiveMemtable(), keys, BatchReaderWindowKeys, BatchReaderWindowBytes)
	require.NoError(t, err)
	require.Equal(t, 2, after.Stats().Memtables,
		"the split must leave a flushing memtable and a live one the reader keeps")
}

// BenchmarkRoaringSetWindowRead is the reader side alone, with nothing
// writing: what the window is worth before any contention. clone-B is
// reported beside the timings since allocs/op is flat across every size here;
// read the timings for what the window is worth, clone-B for what it costs.
func BenchmarkRoaringSetWindowRead(b *testing.B) {
	// One row per document leaves nothing for a stride to spread. The flushing
	// row is the clustered one with a flush left in flight, so the pair prices
	// a second memtable: two reads per fill against one, and half the budget
	// each. These rows stay well inside that half, so nothing narrows here.
	rows := []struct {
		label      string
		docsPerRow int
		docsStride int
		flushing   bool
	}{
		{"docs=1", 1, benchWindowBatch, false},
		{"docs=1000/spread=clustered", 1000, 1, false},
		{"docs=1000/spread=clustered/flushing", 1000, 1, true},
		{"docs=1000/spread=strided", 1000, benchWindowBatch, false},
	}
	for _, row := range rows {
		for _, diskHitPct := range []int{0, 90} {
			for _, window := range benchWindows {
				name := fmt.Sprintf("%s/disk=%d%%/window=%d", row.label, diskHitPct, window)
				b.Run(name, func(b *testing.B) {
					bucket, keys, memKeys := benchWindowFixture(b, diskHitPct, row.docsPerRow, row.docsStride)
					if row.flushing {
						benchWindowSplitAcrossMemtables(b, bucket, memKeys, row.docsPerRow, row.docsStride)
					}
					cloneBytes := benchWindowCloneBytes(b, bucket, keys, window)
					b.ResetTimer()
					b.ReportAllocs()
					for n := 0; n < b.N; n++ {
						benchWindowFold(b, bucket, keys, window)
					}
					b.ReportMetric(float64(cloneBytes), "clone-B")
				})
			}
		}
	}
}

// BenchmarkRoaringSetWindowUnderWrite folds the batch from several readers
// with one writer on the same memtable. b.N is the number of folds shared
// across the readers; fold-ms is one fold's cost, w- metrics are the writer's.
func BenchmarkRoaringSetWindowUnderWrite(b *testing.B) {
	for _, readers := range benchWindowReaderCounts() {
		for _, diskHitPct := range []int{0, 90} {
			for _, window := range benchWindows {
				name := fmt.Sprintf("readers=%d/disk=%d%%/window=%d", readers, diskHitPct, window)
				b.Run(name, func(b *testing.B) {
					bucket, keys, memKeys := benchWindowFixture(b, diskHitPct, 1, benchWindowBatch)

					var remaining atomic.Int64
					remaining.Store(int64(b.N))
					var readersDone, writerDone sync.WaitGroup
					var stopWriter atomic.Bool
					var mu sync.Mutex
					var foldLat, writeLat []time.Duration
					var writeTotal int
					var writeErr atomic.Pointer[error]

					b.ResetTimer()
					start := time.Now()

					writerDone.Add(1)
					go func() {
						defer writerDone.Done()
						local := make([]time.Duration, writeLatCap)
						writes := 0
						for ; !stopWriter.Load(); writes++ {
							t0 := time.Now()
							err := bucket.RoaringSetAddList([]byte(memKeys[writes%len(memKeys)]), []uint64{uint64(writes)})
							if err != nil {
								writeErr.Store(&err)
								return
							}
							local[writes%writeLatCap] = time.Since(t0)
						}
						mu.Lock()
						writeLat = local[:min(writes, writeLatCap)]
						writeTotal = writes
						mu.Unlock()
					}()

					for r := 0; r < readers; r++ {
						readersDone.Add(1)
						go func() {
							defer readersDone.Done()
							local := make([]time.Duration, 0, 64)
							for remaining.Add(-1) >= 0 {
								t0 := time.Now()
								benchWindowFold(b, bucket, keys, window)
								local = append(local, time.Since(t0))
							}
							mu.Lock()
							foldLat = append(foldLat, local...)
							mu.Unlock()
						}()
					}
					readersDone.Wait()
					stopWriter.Store(true)
					writerDone.Wait()
					elapsed := time.Since(start)
					b.StopTimer()

					if err := writeErr.Load(); err != nil {
						b.Fatal(*err)
					}
					if len(foldLat) == 0 || len(writeLat) == 0 {
						b.Fatal("nothing recorded")
					}
					pct := func(d []time.Duration, p int) float64 {
						sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
						return float64(d[len(d)*p/100].Nanoseconds())
					}
					b.ReportMetric(pct(foldLat, 50)/1e6, "fold-ms")
					b.ReportMetric(pct(writeLat, 50)/1e3, "w-p50-us")
					b.ReportMetric(pct(writeLat, 99)/1e3, "w-p99-us")
					// writes/s counts every write; the percentiles above only cover the
					// samples the ring retained, which w-samples reports.
					b.ReportMetric(float64(writeTotal)/elapsed.Seconds(), "writes/s")
					b.ReportMetric(float64(len(writeLat)), "w-samples")
				})
			}
		}
	}
}
