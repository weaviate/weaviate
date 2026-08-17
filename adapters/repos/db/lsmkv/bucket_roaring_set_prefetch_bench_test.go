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

// What memtableWindowKeys trades, measured on a bucket rather than on stand-ins.
//
// The window bounds how long the memtable's read lock is held, so it is a
// reader/writer trade and neither side alone settles it. Three things decide
// whether the numbers mean anything, and getting any of them wrong quietly
// answers a different question:
//
//   - The fixture is built inside each sub-benchmark. Shared across a sweep, a
//     writer grows the memtable as it runs and how much depends on the window
//     under test, so the variable would be driving the conditions.
//   - The writer only touches keys the memtable already holds, so it adds bytes
//     and never nodes: the tree the readers walk stays the size it started.
//   - The disk is populated. Against an empty one every row is a miss and
//     cloning memtable rows becomes the reader's entire cost, which is not the
//     shape a flushed bucket has. The disk hit rate is swept because it decides
//     whether a row has a disk half to fold into at all.
//
// TestBenchWindowFixtureIsStable checks the first two before any number here is
// worth reading.
//
// Reader cost is timed in the reader rather than taken from ns/op, which with
// several readers sharing b.N is a fold divided by the reader count. Writer
// throughput is per second, not a raw count over a wall time that varies from
// one sub-benchmark to the next. Writer latencies are bimodal and the tail
// needs a long run to settle, so the sample count is reported alongside it.
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

// writeLatCap bounds the writer's latency samples. Appending without a bound
// reallocates on the writer goroutine, inside the loop being timed, so the tail
// this benchmark exists to report would carry the cost of recording it. The ring
// is allocated once and never grows: the documented run fits entirely, and only a
// sustained one wraps, keeping the most recent writes rather than the warm-up.
const writeLatCap = 1 << 20

// benchWindowReaderCounts straddles the core count, since that is what the
// contention conclusions are stated against. Hardcoding readers instead ties
// them to whichever machine picked the numbers: the same pair sits either side
// of the threshold on one host and wholly below it on a bigger one, and the
// sweep then reports a different shape without saying anything changed.
func benchWindowReaderCounts() []int {
	procs := runtime.GOMAXPROCS(0)
	// Deduped: on a small host procs/2 and procs collide, and two sub-benchmarks
	// under one name are not a sweep.
	counts := []int{max(2, procs/2), procs, 2 * procs}
	slices.Sort(counts)
	return slices.Compact(counts)
}

func benchWindowKey(i int) string { return fmt.Sprintf("key_%08d", i) }

// benchWindowDocs spreads a row's documents stride apart, which is what decides
// what cloning the row costs: a clone copies whole containers, so a row's cost
// follows how many of them its documents touch rather than how many documents it
// holds.
//
// Both ends of that range are ordinary, and they are far apart. A property with N
// values ingested in document order gives one value's documents a stride of N, so
// at benchWindowBatch a thousand documents touch about 305 of the 65,536-id
// containers and cost about 47KB. The same thousand ingested grouped by value sit
// adjacent, share one container, and cost about 2KB. Twenty-two times apart for the
// same count, which is why the spread is a dimension here and not a constant: a
// figure from one end says nothing about the other. Neither end is the worst case —
// a stride of a full container gives one container per document, about 141KB.
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
// docsPerRow and docsStride are together the dimension the window's memory cost
// scales with, and the one a filter has no say over: a low-cardinality property
// carries thousands of documents per value, and how those documents are spread
// decides what each one adds. Both are separate from the key count because they
// move independently — the batch decides how many rows are cloned, these decide
// what each clone costs. See benchWindowDocs for how far apart the ends are.
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

// benchWindowFold reads the whole batch through one reader the way the contains
// fold does: in order, once each, releasing as it goes.
func benchWindowFold(tb testing.TB, b *Bucket, keys inverted.SortedKeys, window int) {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	r, err := newRoaringSetBatchReader(view, keys, window)
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

// benchWindowCloneBytes is what one filled window holds at once: the cost the
// window size actually bounds, and the one allocs/op cannot report, since it
// counts allocation events and this is live bytes. It fills through the reader
// rather than serving through it, because a served slot releases its clone.
func benchWindowCloneBytes(tb testing.TB, b *Bucket, keys inverted.SortedKeys, window int) int {
	view := b.GetConsistentView()
	defer view.ReleaseView()

	r, err := newRoaringSetBatchReader(view, keys, window)
	require.NoError(tb, err)
	require.NoError(tb, r.fillWindow())

	total := 0
	for mt := 0; mt < r.mtCount; mt++ {
		for _, layer := range r.layers[mt] {
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

// TestBenchWindowFixtureIsStable is the harness checking itself, and the reason
// the sweep can be read as a comparison: a fixture comes out the same every time
// it is built, in nodes and in the bytes they hold, and running the writer
// leaves the node count where it found it. A sweep sharing one fixture, or a
// writer that grows the tree, measures the window against a moving target.
func TestBenchWindowFixtureIsStable(t *testing.T) {
	// Nodes and the bytes they hold, since the two move for different reasons.
	// The node count is what a shared fixture or a tree-growing writer would
	// disturb; the bytes are what a row costs to clone, which is what clone-B
	// reports and what every memory conclusion drawn from these benchmarks
	// rests on.
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
	// Not that the encoding grows: at this write count sroar's containers absorb
	// the new documents without growing, and the sweep's own writer runs long
	// enough that they do. What is asserted is the bound — a fixture change that
	// made a row cost several times more to clone fails here rather than
	// silently moving every clone-B number.
	require.LessOrEqual(t, afterBytes, 4*firstBytes,
		"a row grew far more than this writer explains; clone-B moved with it")
}

// BenchmarkRoaringSetWindowRead is the reader side alone, with nothing writing:
// what the window is worth before any contention.
//
// clone-B is reported beside the timings because the two answer different
// questions and only one of them scales with the window: allocs/op is flat
// across every size here, while clone-B — the live bytes one filled window
// holds — is what a bigger window actually spends. Read the timings for what
// the window is worth and clone-B for what it costs.
func BenchmarkRoaringSetWindowRead(b *testing.B) {
	// One row per document leaves nothing for a stride to spread, so the spread is
	// swept only where it changes anything.
	rows := []struct {
		label      string
		docsPerRow int
		docsStride int
	}{
		{"docs=1", 1, benchWindowBatch},
		{"docs=1000/spread=clustered", 1000, 1},
		{"docs=1000/spread=strided", 1000, benchWindowBatch},
	}
	for _, row := range rows {
		for _, diskHitPct := range []int{0, 90} {
			for _, window := range benchWindows {
				name := fmt.Sprintf("%s/disk=%d%%/window=%d", row.label, diskHitPct, window)
				b.Run(name, func(b *testing.B) {
					bucket, keys, _ := benchWindowFixture(b, diskHitPct, row.docsPerRow, row.docsStride)
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

// BenchmarkRoaringSetWindowUnderWrite folds the batch from several readers with
// one writer on the same memtable. b.N is the number of folds, shared out
// across the readers; fold-ms is what one of them cost and the w- metrics are
// what the writer paid for it.
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
					// Throughput is every write; the percentiles above are the samples
					// the ring retained, which w-samples reports so the two are not
					// mistaken for each other.
					b.ReportMetric(float64(writeTotal)/elapsed.Seconds(), "writes/s")
					b.ReportMetric(float64(len(writeLat)), "w-samples")
				})
			}
		}
	}
}
