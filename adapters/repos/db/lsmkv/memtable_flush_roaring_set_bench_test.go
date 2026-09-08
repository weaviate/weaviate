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
	"fmt"
	"os"
	"runtime"
	"testing"
)

// The benchmark fixture's cardinalities, chosen to mirror a filterable inverted
// index rather than the golden fixture's node shapes. What separates the two
// rewrites is allocation, not segment size: doc IDs spread across container
// keys make an empty-bitmap-plus-Or grow, where a size-first rewrite allocates
// once. IDs that all fit one container separate nothing.
const (
	benchmarkHighCardKeys = 3
	benchmarkHighCardIDs  = 50_000
	benchmarkLowCardKeys  = 500
	benchmarkLowCardIDs   = 5
	benchmarkSlackIDs     = 10_000
	benchmarkSlackKept    = 200
)

// benchmarkFlushShapes holds a few high-cardinality keys, many low-cardinality
// ones, and one key whose additions were mostly removed again so it carries
// container slack. The low-cardinality keys start after the high-cardinality
// block on purpose: that puts their doc IDs above container key 0, which is
// where nearly all of the separation between the two rewrites comes from.
// Starting them at 0 would collapse this fixture to the golden one.
// checksumSettings is what every benchmark here crosses. Production defaults
// to off, and the checksum path routes every written byte through the segment
// file's hash, so a figure read from one setting says nothing about the other.
var checksumSettings = []bool{false, true}

func benchmarkFlushShapes() []fixtureShape {
	shapes := make([]fixtureShape, 0, benchmarkHighCardKeys+benchmarkLowCardKeys+1)

	var next uint64
	for i := 0; i < benchmarkHighCardKeys; i++ {
		shapes = append(shapes, fixtureShape{
			key: []byte(fmt.Sprintf("high-%03d", i)),
			add: docIDRange(next, next+benchmarkHighCardIDs),
		})
		next += benchmarkHighCardIDs
	}

	for i := 0; i < benchmarkLowCardKeys; i++ {
		shapes = append(shapes, fixtureShape{
			key: []byte(fmt.Sprintf("low-%04d", i)),
			add: docIDRange(next, next+benchmarkLowCardIDs),
		})
		next += benchmarkLowCardIDs
	}

	return append(shapes, fixtureShape{
		key:    []byte("slack"),
		add:    docIDRange(next, next+benchmarkSlackIDs),
		remove: docIDRange(next+benchmarkSlackKept, next+benchmarkSlackIDs),
	})
}

func BenchmarkFlushRoaringSet(b *testing.B) {
	shapes := benchmarkFlushShapes()
	for _, checksums := range checksumSettings {
		b.Run(fmt.Sprintf("checksums=%t", checksums), func(b *testing.B) {
			b.ReportAllocs()
			// Without this the fixed pre-loop cost is divided by b.N, so a change
			// that makes the flush faster raises b.N and books the smaller share
			// as a memory win it did not earn.
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// flush() closes and deletes its commit log, so a memtable cannot
				// be flushed twice and each iteration builds its own outside the
				// timer.
				b.StopTimer()
				m := newRoaringSetFlushFixture(b, shapes, checksums)
				b.StartTimer()

				segmentPath, err := m.flush()

				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if err := os.Remove(segmentPath); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkFlushRoaringSetRetention reports how much heap the index keys the
// flush returns hold on to. Sampling HeapInuse during the flush cannot see
// this: no collection runs inside a flush, so nothing is reclaimed inside the
// sampled window however much is retained, and the reading moves with what was
// allocated rather than with what is held.
//
// The figure is a measurement rather than a gate. It is read across steps: a
// change that stops the returned keys pinning the serialized nodes shows up
// here and nowhere else in the benchmark output.
// Its two forced collections dominate its own ns/op; read timings from
// BenchmarkFlushRoaringSet.
func BenchmarkFlushRoaringSetRetention(b *testing.B) {
	shapes := benchmarkFlushShapes()
	for _, checksums := range checksumSettings {
		b.Run(fmt.Sprintf("checksums=%t", checksums), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			// Every iteration measures the same deterministic quantity under
			// forced collections, so the last reading is the reported one.
			var retained uint64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				m := newRoaringSetFlushFixture(b, shapes, checksums)
				b.StartTimer()

				retained = retainedByFlushKeys(b, m)
			}

			b.ReportMetric(float64(retained), "retained-B")
		})
	}
}

// retainedByFlushKeys reports the heap still held once the flush's index keys
// are reachable, less the heap held after they go out of scope. The keys live
// in their own call so that scope drops them: assigning nil instead would read
// as an ineffectual assignment, since nothing here reads them back.
func retainedByFlushKeys(b *testing.B, m *Memtable) uint64 {
	b.Helper()

	held := func() uint64 {
		keys, err := m.flushDataRoaringSet(discardingSegmentFile())
		if err != nil {
			b.Fatal(err)
		}

		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		runtime.KeepAlive(keys)
		return ms.HeapAlloc
	}()

	runtime.GC()
	var dropped runtime.MemStats
	runtime.ReadMemStats(&dropped)

	if held <= dropped.HeapAlloc {
		return 0
	}
	return held - dropped.HeapAlloc
}
