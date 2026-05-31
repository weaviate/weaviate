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

// Local PR-grade perf evidence for the D2 V2 property-length read path. These
// benchmarks address the D1-review G5 latency concern WITHOUT a cluster: they
// measure the production functions propLengthAtV2 / propLengthForScore against
// the legacy access shapes (V0 resident-slice binary search; the pre-D1
// map[uint64]uint32 lookup) at 1M and 10M docs, under BOTH the warm/monotonic
// access pattern (the real BlockMax-WAND cursor) and the cache-cold random
// pattern (the worst case the review worried about).
//
// All benchmarks run mmap-mode (readFromMemory=true) -- the production hot
// path -- and reuse the real on-disk section layout via buildV2Section. No
// production source is touched; this file is test-only.
//
// Run scoped, e.g.:
//
//	go test -run=^$ -bench='BenchmarkPropLength' -benchmem \
//	  -benchtime=2000000x ./adapters/repos/db/lsmkv/
//	go test -run=TestV2HotPathZeroAlloc ./adapters/repos/db/lsmkv/

import (
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

// benchN are the segment sizes the D1 review named as the worry zone: 1M docs
// is a typical large shard, 10M docs is the 80MB-docID-column case (10M*8B)
// the review flagged for cache-cold DRAM misses per probe.
var benchN = []struct {
	name string
	n    int
}{
	{"1M", 1_000_000},
	{"10M", 10_000_000},
}

// buildBenchV2Segment builds a V2 mmap segment with n docs, docIDs strided so
// they are sparse-but-sorted (docID = i*7+1, modeling the real non-contiguous
// global docID space the gallop must search), and loads the V2 reader. Setup
// allocations are outside the timed loop; the caller calls b.ResetTimer after.
func buildBenchV2Segment(tb testing.TB, n int) (*segment, []uint64) {
	tb.Helper()
	docIDs := make([]uint64, n)
	lengths := make([]uint32, n)
	for i := 0; i < n; i++ {
		docIDs[i] = uint64(i)*7 + 1
		// realistic-ish lengths; value does not affect lookup cost.
		lengths[i] = uint32((i % 4096) + 1)
	}
	section := buildV2Section(5.0, uint64(n), docIDs, lengths, 0)

	const pad = 64
	contents := make([]byte, pad+len(section))
	copy(contents[pad:], section)

	s := &segment{
		path:           "bench-v2-mmap",
		strategy:       segmentindex.StrategyInverted,
		readFromMemory: true,
		contents:       contents,
		logger:         logrus.New(),
		invertedHeader: &segmentindex.HeaderInverted{
			Version:               segmentindex.SegmentInvertedVersionV2,
			PropertyLengthsOffset: pad,
		},
		invertedData: &segmentInvertedData{},
	}
	if err := s.loadPropertyLengthsV2(); err != nil {
		tb.Fatalf("loadPropertyLengthsV2: %v", err)
	}
	return s, docIDs
}

// buildBenchV0Slices builds the V0 resident sorted-slice representation
// (propLengthDocIDs/propLengthValues) the V0 score path binary-searches via
// propLengthAtLocked, plus the pre-D1 Go map for the map-lookup baseline.
func buildBenchV0Slices(n int) (*segmentInvertedData, map[uint64]uint32, []uint64) {
	docIDs := make([]uint64, n)
	values := make([]uint32, n)
	m := make(map[uint64]uint32, n)
	for i := 0; i < n; i++ {
		id := uint64(i)*7 + 1
		v := uint32((i % 4096) + 1)
		docIDs[i] = id
		values[i] = v
		m[id] = v
	}
	d := &segmentInvertedData{
		propLengthDocIDs:      docIDs,
		propLengthValues:      values,
		propertyLengthsLoaded: true,
	}
	return d, m, docIDs
}

// shuffledOrder returns a permutation of [0,n) for the cache-cold random probe
// pattern. A fixed seed keeps the bench deterministic run-to-run.
func shuffledOrder(n int) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	r := rand.New(rand.NewSource(0xD2))
	r.Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order
}

// ---------------------------------------------------------------------------
// Group 1: gallop (V2) vs binary-search-slice (V0) vs map, warm + cold.
// ---------------------------------------------------------------------------

// BenchmarkPropLengthV2_GallopWarm is the REAL BlockMax-WAND pattern: docIDs
// probed in ascending order, threading the per-iterator gallop cursor so each
// lookup starts spatially local to the previous one. Amortized near-O(1).
func BenchmarkPropLengthV2_GallopWarm(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			s, docIDs := buildBenchV2Segment(b, tc.n)
			n := uint64(tc.n)
			b.ReportAllocs()
			b.ResetTimer()
			var cursor uint64
			var sink uint32
			for i := 0; i < b.N; i++ {
				// monotonic ascending sweep; wrap resets the cursor (a new query).
				j := uint64(i) % n
				if j == 0 {
					cursor = 0
				}
				sink = s.propLengthForScore(docIDs[j], &cursor)
			}
			runtimeSink32 = sink
		})
	}
}

// BenchmarkPropLengthV2_GallopCold is the cache-cold worst case: probes arrive
// in shuffled order and the gallop cursor is RESET to 0 every call, so the
// gallop+binary-search re-traverses the full O(log n) probe chain across the
// 80-800MB column with no locality -- every probe is a likely LLC/DRAM miss.
func BenchmarkPropLengthV2_GallopCold(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			s, docIDs := buildBenchV2Segment(b, tc.n)
			order := shuffledOrder(tc.n)
			b.ReportAllocs()
			b.ResetTimer()
			var sink uint32
			for i := 0; i < b.N; i++ {
				var cursor uint64 // reset every call: no cursor locality
				idx := order[i%len(order)]
				sink = s.propLengthForScore(docIDs[idx], &cursor)
			}
			runtimeSink32 = sink
		})
	}
}

// BenchmarkPropLengthV0_BinarySearchWarm is the V0 resident-slice binary search
// (propLengthAtLocked) on monotonic-ascending probes -- the V0 score-path
// baseline. sort.Search is O(log n) every call (no cursor), but on resident
// Go slices that the allocator keeps hot.
func BenchmarkPropLengthV0_BinarySearchWarm(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			d, _, docIDs := buildBenchV0Slices(tc.n)
			n := len(docIDs)
			b.ReportAllocs()
			b.ResetTimer()
			var sink uint32
			for i := 0; i < b.N; i++ {
				sink = d.propLengthAtLocked(docIDs[i%n])
			}
			runtimeSink32 = sink
		})
	}
}

// BenchmarkPropLengthV0_BinarySearchCold is the V0 binary search on shuffled
// probes -- the V0 cache-cold counterpart to the V2 gallop-cold bench, so the
// two are apples-to-apples (same access pattern, different data structure).
func BenchmarkPropLengthV0_BinarySearchCold(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			d, _, docIDs := buildBenchV0Slices(tc.n)
			order := shuffledOrder(tc.n)
			b.ReportAllocs()
			b.ResetTimer()
			var sink uint32
			for i := 0; i < b.N; i++ {
				idx := order[i%len(order)]
				sink = d.propLengthAtLocked(docIDs[idx])
			}
			runtimeSink32 = sink
		})
	}
}

// BenchmarkPropLengthMap_Warm models the PRE-D1 map[uint64]uint32 lookup the
// review compared the gallop against ("~8ns/lookup slower than the old map").
// Monotonic key order.
func BenchmarkPropLengthMap_Warm(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			_, m, docIDs := buildBenchV0Slices(tc.n)
			n := len(docIDs)
			b.ReportAllocs()
			b.ResetTimer()
			var sink uint32
			for i := 0; i < b.N; i++ {
				sink = m[docIDs[i%n]]
			}
			runtimeSink32 = sink
		})
	}
}

// BenchmarkPropLengthMap_Cold is the map lookup on shuffled keys -- the
// cache-cold map baseline. A hash lookup is O(1) but each probe still chases a
// random bucket pointer across the 45+ B/doc map (45-450MB), so it suffers the
// same DRAM-miss exposure as the gallop on a large segment.
func BenchmarkPropLengthMap_Cold(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			_, m, docIDs := buildBenchV0Slices(tc.n)
			order := shuffledOrder(tc.n)
			b.ReportAllocs()
			b.ResetTimer()
			var sink uint32
			for i := 0; i < b.N; i++ {
				idx := order[i%len(order)]
				sink = m[docIDs[idx]]
			}
			runtimeSink32 = sink
		})
	}
}

// ---------------------------------------------------------------------------
// Group 3: legacy-WAND getPropertyLengths transient-map alloc (M2).
// ---------------------------------------------------------------------------

// BenchmarkGetPropertyLengthsV2_Cold characterizes the M2 transient-map
// allocation: the legacy non-BlockMax path calls getPropertyLengths(), which on
// a V2 segment materializes the full lossless run (8B+4B per doc) AND a fresh
// map[uint64]uint32 (~45+ B/doc) every call. This is the bounded cold-path
// regression vs the old by-reference cached-map return (0-alloc). The hot
// BlockMax score path does NOT call this -- it uses propLengthForScore.
func BenchmarkGetPropertyLengthsV2_Cold(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			s, _ := buildBenchV2Segment(b, tc.n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := s.getPropertyLengths()
				if err != nil {
					b.Fatalf("getPropertyLengths: %v", err)
				}
				runtimeSinkMapLen = len(m)
			}
		})
	}
}

// BenchmarkGetPropertyLengthsV0_ByReference is the OLD shape's lower bound: a
// pre-built resident map returned by reference (0 alloc/call). This is the
// baseline the M2 transient-map regression is measured against. It does not
// call production code (the production V0 path also materializes a transient
// map now via materializePropertyLengthsLocked); it isolates the by-reference
// cost so the regression delta is explicit.
func BenchmarkGetPropertyLengthsV0_ByReference(b *testing.B) {
	for _, tc := range benchN {
		b.Run(tc.name, func(b *testing.B) {
			_, m, _ := buildBenchV0Slices(tc.n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runtimeSinkMapLen = len(m) // by-reference: no per-call alloc
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Group 2: 0-alloc hot path -- hard gate (not just the -benchmem column).
// ---------------------------------------------------------------------------

// TestV2HotPathZeroAlloc asserts the V2 hot score read (propLengthForScore on a
// V2 mmap segment, threading the gallop cursor) does ZERO heap allocation per
// call. This is the lock-free immutable-mmap read claim: readDocIDV2/readLenV2
// slice s.contents zero-copy in readFromMemory mode, so the only work is index
// arithmetic + binary.LittleEndian reads -- no make, no map, no escape.
//
// This catches an alloc regression because testing.AllocsPerRun fails loudly if
// the average allocs/call exceeds 0 (e.g. if a future change made the gallop
// box the cursor, materialize a slice, or take the V0 lock path that touches a
// sync.RWMutex's internals). A -benchmem 0 allocs/op column alone could mask a
// sub-1-alloc-per-op average; AllocsPerRun rounds and asserts exactly 0.
func TestV2HotPathZeroAlloc(t *testing.T) {
	const n = 100_000
	s, docIDs := buildBenchV2Segment(t, n)

	// Monotonic cursor-threaded reads -- the real hot-path shape.
	var cursor uint64
	var k uint64
	allocs := testing.AllocsPerRun(2000, func() {
		j := k % uint64(n)
		if j == 0 {
			cursor = 0
		}
		runtimeSink32 = s.propLengthForScore(docIDs[j], &cursor)
		k++
	})
	if allocs != 0 {
		t.Fatalf("V2 hot score path must be 0-alloc; got %.4f allocs/op", allocs)
	}

	// Also assert the stateless single-shot (cursor reset every call) is 0-alloc:
	// the cold-access shape must not allocate either.
	allocsCold := testing.AllocsPerRun(2000, func() {
		var c uint64
		runtimeSink32 = s.propLengthForScore(docIDs[(k*2654435761)%uint64(n)], &c)
		k++
	})
	if allocsCold != 0 {
		t.Fatalf("V2 cold single-shot read must be 0-alloc; got %.4f allocs/op", allocsCold)
	}
}

// TestV2PreadPathZeroAlloc is the pread counterpart to TestV2HotPathZeroAlloc.
// It forces readFromMemory=false (the on-disk pread path) and asserts that the
// V2 gallop read via SegmentBlockMax.propLengthFor -- which calls propLengthAtV2
// with a per-iterator *[8]byte scratch buffer -- does ZERO heap allocations per
// call. This is the fix for the reviewer finding: the pre-fix pread path did
// make([]byte, 8) / make([]byte, 4) PER probe (12 allocs/op measured), which
// paid back the ~0-resident-heap win as per-query GC on memory-pressured nodes.
//
// The scratch buffer lives in the (already heap-allocated) SegmentBlockMax
// struct; readDocIDV2/readLenV2 use contentFile.ReadAt into scratch[0:8/0:4]
// directly, bypassing the bufferedReaderAt pool, so no intermediate allocation
// occurs. The test fails on the pre-fix tree (make escapes to heap per probe)
// and passes on the fixed tree (ReadAt into scratch, 0 alloc).
//
// CAUTION: the file I/O during AllocsPerRun means the OS page cache must be
// warm for the measurement to be repeatable; the test setup reads the segment
// once (loadPropertyLengthsV2) which warms it. On a cold machine the first run
// may differ; the 2000-iteration AllocsPerRun average is stable after warmup.
func TestV2PreadPathZeroAlloc(t *testing.T) {
	const n = 10_000
	docIDs := make([]uint64, n)
	lengths := make([]uint32, n)
	for i := 0; i < n; i++ {
		docIDs[i] = uint64(i)*7 + 1
		lengths[i] = uint32((i % 4096) + 1)
	}
	section := buildV2Section(5.0, uint64(n), docIDs, lengths, 0)
	s := newV2TestSegmentPread(t, section)
	if err := s.loadPropertyLengthsV2(); err != nil {
		t.Fatalf("loadPropertyLengthsV2: %v", err)
	}

	// Warm the page cache: one full sweep before we measure.
	var warmScratch [8]byte
	for i, d := range docIDs {
		length, _, err := s.propLengthAtV2(d, uint64(i), &warmScratch)
		if err != nil {
			t.Fatalf("warmup probe docID %d: %v", d, err)
		}
		if length != lengths[i] {
			t.Fatalf("warmup: docID %d: got length %d, want %d", d, length, lengths[i])
		}
	}

	// Measure allocations on the monotonic gallop cursor pattern -- the real
	// SegmentBlockMax hot path. The scratch buffer is allocated once here (part
	// of this stack frame) and reused across all 2000 iterations.
	var scratch [8]byte
	var cursor uint64
	var k uint64
	allocs := testing.AllocsPerRun(2000, func() {
		j := k % uint64(n)
		if j == 0 {
			cursor = 0
		}
		var err error
		runtimeSink32, cursor, err = s.propLengthAtV2(docIDs[j], cursor, &scratch)
		if err != nil {
			t.Errorf("pread propLengthAtV2 docID %d: %v", docIDs[j], err)
		}
		k++
	})
	if allocs != 0 {
		t.Fatalf("V2 pread hot score path must be 0-alloc after fix; got %.4f allocs/op", allocs)
	}
}

// Package-level sinks keep the compiler from optimizing the benchmarked reads
// away (the result must escape the loop).
var (
	runtimeSink32     uint32
	runtimeSinkMapLen int
)
