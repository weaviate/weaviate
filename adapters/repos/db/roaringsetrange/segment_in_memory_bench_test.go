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
	"flag"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// The synthetic shard reproduces the shape of a large production deployment:
// ~24.1 M documents per shard, an integer range predicate that ~97.5% of
// documents satisfy, and therefore ~369 sroar containers (~2.88 MiB) per plane.
var (
	benchDocs      = flag.Int("bench.docs", 24_100_000, "documents in the synthetic shard")
	benchThreshold = flag.Int64("bench.threshold", 101, "range predicate threshold")
)

var benchSegment *SegmentInMemory

// encodeInt64 mirrors entities/inverted.LexicographicallySortableInt64 followed
// by binary.BigEndian.Uint64: the sign bit is flipped, so plane 64 is a copy of
// plane 0 for any non-negative value.
func encodeInt64(v int64) uint64 { return uint64(v ^ math.MinInt64) }

// benchScore models a retailer score: ~97.5% of documents land above the
// threshold, matching the measured selectivity.
func benchScore(rng *rand.Rand) int64 {
	if rng.Float64() < 0.025 {
		return rng.Int63n(101)
	}
	return 101 + int64(rng.ExpFloat64()*400)
}

// buildBenchSegment materialises the 65 planes one at a time, so peak memory
// stays near one plane's worth of doc IDs rather than 65.
func buildBenchSegment(b *testing.B) *SegmentInMemory {
	if benchSegment != nil {
		return benchSegment
	}

	logger, _ := test.NewNullLogger()
	rng := rand.New(rand.NewSource(42))

	n := *benchDocs
	scores := make([]uint64, n)
	for i := range scores {
		scores[i] = encodeInt64(benchScore(rng))
	}

	s := NewSegmentInMemory(logger)
	ids := make([]uint64, 0, n)
	for plane := 0; plane < 65; plane++ {
		ids = ids[:0]
		for i := 0; i < n; i++ {
			if plane == 0 || scores[i]&(1<<uint(plane-1)) != 0 {
				ids = append(ids, uint64(i))
			}
		}
		s.bitmaps[plane] = sroar.FromSortedList(ids)
	}

	b.Logf("built %d-doc shard, plane 0 = %.2f MiB (%d containers)",
		n, float64(s.bitmaps[0].LenInBytes())/(1<<20), s.bitmaps[0].LenInBytes()/8192)

	benchSegment = s
	return s
}

// benchReader returns a reader over the shared shard with an independent cache,
// so the arms cannot warm each other.
func benchReader(b *testing.B, cacheBytes int) *segmentInMemoryReader {
	s := buildBenchSegment(b)
	return &segmentInMemoryReader{
		bitmaps: s.bitmaps,
		bufPool: roaringset.NewBitmapBufPoolNoop(),
		cache:   newLeafCache(cacheBytes),
	}
}

// BenchmarkMergeGreaterThanEqual measures the range-filter leaf against the
// merge-worker axis. Arms:
//
//	shipped    — cache disabled, i.e. the loop exactly as it ships
//	cache-hit  — the memoised path: one clone, no cascade
//	cache-miss — the long-tail path: probe, then the full cascade. Nothing is
//	             cloned and nothing is retained, which is what a workload with no
//	             repeated predicates pays.
//
// Admission, which happens once per (predicate, generation), costs the cascade
// plus one bitmap clone; BenchmarkLeafAdmissionClone measures that clone.
func BenchmarkMergeGreaterThanEqual(b *testing.B) {
	value := encodeInt64(*benchThreshold)

	arms := []struct {
		name  string
		build func(b *testing.B) *segmentInMemoryReader
	}{
		{
			name:  "shipped",
			build: func(b *testing.B) *segmentInMemoryReader { return benchReader(b, 0) },
		},
		{
			name: "cache-hit",
			build: func(b *testing.B) *segmentInMemoryReader {
				r := benchReader(b, 64<<20)
				// two probes to clear second-sight admission, then store
				r.mergeGreaterThanEqual(value, 1)
				_, release := r.mergeGreaterThanEqual(value, 1)
				release()
				return r
			},
		},
		{
			name: "cache-miss",
			build: func(b *testing.B) *segmentInMemoryReader {
				// a budget of one byte never has room, so the arm stays on the
				// miss path forever
				r := benchReader(b, 1)
				r.mergeGreaterThanEqual(value, 1)
				return r
			},
		},
	}

	for _, arm := range arms {
		for _, conc := range []int{1, 4, 8} {
			b.Run(fmt.Sprintf("%s/workers=%d", arm.name, conc), func(b *testing.B) {
				r := arm.build(b)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					bm, release := r.mergeGreaterThanEqual(value, conc)
					if bm == nil {
						b.Fatal("nil result")
					}
					release()
				}
			})
		}
	}
}

// BenchmarkLeafCacheProbeMiss is the cost a workload with no repeated
// predicates pays per query: one linear scan of the entries and one of the
// admission filter. Nothing is cloned and nothing is retained.
func BenchmarkLeafCacheProbeMiss(b *testing.B) {
	c := newLeafCache(64 << 20)
	// fill the admission filter and hold a few entries, the worst case for the
	// two linear scans
	for i := uint64(0); i < leafCacheAdmissions; i++ {
		key := leafKey{kind: leafGreaterThanEqual, valueMin: i}
		c.probe(0, key, 0)
		c.probe(0, key, 0)
		c.store(0, key, roaringset.NewBitmap(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.probe(0, leafKey{kind: leafGreaterThanEqual, valueMin: uint64(i) + 1e6}, 0)
	}
}

// BenchmarkLeafAdmissionClone is the one-time cost of admitting a leaf into the
// cache: an independent copy of the merged bitmap, paid once per predicate per
// segment generation.
func BenchmarkLeafAdmissionClone(b *testing.B) {
	s := buildBenchSegment(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if s.bitmaps[0].Clone() == nil {
			b.Fatal("nil clone")
		}
	}
}
