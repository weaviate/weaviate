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

// The synthetic shard reproduces the shape measured in a large production
// deployment: ~24M docs per shard behind an integer range predicate that ~97.5%
// of them satisfy, which is 369 containers / ~2.88 MiB per plane.
//
// This file is written to compile unchanged against v1.37, so the same
// benchmark can be run on a pristine checkout for the baseline arm.
var (
	cascadeBenchDocs      = flag.Int("cascadebench.docs", 24_100_000, "documents in the synthetic shard")
	cascadeBenchThreshold = flag.Int64("cascadebench.threshold", 101, "range predicate threshold")
)

var cascadeBenchSegment *SegmentInMemory

// cascadeBenchEncodeInt64 mirrors how the inverted index stores an int64: the
// sign bit is flipped, so a non-negative threshold always sets bit 63.
func cascadeBenchEncodeInt64(v int64) uint64 { return uint64(v ^ math.MinInt64) }

func cascadeBenchScore(rng *rand.Rand) int64 {
	if rng.Float64() < 0.025 {
		return rng.Int63n(101)
	}
	return 101 + int64(rng.ExpFloat64()*400)
}

// buildCascadeBenchSegment materialises the 65 planes one at a time so peak
// memory stays near one plane's worth of doc IDs rather than 65.
func buildCascadeBenchSegment(b *testing.B) *SegmentInMemory {
	if cascadeBenchSegment != nil {
		return cascadeBenchSegment
	}

	logger, _ := test.NewNullLogger()
	rng := rand.New(rand.NewSource(42))

	n := *cascadeBenchDocs
	scores := make([]uint64, n)
	for i := range scores {
		scores[i] = cascadeBenchEncodeInt64(cascadeBenchScore(rng))
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

	cascadeBenchSegment = s
	return s
}

func cascadeBenchReader(b *testing.B) *segmentInMemoryReader {
	s := buildCascadeBenchSegment(b)
	return &segmentInMemoryReader{
		bitmaps: s.bitmaps,
		bufPool: roaringset.NewBitmapBufPoolNoop(),
	}
}

// BenchmarkCascadeGreaterThanEqual measures the range leaf at the production
// predicate value. Its lowest set bit is bit 0, so the seeded cascade drops one
// of the whole-shard passes the shipped cascade performs.
func BenchmarkCascadeGreaterThanEqual(b *testing.B) {
	value := cascadeBenchEncodeInt64(*cascadeBenchThreshold)

	for _, conc := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", conc), func(b *testing.B) {
			r := cascadeBenchReader(b)
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

// BenchmarkCascadeBetween covers the equality leaf, where both cascades seed
// independently and two whole-shard passes are dropped.
func BenchmarkCascadeBetween(b *testing.B) {
	value := cascadeBenchEncodeInt64(*cascadeBenchThreshold)

	for _, conc := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", conc), func(b *testing.B) {
			r := cascadeBenchReader(b)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bm, release := r.mergeBetween(value, value+1, conc)
				if bm == nil {
					b.Fatal("nil result")
				}
				release()
			}
		})
	}
}
