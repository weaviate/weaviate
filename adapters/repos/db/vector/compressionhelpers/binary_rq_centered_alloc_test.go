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

// The race detector makes sync.Pool drop items at random, so allocation
// counts are only meaningful without it.
//go:build !race

package compressionhelpers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func newAllocTestCRQ1(tb testing.TB, dim int) (*CenteredBinaryRotationalQuantizer, []float32) {
	tb.Helper()
	vecs, mean := crqTestData(2, dim, 7)
	rq, err := NewCenteredBinaryRotationalQuantizer(dim, 42, mean, distancer.NewCosineDistanceProvider())
	require.NoError(tb, err)
	return rq, vecs[0]
}

// TestCenteredRQ1Allocs pins the pooled scratch buffers: Encode allocates
// only the returned code, and NewDistancer only the retained query code
// (five bit planes) and the distancer itself.
func TestCenteredRQ1Allocs(t *testing.T) {
	rq, x := newAllocTestCRQ1(t, 1536)
	rq.Encode(x) // warm the pool

	require.Equal(t, 1.0, testing.AllocsPerRun(100, func() { rq.Encode(x) }), "Encode")
	require.Equal(t, 6.0, testing.AllocsPerRun(100, func() { rq.NewDistancer(x) }), "NewDistancer")
}

func BenchmarkCenteredRQ1Encode(b *testing.B) {
	rq, x := newAllocTestCRQ1(b, 1536)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rq.Encode(x)
	}
}

func BenchmarkCenteredRQ1NewDistancer(b *testing.B) {
	rq, x := newAllocTestCRQ1(b, 1536)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rq.NewDistancer(x)
	}
}
