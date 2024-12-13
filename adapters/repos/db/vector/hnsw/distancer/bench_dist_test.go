//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package distancer_test

import (
	"math/rand"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func BenchmarkDist(b *testing.B) {
	distancerDot := distancer.NewDotProductProvider()
	v, _ := testinghelpers.RandomVecs(2, 0, 1536)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		distancerDot.SingleDist(v[0], v[1])
	}
}

func BenchmarkAllow(b *testing.B) {
	ids := make([]uint64, 0, 40_000)
	for i := 0; i < 40_000; i++ {
		ids = append(ids, rand.Uint64()%300_000_000)
	}
	allow := helpers.NewAllowList(ids...)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allow.Contains(rand.Uint64() % 300_000_000)
	}
}
