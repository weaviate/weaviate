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
	"testing"
)

// TODO aliszka:keydoccolumn drop before the PR is finished — this measures a
// one-time startup cost while it is still being tuned, and answers nothing a
// maintainer will ask again.
//
// BenchmarkKeyDocColumnPOC_Build measures the one-time startup build cost over
// the full corpus — the price paid once per property per shard on load. The
// index is built when the bucket opens, so each iteration reopens it; the
// "no_index" row is the same reopen without the index, and the gap between the
// two is what the index costs at load.
func BenchmarkKeyDocColumnPOC_Build(b *testing.B) {
	for _, withIndex := range []bool{false, true} {
		name := "no_index"
		if withIndex {
			name = "index"
		}
		b.Run(name, func(b *testing.B) {
			f := newContainsFixture(b, benchCorpusSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.reopenBucket(b, withIndex)
			}
		})
	}
}
