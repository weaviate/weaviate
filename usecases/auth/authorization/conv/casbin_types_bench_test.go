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

package conv

import "testing"

// validResource runs once per policy on every role read, and validVerb runs on
// every authorization audit line. Both matched against freshly compiled
// patterns before, which dominated listing users. These benchmarks pin the
// compiled-once behaviour: a regression back to compiling per call shows up as
// a jump from nanoseconds to tens of microseconds and an allocation count in
// the hundreds.

func BenchmarkValidResource_Schema(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		validResource("schema/collections/Movies")
	}
}

// The data domain sits late in resourcePatterns, so it walks nearly the whole
// pattern list before matching — the worst case for per-call compilation.
func BenchmarkValidResource_Data(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		validResource("data/collections/Movies/shards/s1/objects/o1")
	}
}

func BenchmarkValidVerb(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		validVerb("R")
	}
}
