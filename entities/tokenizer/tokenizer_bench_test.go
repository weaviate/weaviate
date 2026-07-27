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

package tokenizer

import (
	"fmt"
	"testing"

	"github.com/weaviate/weaviate/entities/models"
)

var benchTokenizations = []string{
	models.PropertyTokenizationField,
	models.PropertyTokenizationWord,
	models.PropertyTokenizationWhitespace,
	models.PropertyTokenizationLowercase,
	models.PropertyTokenizationTrigram,
}

// BenchmarkAnalyze guards the single-value allocation profile: the append-style
// kernels must not cost more than materializing a fresh slice per call did.
func BenchmarkAnalyze(b *testing.B) {
	for _, tokenization := range benchTokenizations {
		b.Run(tokenization, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Analyze("some value number 42 here", tokenization, "C", nil, nil)
			}
		})
	}
}

// BenchmarkAnalyzeBatch is the reason the kernels are append-style: allocations
// per batch should stay flat in the number of values rather than scaling with
// it. Compare against BenchmarkAnalyzeBatchPerValueLoop.
func BenchmarkAnalyzeBatch(b *testing.B) {
	values := benchValues(1000)
	for _, tokenization := range benchTokenizations {
		b.Run(tokenization, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = AnalyzeBatch(values, tokenization, "C", nil, nil)
			}
		})
	}
}

// BenchmarkAnalyzeBatchPerValueLoop is the baseline AnalyzeBatch replaces:
// calling Analyze per value and keeping each result.
func BenchmarkAnalyzeBatchPerValueLoop(b *testing.B) {
	values := benchValues(1000)
	for _, tokenization := range benchTokenizations {
		b.Run(tokenization, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := make([][]string, len(values))
				for j, v := range values {
					out[j] = Analyze(v, tokenization, "C", nil, nil).Query
				}
			}
		})
	}
}

func benchValues(n int) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = fmt.Sprintf("some value number %d here", i)
	}
	return values
}
