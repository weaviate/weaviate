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

package packed

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkMaxSimKernels (below) measures the scoring cost per (query token,
// document token) pair of three scorers: the shipped rescoring loop over
// [][]float32 (referenceProduction, the same distance provider and so the same
// assembly), the int8-table reference RQ1LUTInt8Scorer, and the fast scan over
// the interleaved layout.
//
// Centered and uncentered RQ1 have identical per-pair arithmetic (centering is
// a subtraction at encode time and a correction after the maximum), so only
// the centered row is run. Blob parsing is outside the loop: it is a header
// read, measured with the rest of the read path.
//
// ns/pair is reported beside ns/op because the shapes have different pair
// counts. Compare runs on one machine only: arm64 and x86 figures are not
// comparable, and -race distorts the kernels.
//
//	go test -run '^$' -bench BenchmarkMaxSimKernels -benchtime 2s \
//	  ./adapters/repos/db/vector/multivector/packed/

// benchSink keeps the compiler from deleting a call whose result is unused.
var benchSink float32

// benchShapes are the (query tokens, document tokens, dims) shapes measured:
// 32 query tokens against one, eight and sixty-four blocks of 16 document
// tokens at d=128.
var benchShapes = []struct {
	nq, nd, dims int
}{
	{32, 16, 128},
	{32, 128, 128},
	{32, 1024, 128},
}

func BenchmarkMaxSimKernels(b *testing.B) {
	for _, s := range benchShapes {
		rng := rand.New(rand.NewSource(int64(s.nd)))
		query := unitTokens(rng, s.nq, s.dims)
		doc := unitTokens(rng, s.nd, s.dims)

		// --- RQ1, exact-query estimator, centered ---------------------------
		rp, err := NewRQ1Params(s.dims, 0x5eed, tokenMean(doc, s.dims), 1, 1)
		if err != nil {
			b.Fatalf("NewRQ1Params: %v", err)
		}
		rqBlob, err := EncodeRQ1(doc, rp)
		if err != nil {
			b.Fatalf("EncodeRQ1: %v", err)
		}
		rqParsed, err := Parse(rqBlob)
		if err != nil {
			b.Fatalf("Parse: %v", err)
		}
		i8, err := NewRQ1LUTInt8Scorer(query, rp)
		if err != nil {
			b.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
		}
		fsBlob, err := EncodeRQ1Block16(doc, rp)
		if err != nil {
			b.Fatalf("EncodeRQ1Block16: %v", err)
		}
		fsParsed, err := Parse(fsBlob)
		if err != nil {
			b.Fatalf("Parse: %v", err)
		}
		fs, err := NewRQ1LUTFastScanScorer(query, rp)
		if err != nil {
			b.Fatalf("NewRQ1LUTFastScanScorer: %v", err)
		}

		// check that the fast scan agrees with its reference before timing it
		i8Want, err := i8.Distance(rqParsed)
		if err != nil {
			b.Fatalf("RQ1LUTInt8Scorer.Distance: %v", err)
		}
		fsGot, err := fs.Distance(fsParsed)
		if err != nil {
			b.Fatalf("RQ1LUTFastScanScorer.Distance: %v", err)
		}
		if fsGot != i8Want {
			b.Fatalf("RQ1LUTFastScanScorer.Distance = %v, RQ1LUTInt8Scorer = %v", fsGot, i8Want)
		}

		pairs := float64(s.nq * s.nd)
		perPair := func(b *testing.B) {
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/pairs, "ns/pair")
		}
		run := func(b *testing.B, name string, fn func() (float32, error)) {
			b.Run(name, func(b *testing.B) {
				for range b.N {
					v, err := fn()
					if err != nil {
						b.Fatal(err)
					}
					benchSink = v
				}
				perPair(b)
			})
		}

		b.Run(fmt.Sprintf("%dx%dx%d", s.nq, s.nd, s.dims), func(b *testing.B) {
			run(b, "exact-f32-shipped", func() (float32, error) {
				return referenceProduction(query, doc)
			})
			run(b, "rq1-lut-int8-reference", func() (float32, error) {
				return i8.Distance(rqParsed)
			})
			run(b, "rq1-lut-int8-fastscan", func() (float32, error) {
				return fs.Distance(fsParsed)
			})
		})
	}
}
