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

package compressionhelpers_test

import (
	"math"
	"sort"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/datasets"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// Diagnostic: estimator error tails of rq4 vs rq4r on real dbpedia vectors.
func TestRQ4RDiagDBPedia(t *testing.T) {
	hf := datasets.NewHubDataset("weaviate/ann-datasets", "dbpedia-openai-ada002-1536-angular-20k")
	_, vectors, err := hf.LoadTrainData()
	if err != nil {
		t.Skipf("dataset unavailable: %v", err)
	}
	_, queries, err := hf.LoadTestData()
	if err != nil {
		t.Skipf("dataset unavailable: %v", err)
	}
	normalizeVectors(vectors)
	normalizeVectors(queries)
	d := len(vectors[0])
	m := distancer.NewCosineDistanceProvider()
	base := compressionhelpers.NewFourBitRotationalQuantizer(d, 42, m)
	residual := compressionhelpers.NewFourBitResidualQuantizer(d, 42, m)

	nQ, nV := 20, 2000
	var baseErrs, resErrs []float64
	for qi := range nQ {
		q := queries[qi]
		bd := base.NewDistancer(q)
		rd := residual.NewDistancer(q)
		for vi := range nV {
			x := vectors[vi]
			target, _ := m.SingleDist(q, x)
			be, _ := bd.Distance(base.Encode(x))
			re, _ := rd.Distance(residual.Encode(x))
			baseErrs = append(baseErrs, math.Abs(float64(be-target)))
			resErrs = append(resErrs, math.Abs(float64(re-target)))
		}
	}
	report := func(name string, errs []float64) {
		s := append([]float64(nil), errs...)
		sort.Float64s(s)
		var sum float64
		for _, e := range s {
			sum += e
		}
		t.Logf("%s: mean=%.5f p50=%.5f p99=%.5f max=%.5f",
			name, sum/float64(len(s)), s[len(s)/2], s[len(s)*99/100], s[len(s)-1])
	}
	report("base    ", baseErrs)
	report("residual", resErrs)
}

// Diagnostic: estimator error of rq4 vs rq4r by query/data correlation.
func TestRQ4RDiagCorrelated(t *testing.T) {
	rng := newRNG(1)
	d := 1536
	n := 200
	seed := rng.Uint64()
	base := compressionhelpers.NewFourBitRotationalQuantizer(d, seed, distancer.NewDotProductProvider())
	residual := compressionhelpers.NewFourBitResidualQuantizer(d, seed, distancer.NewDotProductProvider())

	for _, alpha := range []float32{0.0, 0.5, 0.8, 0.9, 0.95} {
		var baseErr, resErr, baseBias, resBias float64
		for range n {
			// Random rotation of the correlated pair so every trial uses
			// different coordinates.
			q0, x0 := correlatedVectors(d, alpha)
			perm := rng.Perm(d)
			q, x := make([]float32, d), make([]float32, d)
			for i, p := range perm {
				s := float32(1)
				if rng.IntN(2) == 0 {
					s = -1
				}
				q[p], x[p] = s*q0[i], s*x0[i]
			}
			trueDot := float64(dotFloat(q, x))

			be, _ := base.NewDistancer(q).Distance(base.Encode(x))
			re, _ := residual.NewDistancer(q).Distance(residual.Encode(x))
			baseErr += math.Abs(-float64(be) - trueDot)
			resErr += math.Abs(-float64(re) - trueDot)
			baseBias += -float64(be) - trueDot
			resBias += -float64(re) - trueDot
		}
		t.Logf("alpha=%.2f base err=%.5f bias=%+.5f | residual err=%.5f bias=%+.5f",
			alpha, baseErr/float64(n), baseBias/float64(n), resErr/float64(n), resBias/float64(n))
	}
}
