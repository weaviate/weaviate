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

package compressionhelpers

import (
	"math"
	"math/rand/v2"
	"testing"
)

// selRef is a deliberately obvious top-2-by-magnitude, against which the
// optimized bit-pattern selection is differentially tested. Ties break
// toward the lower index and NaN is never selected.
func selRef(rx []float32) (int, int) {
	type e struct {
		i int
		m float64
	}
	best := []e{}
	for i, v := range rx {
		m := math.Abs(float64(v))
		if math.IsNaN(m) {
			m = -1 // never selected
		}
		best = append(best, e{i, m})
	}
	p0, p1 := -1, -1
	for _, c := range best {
		if c.m < 0 {
			continue
		}
		if p0 == -1 || c.m > best[p0].m {
			p1 = p0
			p0 = c.i
		} else if p1 == -1 || c.m > best[p1].m {
			p1 = c.i
		}
	}
	return p0, p1
}

func TestRQ4SelectOutliersMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 9))
	for trial := 0; trial < 20000; trial++ {
		n := 64
		v := make([]float32, n)
		mode := trial % 6
		for i := range v {
			switch mode {
			case 0:
				v[i] = float32(rng.NormFloat64())
			case 1:
				v[i] = 0
			case 2:
				v[i] = float32(rng.IntN(3) - 1) // many ties
			case 3:
				if rng.IntN(10) == 0 {
					v[i] = float32(rng.NormFloat64())
				}
			case 4:
				v[i] = float32(math.Inf(1-2*rng.IntN(2))) * float32(rng.IntN(2))
			case 5:
				if rng.IntN(5) == 0 {
					v[i] = float32(math.NaN())
				} else {
					v[i] = float32(rng.NormFloat64())
				}
			}
		}
		// single nonzero at index 0 — the collision edge case
		if trial%97 == 0 {
			for i := range v {
				v[i] = 0
			}
			v[0] = 5
		}
		cp := append([]float32(nil), v...)
		p0, p1, v0, v1 := rq4SelectOutliers(cp)
		if p0 == p1 {
			t.Fatalf("trial %d mode %d: positions collided at %d (input head %v)", trial, mode, p0, v[:4])
		}
		if v0 != v[p0] || v1 != v[p1] {
			if !(math.IsNaN(float64(v0)) && math.IsNaN(float64(v[p0]))) {
				t.Fatalf("trial %d: values %v/%v != %v/%v", trial, v0, v1, v[p0], v[p1])
			}
		}
		if cp[p0] != 0 || cp[p1] != 0 {
			t.Fatalf("trial %d: outliers not zeroed", trial)
		}
		if math.IsNaN(float64(v0)) || math.IsNaN(float64(v1)) {
			allNaN := true
			for _, x := range v {
				if !math.IsNaN(float64(x)) {
					allNaN = false
				}
			}
			if !allNaN {
				t.Fatalf("trial %d: NaN selected at %d/%d", trial, p0, p1)
			}
		}
		r0, r1 := selRef(v)
		// reference agrees whenever the top-2 magnitudes are unambiguous
		if r0 != -1 && math.Abs(float64(v[r0])) != math.Abs(float64(v[r1])) {
			if p0 != r0 {
				t.Fatalf("trial %d mode %d: p0=%d want %d (|v|=%v vs %v)", trial, mode, p0, r0, math.Abs(float64(v[p0])), math.Abs(float64(v[r0])))
			}
		}
	}
}
