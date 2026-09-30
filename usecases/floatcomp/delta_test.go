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

package floatcomp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithinCutoff(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dist   float32
		cutoff float64
		cosine bool
		want   bool
	}{
		{name: "below", dist: 0.3, cutoff: 0.5, want: true},
		{name: "above", dist: 0.7, cutoff: 0.5, want: false},
		{name: "exact float32 distance fed back as float64", dist: 0.1, cutoff: float64(float32(0.1)), want: true},
		{name: "float32 rounding above a decimal cutoff", dist: float32(0.1), cutoff: 0.1, want: true},
		{name: "just above tolerance", dist: 0.5001, cutoff: 0.5, want: false},
		{name: "zero cutoff, zero distance", dist: 0, cutoff: 0, want: true},
		{name: "zero cutoff, positive distance", dist: 1e-7, cutoff: 0, want: false},
		// gh-13315: tiny cutoffs used to accept everything within 1e-6
		{name: "tiny cutoff, larger tiny distance", dist: 5e-7, cutoff: 1e-7, want: false},
		{name: "tiny cutoff, smaller tiny distance", dist: 5e-8, cutoff: 1e-7, want: true},
		{name: "subnormal cutoff, larger subnormal distance", dist: 9e-40, cutoff: 5e-40, want: false},
		{name: "subnormal cutoff, smaller subnormal distance", dist: 1e-40, cutoff: 5e-40, want: true},
		// dot product distances are negative
		{name: "negative cutoff, below", dist: -6, cutoff: -5, want: true},
		{name: "negative cutoff, above", dist: -4, cutoff: -5, want: false},
		{name: "negative cutoff, float32 rounding", dist: float32(-5.1), cutoff: -5.1, want: true},
		// cosine keeps an absolute floor: exact duplicates can compute as ~6e-8
		{name: "cosine, zero cutoff, duplicate with rounding noise", dist: 5.96e-8, cutoff: 0, cosine: true, want: true},
		{name: "cosine, tiny cutoff, noise-level distance", dist: 5e-7, cutoff: 1e-7, cosine: true, want: true},
		{name: "cosine, zero cutoff, real distance", dist: 1e-5, cutoff: 0, cosine: true, want: false},
		{name: "cosine, regular cutoff", dist: 0.5001, cutoff: 0.5, cosine: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, WithinCutoff(tc.dist, tc.cutoff, tc.cosine))
		})
	}
}
