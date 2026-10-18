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

import "math"

// WithinCutoff reports whether dist is at or below a user-supplied distance
// cutoff, tolerating float32 rounding (e.g. a distance copied back from a
// result). Cosine distances are 1-dot of unit vectors, so their error is
// absolute (an exact duplicate can compute as ~6e-8) and keeps a 1e-6 floor.
// For every other metric the error scales with the values, so the tolerance
// scales with the cutoff: an absolute one accepted every distance below it,
// so cutoffs smaller than ~1e-6 filtered nothing.
func WithinCutoff(dist float32, cutoff float64, cosine bool) bool {
	tolerance := math.Abs(cutoff) * 1e-6
	if cosine {
		tolerance = max(tolerance, 1e-6)
	}
	return float64(dist) <= cutoff+tolerance
}
