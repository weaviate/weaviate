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

// Scorer is one query prepared for one encoding, scoring packed documents
// against it.
//
// A Scorer holds the query in prepared form (rotated codes, lookup tables), so
// the preparation is paid once per query, however many candidates are scored.
//
// A Scorer is bound to one query and is not safe for concurrent use: scorers
// keep per-document scratch (widened scalars, lane maxima, reconstructed
// codes). Callers scoring in parallel need one Scorer each.
type Scorer interface {
	// Distance returns the negated MaxSim of the query against b: the sum
	// over query tokens of the smallest distance to any document token.
	//
	// Negation is the convention in the vector layer: the dot product
	// distancer returns -<a,b>, so a smaller distance is a better match.
	// Scorers that estimate the dot product take the maximum over the
	// document's tokens first and negate once.
	//
	// It returns an error when b is not a blob this Scorer was prepared for.
	// The blob describes itself, and that mismatch is what the header exists
	// to catch.
	Distance(b Blob) (float32, error)
}
