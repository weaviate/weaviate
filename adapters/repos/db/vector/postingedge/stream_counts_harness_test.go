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

//go:build postingedgebench

package postingedge

// What the counts of the edges of a stream tell, for the measures of the
// bench harness: the share of the wanted postings the edges reach, and the
// edges per vector.

func (c edgeCounts) postingRecall() float64 {
	return float64(c.PostingsCaptured) / float64(max(c.PostingsWanted, 1))
}

func (c edgeCounts) degree() float64 {
	return float64(c.Edges) / float64(max(c.Vectors, 1))
}
