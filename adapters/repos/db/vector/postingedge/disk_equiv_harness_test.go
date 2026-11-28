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

import "testing"

// Every cell searched from disk returns what its equivalent in memory
// returns: the same ids in the same order, the same distances and the same
// work. And what it read is what its postings hold. The whole matrix, over
// 3 000 vectors of 64 dimensions in postings of the size of smallConfig, and
// 40 queries: TestDiskSearchEqualsMemory checks one cell of each kind in the
// package, over a few hundred.
func TestDiskSearchEqualsMemoryAtScale(t *testing.T) {
	checkDiskSearchEqualsMemory(t, 3000, 64, smallConfig().TargetPostingSize, 40, []diskEquivCell{
		{name: "the full code in memory"},
		{name: "the full code in memory, cosine", cosine: true},
		{name: "a sketch in memory", bits: 64},
		{name: "a sketch in memory, cosine", bits: 128, cosine: true},
		{name: "a halo", bits: 64, halo: true},
		{name: "a halo, cosine", bits: 128, halo: true, cosine: true},
		{name: "a halo over pruned edges", bits: 64, halo: true, onePer: true, degree: 4},
		{name: "a halo of full codes", halo: true},
		{name: "pruned edges, the full code in memory", onePer: true},
		{name: "the scan by a sketch in memory", bits: 64, scan: true},
		{name: "the scan by a sketch in memory, cosine", bits: 128, scan: true, cosine: true},
		{name: "the scan by a wider sketch in memory", bits: 192, scan: true},
		{name: "the scan by a sketch in memory over pruned edges", bits: 64, scan: true, onePer: true, degree: 4},
	})
}
