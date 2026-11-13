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

package postingedge

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// One edge per target posting, the prototype of v1.6 (bench-report.md): of
// the edges of a vector into one posting, the nearest only. A search scores
// an endpoint only to rank the posting it lives in, so the others reach no
// posting the nearest does not; they can only lower the rank of one it
// reaches. It is measured as a filter over a build (onePerTarget, then
// replaceEdges), and is not a parameter of the index.

// onePerTarget keeps, of every row of edges, the nearest edge into each of its
// target postings. The rows of buildEdges are nearest first, so that is the
// first edge of the row into the posting, and no distance is computed. The
// result is new arrays in the same CSR form, and the given ones are left as
// they are.
func onePerTarget(edgeStart, edges, postingOf []uint32) (keptStart, kept []uint32) {
	if len(edgeStart) == 0 {
		return nil, nil
	}
	n := len(edgeStart) - 1
	// Two passes, the first to size the result: at 10M a result grown by
	// appends would hold several times the edges it keeps.
	keptStart = make([]uint32, n+1)
	var marks []bool
	for slot := 0; slot < n; slot++ {
		marks = redundantEdges(edges[edgeStart[slot]:edgeStart[slot+1]], postingOf, marks)
		count := uint32(0)
		for _, redundant := range marks {
			if !redundant {
				count++
			}
		}
		keptStart[slot+1] = keptStart[slot] + count
	}
	kept = make([]uint32, 0, keptStart[n])
	for slot := 0; slot < n; slot++ {
		row := edges[edgeStart[slot]:edgeStart[slot+1]]
		marks = redundantEdges(row, postingOf, marks)
		for j, endpoint := range row {
			if !marks[j] {
				kept = append(kept, endpoint)
			}
		}
	}
	return keptStart, kept
}

func TestOnePerTarget(t *testing.T) {
	// Ten slots in four postings.
	postingOf := []uint32{0, 0, 1, 1, 1, 2, 2, 3, 3, 3}

	tests := []struct {
		name string
		rows [][]uint32
		want [][]uint32
	}{
		{
			name: "rows into distinct postings stay whole",
			rows: [][]uint32{{2, 5, 7}, {0}},
			want: [][]uint32{{2, 5, 7}, {0}},
		},
		{
			name: "the nearest edge into each posting stays",
			rows: [][]uint32{{2, 3, 5, 4, 6}},
			want: [][]uint32{{2, 5}},
		},
		{
			name: "every edge into one posting",
			rows: [][]uint32{{7, 8, 9}},
			want: [][]uint32{{7}},
		},
		{
			name: "a posting first reached late keeps its first edge",
			rows: [][]uint32{{3, 2, 7, 4, 8, 5}},
			want: [][]uint32{{3, 7, 5}},
		},
		{
			name: "rows without edges",
			rows: [][]uint32{{}, {2, 4}, {}},
			want: [][]uint32{{}, {2}, {}},
		},
		{
			name: "every row on its own",
			rows: [][]uint32{{2, 3}, {3, 2}, {9, 7, 5}},
			want: [][]uint32{{2}, {3}, {9, 5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			edgeStart, edges := csrOf(tt.rows)
			startBefore, edgesBefore := slices.Clone(edgeStart), slices.Clone(edges)

			keptStart, kept := onePerTarget(edgeStart, edges, postingOf)
			require.Len(t, keptStart, len(tt.rows)+1)
			require.Equal(t, uint32(len(kept)), keptStart[len(tt.rows)])
			assert.Equal(t, len(kept), cap(kept), "the result is sized, not grown")
			for slot, want := range tt.want {
				assert.Equalf(t, want, kept[keptStart[slot]:keptStart[slot+1]], "row %d", slot)
			}
			assert.Equal(t, startBefore, edgeStart, "the given offsets are left as they are")
			assert.Equal(t, edgesBefore, edges, "the given edges are left as they are")
			if len(kept) > 0 {
				kept[0] = 99
				assert.Equal(t, edgesBefore, edges, "the result is a copy")
			}
		})
	}

	t.Run("no vectors", func(t *testing.T) {
		keptStart, kept := onePerTarget(nil, nil, postingOf)
		assert.Empty(t, keptStart)
		assert.Empty(t, kept)
	})
}

// csrOf lays rows of edges out as buildEdges returns them.
func csrOf(rows [][]uint32) (edgeStart, edges []uint32) {
	edgeStart = make([]uint32, len(rows)+1)
	edges = []uint32{}
	for i, row := range rows {
		edges = append(edges, row...)
		edgeStart[i+1] = uint32(len(edges))
	}
	return edgeStart, edges
}

// On a real build, every vector keeps one edge into each of its target
// postings, the one to the endpoint nearest to it by exact distance, and the
// edges it keeps stay in the order they had.
func TestOnePerTargetOfABuild(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	f := newEdgeFixture(t, clusteredVectors(1500, 24, 25, 43), provider, 30, 6, 16, 32)
	postingOf := f.side.postingOf
	n := len(f.edgeStart) - 1

	keptStart, kept := onePerTarget(f.edgeStart, f.edges, postingOf)
	require.Len(t, keptStart, n+1)
	require.Equal(t, uint32(len(kept)), keptStart[n])

	full := edgeRedundancyOf(f.edgeStart, f.edges, postingOf)
	require.Positive(t, full.Shared, "the fixture must have vectors with several edges into one posting")
	one := edgeRedundancyOf(keptStart, kept, postingOf)
	assert.Equal(t, full.Targets, one.Targets, "no vector loses a target posting")
	assert.Zero(t, one.Shared, "no two kept edges of a vector share a target posting")
	assert.Equal(t, full.Targets, len(kept))

	for slot := uint32(0); slot < uint32(n); slot++ {
		row := f.edgesOf(slot)
		got := kept[keptStart[slot]:keptStart[slot+1]]
		source := f.vectors[f.layout.order[slot]]
		dist := func(endpoint uint32) float32 {
			d, err := provider.SingleDist(source, f.vectors[f.layout.order[endpoint]])
			require.NoError(t, err)
			return d
		}

		// The nearest endpoint of the row in each target posting, and the
		// order in which the row first reaches each posting.
		nearest := map[uint32]uint32{}
		var order []uint32
		for _, endpoint := range row {
			target := postingOf[endpoint]
			best, seen := nearest[target]
			if !seen {
				order = append(order, target)
			}
			if !seen || dist(endpoint) < dist(best) {
				nearest[target] = endpoint
			}
		}
		want := make([]uint32, 0, len(order))
		for _, target := range order {
			want = append(want, nearest[target])
		}
		require.Equalf(t, want, got, "slot %d", slot)
	}
}
