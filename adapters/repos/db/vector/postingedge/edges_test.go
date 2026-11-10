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
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// edgeFixture is a small corpus taken through every build stage up to and
// including the edges.
type edgeFixture struct {
	vectors   [][]float32
	provider  distancer.Provider
	assign    *assignment
	layout    slotLayout
	side      *sidecar
	edgeStart []uint32
	edges     []uint32
	scored    int64
}

func (f *edgeFixture) edgesOf(slot uint32) []uint32 {
	return f.edges[f.edgeStart[slot]:f.edgeStart[slot+1]]
}

func newEdgeFixture(t *testing.T, vectors [][]float32, provider distancer.Provider,
	postings, probe, neighbors, rescue int,
) *edgeFixture {
	t.Helper()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	dims := len(vectors[0])
	cfg := testConfig(t, vectors, 0, provider)

	centroids, err := partition(ctx, logger, vectors, dims, postings, 3)
	require.NoError(t, err)
	assigned, err := assign(ctx, logger, vectors, centroids, probe)
	require.NoError(t, err)
	centroids = dropEmpty(centroids, assigned)
	layout := newSlotLayout(assigned.primary, len(centroids))

	quantizer, err := trainQuantizer(vectors, dims, 1000, &cfg)
	require.NoError(t, err)
	codes, codeWords, err := encode(ctx, logger, vectors, layout, quantizer)
	require.NoError(t, err)
	side := &sidecar{codeWords: codeWords, codes: codes, postingOf: postingOfSlots(layout.start)}

	edgeStart, edges, scored, err := buildEdges(ctx, logger, edgeInput{
		vectors:   vectors,
		layout:    layout,
		assign:    assigned,
		side:      side,
		quantizer: quantizer,
		provider:  provider,
		neighbors: neighbors,
		rescue:    rescue,
	})
	require.NoError(t, err)

	return &edgeFixture{
		vectors: vectors, provider: provider, assign: assigned, layout: layout, side: side,
		edgeStart: edgeStart, edges: edges, scored: scored,
	}
}

// wantEdges is the definition of the edges of a vector: of its neighbors
// nearest exact neighbors, the ones in other postings, nearest first.
func (f *edgeFixture) wantEdges(t *testing.T, input, neighbors int) []uint32 {
	t.Helper()
	want := []uint32{}
	for _, nb := range exactNearest(t, f.vectors, f.vectors[input], neighbors, input, f.provider) {
		if f.assign.primary[nb.id] != f.assign.primary[input] {
			want = append(want, f.layout.slotOf[nb.id])
		}
	}
	return want
}

// With every posting probed and every candidate rescued, nothing is
// approximate: the edges must be exactly the cross-posting members of the
// exact nearest neighbors.
func TestBuildEdgesMatchesTheExactNeighborsUnderFullCoverage(t *testing.T) {
	tests := []struct {
		name     string
		provider distancer.Provider
		vectors  [][]float32
	}{
		{
			name:     "l2",
			provider: distancer.NewL2SquaredProvider(),
			vectors:  clusteredVectors(400, 24, 9, 31),
		},
		{
			name:     "cosine",
			provider: distancer.NewCosineDistanceProvider(),
			vectors:  normalizedCopy(clusteredVectors(400, 24, 9, 32)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const neighbors = 8
			n := len(tt.vectors)
			f := newEdgeFixture(t, tt.vectors, tt.provider, 12, 1000, neighbors, n)

			crossPosting := 0
			for input := range tt.vectors {
				want := f.wantEdges(t, input, neighbors)
				got := f.edgesOf(f.layout.slotOf[input])
				require.Equalf(t, want, append([]uint32{}, got...), "edges of input %d", input)
				crossPosting += len(want)
			}
			require.Positive(t, crossPosting, "the fixture must have neighbors across postings")
			assert.Equal(t, int64(n)*int64(n), f.scored,
				"full coverage ranks every vector for every vector")
		})
	}
}

// Whatever the coverage, the structural invariants of the edges hold.
func TestBuildEdgesInvariants(t *testing.T) {
	const neighbors = 8
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(1500, 24, 25, 41)
	// 30 postings, 6 probed, 24 rescued: every stage approximates.
	f := newEdgeFixture(t, vectors, provider, 30, 6, neighbors, 24)
	n := uint32(len(vectors))

	require.Len(t, f.edgeStart, len(vectors)+1)
	require.Equal(t, uint32(0), f.edgeStart[0])
	require.Equal(t, uint32(len(f.edges)), f.edgeStart[n])

	captured, wanted := 0, 0
	for slot := uint32(0); slot < n; slot++ {
		require.LessOrEqual(t, f.edgeStart[slot], f.edgeStart[slot+1], "offsets must not decrease")
		edges := f.edgesOf(slot)
		require.LessOrEqual(t, len(edges), neighbors)

		source := f.vectors[f.layout.order[slot]]
		seen := map[uint32]struct{}{}
		previous := float32(-1)
		for _, endpoint := range edges {
			require.Less(t, endpoint, n)
			require.NotEqual(t, slot, endpoint, "no self edges")
			require.NotEqualf(t, f.side.postingOf[slot], f.side.postingOf[endpoint],
				"an edge must leave the posting (slot %d)", slot)
			_, duplicate := seen[endpoint]
			require.Falsef(t, duplicate, "duplicate endpoint %d for slot %d", endpoint, slot)
			seen[endpoint] = struct{}{}

			dist, err := provider.SingleDist(source, f.vectors[f.layout.order[endpoint]])
			require.NoError(t, err)
			require.GreaterOrEqual(t, dist, previous, "edges are ordered nearest first")
			previous = dist
		}

		for _, w := range f.wantEdges(t, int(f.layout.order[slot]), neighbors) {
			wanted++
			if _, ok := seen[w]; ok {
				captured++
			}
		}
	}

	// Coverage and rescue are deliberately tight here, yet most true
	// cross-posting neighbors must still be found: this is the quality the
	// search cost depends on.
	recall := float64(captured) / float64(wanted)
	assert.Greaterf(t, recall, 0.85, "edge recall %.3f", recall)
	assert.Less(t, f.scored, int64(len(vectors))*int64(len(vectors))/2,
		"a narrow probe must rank far fewer candidates than brute force")
}

func TestBuildEdgesWithASinglePostingHasNoEdges(t *testing.T) {
	vectors := clusteredVectors(50, 8, 2, 51)
	f := newEdgeFixture(t, vectors, distancer.NewL2SquaredProvider(), 1, 4, 8, 16)
	assert.Empty(t, f.edges)
	for _, offset := range f.edgeStart {
		assert.Zero(t, offset)
	}
}
