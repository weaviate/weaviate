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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// edgeRedundancy tells how the edges of the vectors spread over the postings
// they lead to. A search scores an endpoint only to rank the posting it lives
// in, by the nearest endpoint that leads there, so an edge whose target
// posting a nearer edge of the same vector already leads to adds an endpoint
// to score and no posting to rank. Such an edge is shared here.
type edgeRedundancy struct {
	Vectors int `json:"vectors"`
	Edges   int `json:"edges"`
	// Targets is the distinct target postings of each vector, summed.
	Targets int `json:"targets"`
	// Shared is the edges whose target posting a nearer edge of the same
	// vector leads to: Edges - Targets.
	Shared int `json:"shared"`
	// VectorsWithShared is the vectors with at least one shared edge.
	VectorsWithShared int `json:"vectorsWithShared"`
	// ByRank[r] is about the edges of rank r of their vector, 0 the nearest.
	ByRank []edgeRankRow `json:"byRank"`
	// ByDegree[d] is about the vectors with d edges.
	ByDegree []edgeDegreeRow `json:"byDegree"`
	// TargetsHist[n] is the vectors whose edges lead to n distinct postings.
	TargetsHist []int `json:"targetsHist"`
	// SharedHist[n] is the vectors with n shared edges.
	SharedHist []int `json:"sharedHist"`
	// LargestHist[n] is the vectors whose most targeted posting takes n of
	// their edges.
	LargestHist []int `json:"largestHist"`
}

type edgeRankRow struct {
	Edges  int `json:"edges"`
	Shared int `json:"shared"`
}

type edgeDegreeRow struct {
	Vectors int `json:"vectors"`
	Targets int `json:"targets"`
}

// edgeRedundancyOf reads the edges in the form of the posting store (rows
// nearest first) against the posting of every slot.
func edgeRedundancyOf(edgeStart, edges, postingOf []uint32) edgeRedundancy {
	r := edgeRedundancy{Edges: len(edges)}
	if len(edgeStart) == 0 {
		return r
	}
	r.Vectors = len(edgeStart) - 1
	count := func(hist []int, n int) []int {
		for len(hist) <= n {
			hist = append(hist, 0)
		}
		hist[n]++
		return hist
	}
	var targets []uint32 // the distinct target postings of a vector, in order
	var taken []int      // how many of its edges each of them takes
	for slot := 0; slot < r.Vectors; slot++ {
		row := edges[edgeStart[slot]:edgeStart[slot+1]]
		targets, taken = targets[:0], taken[:0]
		for rank, endpoint := range row {
			for len(r.ByRank) <= rank {
				r.ByRank = append(r.ByRank, edgeRankRow{})
			}
			r.ByRank[rank].Edges++
			target := postingOf[endpoint]
			if at := slices.Index(targets, target); at >= 0 {
				taken[at]++
				r.ByRank[rank].Shared++
				continue
			}
			targets = append(targets, target)
			taken = append(taken, 1)
		}
		shared := len(row) - len(targets)
		r.Targets += len(targets)
		r.Shared += shared
		if shared > 0 {
			r.VectorsWithShared++
		}
		for len(r.ByDegree) <= len(row) {
			r.ByDegree = append(r.ByDegree, edgeDegreeRow{})
		}
		r.ByDegree[len(row)].Vectors++
		r.ByDegree[len(row)].Targets += len(targets)
		r.TargetsHist = count(r.TargetsHist, len(targets))
		r.SharedHist = count(r.SharedHist, shared)
		largest := 0
		for _, n := range taken {
			largest = max(largest, n)
		}
		r.LargestHist = count(r.LargestHist, largest)
	}
	return r
}

// diagnostics are the numbers of the redundancy that go with the other
// diagnostics of a build.
func (r *edgeRedundancy) diagnostics() map[string]float64 {
	if r.Vectors == 0 {
		return map[string]float64{}
	}
	out := map[string]float64{
		"edgeTargetsPerVector":      float64(r.Targets) / float64(r.Vectors),
		"vectorsWithSharedTarget":   float64(r.VectorsWithShared) / float64(r.Vectors),
		"edgeTargetsP50":            float64(histPercentile(r.TargetsHist, 0.50)),
		"edgeTargetsP95":            float64(histPercentile(r.TargetsHist, 0.95)),
		"edgesSharingTargetMean":    float64(r.Shared) / float64(r.Vectors),
		"edgesSharingTargetP95":     float64(histPercentile(r.SharedHist, 0.95)),
		"edgesSharingTargetShare":   0,
		"edgesIntoLargestTargetP95": float64(histPercentile(r.LargestHist, 0.95)),
	}
	if r.Edges > 0 {
		out["edgesSharingTargetShare"] = float64(r.Shared) / float64(r.Edges)
	}
	return out
}

// histPercentile is the smallest n such that a share q of the counted
// vectors have at most n.
func histPercentile(hist []int, q float64) int {
	total := 0
	for _, c := range hist {
		total += c
	}
	if total == 0 {
		return 0
	}
	seen := 0
	for n, c := range hist {
		seen += c
		if float64(seen) >= q*float64(total) {
			return n
		}
	}
	return len(hist) - 1
}

// report renders the redundancy as Markdown, for the log and the report.
func (r *edgeRedundancy) report() string {
	var b strings.Builder
	share := func(part, whole int) float64 {
		if whole == 0 {
			return 0
		}
		return 100 * float64(part) / float64(whole)
	}
	fmt.Fprintf(&b, "%d vectors, %d edges (%.2f per vector) into %d distinct target postings (%.2f per vector); "+
		"%d edges (%.1f%%) share their target posting with a nearer edge of the same vector, in %d vectors (%.1f%%)\n\n",
		r.Vectors, r.Edges, float64(r.Edges)/float64(max(r.Vectors, 1)), r.Targets, float64(r.Targets)/float64(max(r.Vectors, 1)),
		r.Shared, share(r.Shared, r.Edges), r.VectorsWithShared, share(r.VectorsWithShared, r.Vectors))
	b.WriteString("| edges of the vector | vectors | share | distinct target postings, mean |\n|---|---|---|---|\n")
	for d, row := range r.ByDegree {
		if row.Vectors == 0 {
			continue
		}
		fmt.Fprintf(&b, "| %d | %d | %.1f%% | %.2f |\n", d, row.Vectors, share(row.Vectors, r.Vectors),
			float64(row.Targets)/float64(row.Vectors))
	}
	b.WriteString("\n| rank of the edge | edges | shared | shared, share |\n|---|---|---|---|\n")
	for rank, row := range r.ByRank {
		fmt.Fprintf(&b, "| %d | %d | %d | %.1f%% |\n", rank+1, row.Edges, row.Shared, share(row.Shared, row.Edges))
	}
	b.WriteString("\n| n | vectors with n distinct target postings | with n shared edges | whose most targeted posting takes n edges |\n|---|---|---|---|\n")
	rows := max(len(r.TargetsHist), len(r.SharedHist), len(r.LargestHist))
	at := func(hist []int, n int) int {
		if n < len(hist) {
			return hist[n]
		}
		return 0
	}
	for n := 0; n < rows; n++ {
		fmt.Fprintf(&b, "| %d | %d (%.1f%%) | %d (%.1f%%) | %d (%.1f%%) |\n", n,
			at(r.TargetsHist, n), share(at(r.TargetsHist, n), r.Vectors),
			at(r.SharedHist, n), share(at(r.SharedHist, n), r.Vectors),
			at(r.LargestHist, n), share(at(r.LargestHist, n), r.Vectors))
	}
	return b.String()
}

func TestEdgeRedundancyOf(t *testing.T) {
	// Slots 0..7 live in postings 0,0,1,1,2,2,3,3.
	postingOf := []uint32{0, 0, 1, 1, 2, 2, 3, 3}
	tests := []struct {
		name      string
		edgeStart []uint32
		edges     []uint32
		want      edgeRedundancy
	}{
		{
			name: "no index",
			want: edgeRedundancy{},
		},
		{
			name:      "vectors without edges",
			edgeStart: []uint32{0, 0, 0},
			want: edgeRedundancy{
				Vectors: 2, ByDegree: []edgeDegreeRow{{Vectors: 2}},
				TargetsHist: []int{2}, SharedHist: []int{2}, LargestHist: []int{2},
			},
		},
		{
			name: "every edge into a posting of its own",
			// slot 0 -> slots 2, 4, 6 (postings 1, 2, 3)
			edgeStart: []uint32{0, 3},
			edges:     []uint32{2, 4, 6},
			want: edgeRedundancy{
				Vectors: 1, Edges: 3, Targets: 3,
				ByRank:      []edgeRankRow{{Edges: 1}, {Edges: 1}, {Edges: 1}},
				ByDegree:    []edgeDegreeRow{{}, {}, {}, {Vectors: 1, Targets: 3}},
				TargetsHist: []int{0, 0, 0, 1}, SharedHist: []int{1}, LargestHist: []int{0, 1},
			},
		},
		{
			name: "a nearer edge already leads to the posting",
			// slot 0 -> slots 2, 3, 4 (postings 1, 1, 2): the second is shared.
			// slot 1 -> slots 6, 4, 7, 5 (postings 3, 2, 3, 2): the third and
			// the fourth are shared, two postings take two edges each.
			// slot 2 -> nothing.
			edgeStart: []uint32{0, 3, 7, 7},
			edges:     []uint32{2, 3, 4, 6, 4, 7, 5},
			want: edgeRedundancy{
				Vectors: 3, Edges: 7, Targets: 4, Shared: 3, VectorsWithShared: 2,
				ByRank:      []edgeRankRow{{Edges: 2}, {Edges: 2, Shared: 1}, {Edges: 2, Shared: 1}, {Edges: 1, Shared: 1}},
				ByDegree:    []edgeDegreeRow{{Vectors: 1}, {}, {}, {Vectors: 1, Targets: 2}, {Vectors: 1, Targets: 2}},
				TargetsHist: []int{1, 0, 2}, SharedHist: []int{1, 1, 1}, LargestHist: []int{1, 0, 2},
			},
		},
		{
			name: "every edge into one posting",
			// slot 4 -> slots 0, 1 (posting 0), slot 5 -> slot 0
			edgeStart: []uint32{0, 0, 0, 0, 0, 2, 3},
			edges:     []uint32{0, 1, 0},
			want: edgeRedundancy{
				Vectors: 6, Edges: 3, Targets: 2, Shared: 1, VectorsWithShared: 1,
				ByRank:      []edgeRankRow{{Edges: 2}, {Edges: 1, Shared: 1}},
				ByDegree:    []edgeDegreeRow{{Vectors: 4}, {Vectors: 1, Targets: 1}, {Vectors: 1, Targets: 1}},
				TargetsHist: []int{4, 2}, SharedHist: []int{5, 1}, LargestHist: []int{4, 1, 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := edgeRedundancyOf(tt.edgeStart, tt.edges, postingOf)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, got.Edges-got.Targets, got.Shared)
			report := got.report()
			assert.NotEmpty(t, report)
		})
	}
}

func TestEdgeRedundancyDiagnostics(t *testing.T) {
	// Postings 0,0,1,1,2,2,3,3. Slot 0 -> postings 1, 1, 2; slot 1 -> 3, 2,
	// 3, 2; slot 2 -> nothing: 7 edges, 4 distinct targets, 3 shared.
	r := edgeRedundancyOf([]uint32{0, 3, 7, 7}, []uint32{2, 3, 4, 6, 4, 7, 5}, []uint32{0, 0, 1, 1, 2, 2, 3, 3})
	got := r.diagnostics()
	assert.InDelta(t, 4.0/3, got["edgeTargetsPerVector"], 1e-12)
	assert.InDelta(t, 2.0/3, got["vectorsWithSharedTarget"], 1e-12)
	assert.InDelta(t, 3.0/7, got["edgesSharingTargetShare"], 1e-12)
	assert.InDelta(t, 1.0, got["edgesSharingTargetMean"], 1e-12)
	assert.Equal(t, 2.0, got["edgeTargetsP50"])
	assert.Equal(t, 2.0, got["edgesSharingTargetP95"])
	assert.Equal(t, 2.0, got["edgesIntoLargestTargetP95"])

	empty := edgeRedundancyOf(nil, nil, nil)
	assert.Empty(t, empty.diagnostics())
}

func TestHistPercentile(t *testing.T) {
	tests := []struct {
		name string
		hist []int
		q    float64
		want int
	}{
		{name: "empty", hist: nil, q: 0.5, want: 0},
		{name: "one bucket", hist: []int{0, 0, 7}, q: 0.95, want: 2},
		{name: "median", hist: []int{1, 1, 1, 1}, q: 0.5, want: 1},
		{name: "tail", hist: []int{90, 5, 5}, q: 0.95, want: 1},
		{name: "top", hist: []int{90, 5, 5}, q: 1, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, histPercentile(tt.hist, tt.q))
		})
	}
}

// On a build, the redundancy agrees with the edges it reads: every edge is
// counted once by rank and once by degree, and the shared edges are what is
// left over the distinct targets.
func TestEdgeRedundancyOfABuild(t *testing.T) {
	vectors := clusteredVectors(3000, 24, 30, 914)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), uc)
	st := index.state.Load()
	r := edgeRedundancyOf(st.postings.edgeStart, st.postings.edges, st.side.postingOf)

	assert.Equal(t, st.postings.vectors(), r.Vectors)
	assert.Equal(t, len(st.postings.edges), r.Edges)
	byRank, byDegree, targets := 0, 0, 0
	for _, row := range r.ByRank {
		byRank += row.Edges
	}
	for d, row := range r.ByDegree {
		byDegree += d * row.Vectors
		targets += row.Targets
	}
	assert.Equal(t, r.Edges, byRank)
	assert.Equal(t, r.Edges, byDegree)
	assert.Equal(t, r.Targets, targets)
	assert.Equal(t, r.Edges-r.Targets, r.Shared)
	// No edge leads into the posting of its own source.
	for slot := 0; slot < r.Vectors; slot++ {
		for _, endpoint := range st.postings.edgesOf(uint32(slot)) {
			assert.NotEqual(t, st.side.postingOf[slot], st.side.postingOf[endpoint])
		}
	}
}
