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

import "slices"

// edgeTrace follows, through one search, the redundant edges it meets: the
// edges whose source has a nearer edge into the same posting, earlier in its
// row. It counts how many of the endpoints the search scores come through
// such an edge, and how many of those give their posting its rank when the
// posting is picked, or when the search ends without picking it: with one
// edge per target posting, those are the postings whose priority would
// change.
//
// A search keeps one only when its caller asks for it through SearchStats,
// and does all of the counting outside its timed sections. It is a
// diagnostic, and allocates as it goes.
type edgeTrace struct {
	edgeTraceCounts

	// redundantScored holds the endpoints scored through a redundant edge
	// that no direct edge has reached since.
	redundantScored map[uint32]struct{}
	// setter is the endpoint that gave each ranked posting its rank.
	setter map[uint32]uint32

	// The load being traced: the endpoints it is predicted to score, in
	// order, and the ranks of their postings before the load.
	predicted []uint32
	prior     map[uint32]float32
	seen      map[uint32]struct{}
	row       []bool
}

// edgeTraceCounts are the counts of one search.
type edgeTraceCounts struct {
	// Redundant is the number of scored endpoints that came through a
	// redundant edge.
	Redundant int
	// ReachedDirectly is the number of those that a direct edge of another
	// source reached later in the same search. With one edge per target
	// posting they would still be scored, through that edge.
	ReachedDirectly int
	// RankSet is the number of postings whose rank, when they were picked
	// or when the search ended, came from an endpoint in Redundant that no
	// direct edge had reached by then. RankSetLoaded is how many of them
	// were picked, and so loaded.
	RankSet       int
	RankSetLoaded int
	// RankedLoaded is the number of postings loaded by rank: all of the
	// loaded ones but the entry postings.
	RankedLoaded int
	// Mispredicted is the number of loads whose scored endpoints were not
	// the ones the trace predicted. Anything but 0 is a bug of the trace.
	Mispredicted int
}

func newEdgeTrace() *edgeTrace {
	return &edgeTrace{
		redundantScored: map[uint32]struct{}{},
		setter:          map[uint32]uint32{},
		prior:           map[uint32]float32{},
		seen:            map[uint32]struct{}{},
	}
}

// redundantEdges marks, in a row of edges nearest first, the edges whose
// target posting a nearer edge of the row already leads to. It reuses marks.
func redundantEdges(row, postingOf []uint32, marks []bool) []bool {
	marks = marks[:0]
	for j, endpoint := range row {
		target := postingOf[endpoint]
		redundant := false
		for _, nearer := range row[:j] {
			if postingOf[nearer] == target {
				redundant = true
				break
			}
		}
		marks = append(marks, redundant)
	}
	return marks
}

// setByRedundant tells whether the rank of posting p comes from an endpoint
// scored through a redundant edge that no direct edge has reached since.
func (tr *edgeTrace) setByRedundant(p uint32) bool {
	endpoint, ranked := tr.setter[p]
	if !ranked {
		return false
	}
	_, redundant := tr.redundantScored[endpoint]
	return redundant
}

// picked counts posting p, about to be loaded, by where its rank came from.
// Its rank is the one it was picked with: once picked, a posting is marked
// loaded and no endpoint of it is scored again.
func (tr *edgeTrace) picked(sc *searchScratch, p uint32) {
	if sc.rank[p] == inf32 {
		return // an entry posting
	}
	tr.RankedLoaded++
	if tr.setByRedundant(p) {
		tr.RankSet++
		tr.RankSetLoaded++
	}
}

// predict walks the sources of a loaded posting as collectEndpoints is
// about to: which endpoints it will score, through which kind of edge, and
// the ranks of their postings before. An endpoint scored before through a
// redundant edge that a direct edge reaches now is reached directly.
func (tr *edgeTrace) predict(sc *searchScratch, postingOf []uint32, post *posting, cut float32) {
	tr.predicted = tr.predicted[:0]
	clear(tr.prior)
	clear(tr.seen)
	for i, dist := range sc.memberDist {
		if dist > cut {
			continue
		}
		row := post.edgesOf(i)
		tr.row = redundantEdges(row, postingOf, tr.row)
		for j, endpoint := range row {
			target := postingOf[endpoint]
			if sc.loaded[target] {
				continue
			}
			_, again := tr.seen[endpoint]
			if again || sc.endpoints.Visited(uint64(endpoint)) {
				if _, ok := tr.redundantScored[endpoint]; ok && !tr.row[j] {
					delete(tr.redundantScored, endpoint)
					tr.ReachedDirectly++
				}
				continue
			}
			tr.seen[endpoint] = struct{}{}
			tr.predicted = append(tr.predicted, endpoint)
			if tr.row[j] {
				tr.Redundant++
				tr.redundantScored[endpoint] = struct{}{}
			}
			if _, ok := tr.prior[target]; !ok {
				tr.prior[target] = sc.rank[target]
			}
		}
	}
}

// scored follows the ranks the endpoints of the load gave their postings,
// as rankPostings folded them in, once they are scored.
func (tr *edgeTrace) scored(sc *searchScratch, postingOf []uint32, dists []float32) {
	if !slices.Equal(tr.predicted, sc.unscored) {
		tr.Mispredicted++
		return
	}
	for i, endpoint := range sc.unscored {
		target := postingOf[endpoint]
		if dists[i] < tr.prior[target] {
			tr.prior[target] = dists[i]
			tr.setter[target] = endpoint
		}
	}
}

// finish counts the ranked postings the search ended without loading.
func (tr *edgeTrace) finish(sc *searchScratch) {
	for _, p := range sc.frontier {
		if !sc.loaded[p] && tr.setByRedundant(p) {
			tr.RankSet++
		}
	}
}
