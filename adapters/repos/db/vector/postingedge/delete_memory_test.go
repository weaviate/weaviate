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
	"slices"

	"github.com/pkg/errors"
)

// The repairs of v6.2 (bench-report.md, "v6.2: reparación en el churn"), on
// top of the maintenance pass of v6.1. That pass takes its share of dead
// edges anew at every pass, so under small frequent passes the lists get
// shorter and are never repaired. Three ways around it:
//
//   - A: a vector remembers how many edges it has lost since a search built
//     its list, and is repaired when what it has lost and what it is about to
//     lose reach the share (repairOver, then eliminate).
//   - B: an edge to a deleted vector is given a substitute out of the list of
//     that vector, by a distance between two codes, with no search
//     (eliminateSubstituting).
//   - C: B, and the search of A for the vectors whose edges without a
//     substitute reach the share (repairOver with its rehearsal, then
//     eliminateSubstituting).

// substituteStats counts what the local substitution did.
type substituteStats struct {
	// Substituted counts the edges to a deleted vector that were given a
	// substitute, and Without those taken out with none.
	Substituted int `json:"edgesSubstituted"`
	Without     int `json:"edgesWithoutSubstitute"`
	// ListsRead counts the lists of deleted vectors read for it, Missing
	// the deleted vectors that were in no posting any more, Candidates the
	// endpoints of the lists read, and CodeDistances the distances between
	// two codes estimated.
	ListsRead     int `json:"deletedListsRead"`
	Missing       int `json:"deletedListsMissing"`
	Candidates    int `json:"candidates"`
	CodeDistances int `json:"codeDistances"`
	// PostingsWritten counts the postings it changed a list of.
	PostingsWritten int `json:"postingsWithAListChanged"`
}

// remember gives the index the memory of v6.2: the count of the edges each
// vector has lost, which a search that builds its list again takes back to
// zero.
func (d *deleteIndex) remember() {
	keys := len(d.s.live)
	d.lost = make([]uint16, keys)
	d.rebuilt = make([]bool, keys)
	d.substitutes = make(map[uint64][]uint32)
	d.s.onRefresh = func(docID uint64, _ []uint32) {
		d.lost[docID] = 0
		d.rebuilt[docID] = true
		delete(d.substitutes, docID)
	}
}

// codeDistance estimates the distance between two vectors from their sidecar
// codes, with no vector read.
func (d *deleteIndex) codeDistance(a, b []uint64) (float32, error) {
	full, ok := d.s.st.quantizer.(fullCodec)
	if !ok {
		return 0, errors.New("the sidecar codes are capped: the quantizer has no distance between two of them")
	}
	return full.DistanceBetweenCompressedVectors(a, b)
}

// substitutesFor takes the edges of a live vector of posting p and returns
// those that lead to live vectors, in their order, and a substitute for each
// of the others that has one: of the list of the deleted vector the edge led
// to, as it is in its posting, the endpoint nearest the vector by the
// distance between their codes that is live, is not the vector, is outside p
// and is not among its edges or the substitutes it was just given. The
// substitutes come nearest first. without counts the edges left with none.
// Nothing is changed.
func (d *deleteIndex) substitutesFor(docID uint64, p uint32, edges []uint32, stats *substituteStats) (kept, subs []uint32, without int, err error) {
	st := d.s.st
	dead := 0
	for _, endpoint := range edges {
		if d.deleted[endpoint] {
			dead++
		}
	}
	if dead == 0 {
		return edges, nil, 0, nil
	}
	kept = make([]uint32, 0, len(edges)-dead)
	for _, endpoint := range edges {
		if !d.deleted[endpoint] {
			kept = append(kept, endpoint)
		}
	}
	own := st.side.code(uint32(docID))
	var dists []float32
	for _, endpoint := range edges {
		if !d.deleted[endpoint] {
			continue
		}
		l := &st.postings.lists[st.side.postingOf[endpoint]]
		m := l.member(uint64(endpoint))
		if m < 0 {
			stats.Missing++
			without++
			continue
		}
		stats.ListsRead++
		var (
			best     uint32
			bestDist float32
			found    bool
		)
		for _, candidate := range l.edgesOf(m) {
			stats.Candidates++
			if d.deleted[candidate] || uint64(candidate) == docID || st.side.postingOf[candidate] == p ||
				slices.Contains(kept, candidate) || slices.Contains(subs, candidate) {
				continue
			}
			dist, err := d.codeDistance(own, st.side.code(candidate))
			if err != nil {
				return nil, nil, 0, errors.Wrapf(err, "the distance from doc %d to doc %d", docID, candidate)
			}
			stats.CodeDistances++
			if !found || dist < bestDist {
				best, bestDist, found = candidate, dist, true
			}
		}
		if !found {
			without++
			continue
		}
		at := len(subs)
		for at > 0 && dists[at-1] > bestDist {
			at--
		}
		subs = slices.Insert(subs, at, best)
		dists = slices.Insert(dists, at, bestDist)
	}
	return kept, subs, without, nil
}

// checkSubstitutes checks the substitutes a vector of posting p was given,
// next to the edges it kept.
func (d *deleteIndex) checkSubstitutes(docID uint64, p uint32, kept, subs []uint32) error {
	postingOf := d.s.st.side.postingOf
	for i, sub := range subs {
		switch {
		case d.deleted[sub]:
			return errors.Errorf("STOP: doc %d was given doc %d as a substitute, which is deleted", docID, sub)
		case uint64(sub) == docID:
			return errors.Errorf("STOP: doc %d was given itself as a substitute", docID)
		case postingOf[sub] == p:
			return errors.Errorf("STOP: doc %d was given doc %d as a substitute, which is in its posting, %d", docID, sub, p)
		case slices.Contains(kept, sub) || slices.Contains(subs[:i], sub):
			return errors.Errorf("STOP: doc %d was given doc %d as a substitute, which it had", docID, sub)
		}
	}
	return nil
}

// checkEdgeLists checks the list of every live vector: no more edges than
// the index gives a vector, none to itself and none twice.
func (d *deleteIndex) checkEdgeLists() error {
	s := d.s
	for p := range s.st.postings.lists {
		l := &s.st.postings.lists[p]
		for i, docID := range l.docIDs {
			if d.deleted[docID] {
				continue
			}
			edges := l.edgesOf(i)
			if len(edges) > s.neighbors {
				return errors.Errorf("STOP: doc %d has %d edges, more than %d", docID, len(edges), s.neighbors)
			}
			for e, endpoint := range edges {
				if uint64(endpoint) == docID {
					return errors.Errorf("STOP: doc %d has an edge to itself", docID)
				}
				if slices.Contains(edges[:e], endpoint) {
					return errors.Errorf("STOP: doc %d has doc %d twice among its edges", docID, endpoint)
				}
			}
		}
	}
	return nil
}

// substitute gives every edge of a live vector that leads to a deleted one
// its substitute, at the end of the list, or takes it out and counts it among
// those the vector has lost. The lists of the deleted vectors are left as
// they are: they are where the substitutes come from.
func (d *deleteIndex) substitute() error {
	s, st := d.s, d.s.st
	if d.lost == nil {
		return errors.New("the index keeps no count of the edges lost")
	}
	for p := range st.postings.lists {
		if s.retired[p] {
			continue
		}
		l := &st.postings.lists[p]
		edges := make([]uint32, 0, len(l.edges))
		starts := make([]uint32, 1, len(l.docIDs)+1)
		changed := false
		for i, docID := range l.docIDs {
			old := l.edgesOf(i)
			if d.deleted[docID] {
				edges = append(edges, old...)
				starts = append(starts, uint32(len(edges)))
				continue
			}
			kept, subs, without, err := d.substitutesFor(docID, uint32(p), old, &d.sub)
			if err != nil {
				return err
			}
			if len(subs)+without > 0 {
				if err := d.checkSubstitutes(docID, uint32(p), kept, subs); err != nil {
					return err
				}
				changed = true
				d.sub.Substituted += len(subs)
				d.sub.Without += without
				d.lost[docID] += uint16(without)
				if len(subs) > 0 {
					d.substitutes[docID] = append(d.substitutes[docID], subs...)
				}
			}
			edges = append(edges, kept...)
			edges = append(edges, subs...)
			starts = append(starts, uint32(len(edges)))
		}
		l.edges, l.edgeStart = edges, starts
		if changed {
			d.sub.PostingsWritten++
		}
	}
	return nil
}

// eliminateSubstituting is the second phase of the pass with the local
// substitution: first every edge to a deleted vector is given its substitute
// or taken out, and then the phase goes on as eliminate does, which finds no
// such edge left.
func (d *deleteIndex) eliminateSubstituting(ctx context.Context, mergeUnder, sumWithin int) error {
	if err := d.substitute(); err != nil {
		return err
	}
	dropped := d.stats.DroppedEdges
	if err := d.eliminate(ctx, mergeUnder, sumWithin); err != nil {
		return err
	}
	if left := d.stats.DroppedEdges - dropped; left != 0 {
		return errors.Errorf("STOP: the substitution left %d edges to deleted vectors", left)
	}
	return d.checkEdgeLists()
}

// pendingPostings counts the postings the second phase has to read and
// write: those that hold a deleted vector, or a live one with an edge to a
// deleted vector.
func (d *deleteIndex) pendingPostings() int {
	s := d.s
	pending := 0
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		l := &s.st.postings.lists[p]
		touched := slices.ContainsFunc(l.docIDs, func(docID uint64) bool { return d.deleted[docID] }) ||
			slices.ContainsFunc(l.edges, func(endpoint uint32) bool { return d.deleted[endpoint] })
		if touched {
			pending++
		}
	}
	return pending
}

// memoryCounts is what the memory of v6.2 holds of the live vectors.
type memoryCounts struct {
	// Lost is the edges the live vectors have lost since their lists were
	// built, and WithLost the vectors that have lost any.
	Lost     int `json:"edgesLost"`
	WithLost int `json:"vectorsWithEdgesLost"`
	// Rebuilt counts the live vectors whose list a search built again.
	Rebuilt int `json:"vectorsRebuilt"`
	// SubstituteEdges counts the edges that are substitutes still in the
	// list they were given to, and WithSubstitute the vectors that have one.
	SubstituteEdges int `json:"substituteEdges"`
	WithSubstitute  int `json:"vectorsWithSubstitute"`
}

// substitutesOf returns, of the edges of a vector, those that are substitutes
// it was given.
func (d *deleteIndex) substitutesOf(docID uint64, edges []uint32) []uint32 {
	var out []uint32
	given := d.substitutes[docID]
	for _, endpoint := range edges {
		if slices.Contains(given, endpoint) {
			out = append(out, endpoint)
		}
	}
	return out
}

// memory counts what the index remembers of its live vectors.
func (d *deleteIndex) memory() memoryCounts {
	var out memoryCounts
	for p := range d.s.st.postings.lists {
		l := &d.s.st.postings.lists[p]
		for i, docID := range l.docIDs {
			if d.deleted[docID] {
				continue
			}
			if lost := int(d.lost[docID]); lost > 0 {
				out.Lost += lost
				out.WithLost++
			}
			if d.rebuilt[docID] {
				out.Rebuilt++
			}
			if subs := len(d.substitutesOf(docID, l.edgesOf(i))); subs > 0 {
				out.SubstituteEdges += subs
				out.WithSubstitute++
			}
		}
	}
	return out
}

func (s substituteStats) minus(o substituteStats) substituteStats {
	return substituteStats{
		Substituted: s.Substituted - o.Substituted, Without: s.Without - o.Without, ListsRead: s.ListsRead - o.ListsRead,
		Missing: s.Missing - o.Missing, Candidates: s.Candidates - o.Candidates,
		CodeDistances: s.CodeDistances - o.CodeDistances, PostingsWritten: s.PostingsWritten - o.PostingsWritten,
	}
}
