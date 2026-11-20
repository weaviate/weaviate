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
	"math/rand/v2"
	"slices"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
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

// overShare lists, by doc id, the live vectors the share of v6.2 takes: those
// for which what they have lost since their list was built, and what they are
// about to lose, are num/den or more of their list as it would be had it lost
// nothing, and are at least one edge. What a vector is about to lose is its
// edges to deleted vectors, or with rehearse those of them the substitution
// would leave without a substitute, with the lists as they are now.
func (d *deleteIndex) overShare(num, den int, rehearse bool) ([]uint64, error) {
	if d.lost == nil {
		return nil, errors.New("the index keeps no count of the edges lost")
	}
	var out []uint64
	for p := range d.s.st.postings.lists {
		l := &d.s.st.postings.lists[p]
		for i, docID := range l.docIDs {
			if d.deleted[docID] {
				continue
			}
			edges := l.edgesOf(i)
			pending := 0
			for _, endpoint := range edges {
				if d.deleted[endpoint] {
					pending++
				}
			}
			if rehearse && pending > 0 {
				var err error
				if _, _, pending, err = d.substitutesFor(docID, uint32(p), edges, &d.trial); err != nil {
					return nil, err
				}
			}
			lost := int(d.lost[docID])
			if gone := lost + pending; gone > 0 && den*gone >= num*(len(edges)+lost) {
				out = append(out, docID)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

// repairOver is the first phase of the pass with its memory: every vector
// overShare lists when it starts builds its list again by search, with the
// deleted vectors navigable, as repair does.
func (d *deleteIndex) repairOver(ctx context.Context, num, den int, rehearse bool) (refreshStats, error) {
	st := d.s.st
	sources, skip := st.deletedAsSources, st.skipDeletedEndpoints
	st.deletedAsSources, st.skipDeletedEndpoints = true, false
	defer func() { st.deletedAsSources, st.skipDeletedEndpoints = sources, skip }()

	docIDs, err := d.overShare(num, den, rehearse)
	if err != nil {
		return refreshStats{}, err
	}
	stats, err := d.s.refreshEdges(ctx, docIDs)
	if err != nil {
		return stats, err
	}
	return stats, d.checkLists(docIDs, "repair")
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

// memorySpec is a cell of v6.2: what the first phase of its pass repairs and
// what the second does with an edge that leads to a deleted vector.
type memorySpec struct {
	key, name string
	// counted has the first phase take its share over what a vector has
	// lost since its list was built (A and C). Without it, and without
	// substitute, the share is taken over the dead edges of the pass alone,
	// as v6.1 does.
	counted bool
	// substitute has the second phase give such an edge a substitute (B and
	// C). A cell that substitutes and does not count has no first phase (B).
	substitute bool
	// mine marks the cell the order did not ask for: the reference.
	mine bool
}

// memorySpecs are the cells of v6.2, in the order they run.
var memorySpecs = []memorySpec{
	{key: "v61", name: "the pass of v6.1: repaired with a quarter of the list dead in the pass, edges taken out", mine: true},
	{key: "A", name: "A: repaired with a quarter of the list lost since it was built, edges taken out", counted: true},
	{key: "B", name: "B: no repair, edges substituted", substitute: true},
	{
		key: "C", name: "C: edges substituted, repaired with a quarter of the list lost with no substitute",
		counted: true, substitute: true,
	},
}

// The share of the first phase of every cell of v6.2, a quarter, and the
// vectors of the sample its edge recall is counted on.
const (
	memoryNum, memoryDen = 1, 4
	memorySampleSize     = 2000
)

func (s substituteStats) minus(o substituteStats) substituteStats {
	return substituteStats{
		Substituted: s.Substituted - o.Substituted, Without: s.Without - o.Without, ListsRead: s.ListsRead - o.ListsRead,
		Missing: s.Missing - o.Missing, Candidates: s.Candidates - o.Candidates,
		CodeDistances: s.CodeDistances - o.CodeDistances, PostingsWritten: s.PostingsWritten - o.PostingsWritten,
	}
}

// memoryPass is one maintenance pass of a cell of v6.2.
type memoryPass struct {
	// Repair is its first phase: the lists a search built again.
	Repair refreshStats `json:"repair"`
	// SecondPhasePostings counts the postings its second phase reads and
	// writes: those that hold a deleted vector or a list that changes.
	SecondPhasePostings int `json:"postingsOfSecondPhase"`
	// Substitution is what the substitution of its second phase did, and
	// Rehearsal what the rehearsal of cell C did before its first phase,
	// which changes nothing.
	Substitution substituteStats `json:"substitution"`
	Rehearsal    substituteStats `json:"rehearsal"`
	// Dropped counts the edges to deleted vectors taken out with nothing in
	// their place, and Merges the postings merged into another.
	Dropped int `json:"edgesDropped"`
	Merges  int `json:"merges"`
	// Memory is what the index remembers of its live vectors after the pass.
	Memory memoryCounts `json:"memory"`
}

// pass runs one maintenance pass as the cell does it: its first phase, if it
// has one, and its second, with the merges of the winner of v6.1.
func (d *deleteIndex) pass(ctx context.Context, spec memorySpec, mergeUnder, sumWithin int) (memoryPass, error) {
	var out memoryPass
	sub, trial, dropped, merges := d.sub, d.trial, d.stats.DroppedEdges, d.stats.Merges
	var err error
	switch {
	case spec.counted:
		out.Repair, err = d.repairOver(ctx, memoryNum, memoryDen, spec.substitute)
	case !spec.substitute:
		out.Repair, err = d.repair(ctx, memoryNum, memoryDen)
	}
	if err != nil {
		return out, errors.Wrap(err, "the first phase")
	}
	out.SecondPhasePostings = d.pendingPostings()
	if spec.substitute {
		err = d.eliminateSubstituting(ctx, mergeUnder, sumWithin)
	} else if err = d.eliminate(ctx, mergeUnder, sumWithin); err == nil {
		err = d.checkEdgeLists()
	}
	if err != nil {
		return out, errors.Wrap(err, "the second phase")
	}
	out.Substitution, out.Rehearsal = d.sub.minus(sub), d.trial.minus(trial)
	out.Dropped = d.stats.DroppedEdges - dropped + out.Substitution.Without
	out.Merges = d.stats.Merges - merges
	out.Memory = d.memory()
	return out, nil
}

// memorySample draws the vectors the edge recall of a stage of v6.2 is
// counted on: size of the live ones, with a fixed seed, by doc id.
func memorySample(live []bool, size int) []uint64 {
	var ids []uint64
	for id, isLive := range live {
		if isLive {
			ids = append(ids, uint64(id))
		}
	}
	rand.New(rand.NewPCG(8, 8)).Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	ids = ids[:min(size, len(ids))]
	slices.Sort(ids)
	return ids
}

// memoryNearest returns, for every vector of the sample, its neighbors
// nearest among the live vectors, itself left out, nearest first, by brute
// force: what streamIndex.nearestLive returns for it.
func memoryNearest(ctx context.Context, logger logrus.FieldLogger, provider distancer.Provider,
	vectorOf func(uint64) []float32, live []bool, sample []uint64, neighbors int,
) ([][]scored, error) {
	out := make([][]scored, len(sample))
	err := parallelChunks(ctx, logger, len(sample), 1, func(_, start, end int) error {
		var nearest topK
		for i := start; i < end; i++ {
			from := vectorOf(sample[i])
			nearest.reset(neighbors)
			for candidate, isLive := range live {
				if !isLive || uint64(candidate) == sample[i] {
					continue
				}
				dist, err := provider.SingleDist(from, vectorOf(uint64(candidate)))
				if err != nil {
					return err
				}
				nearest.push(uint64(candidate), dist)
			}
			out[i] = slices.Clone(nearest.sorted())
		}
		return nil
	})
	return out, err
}

// memoryGroup is the edge recall of the vectors of the sample that are of
// one group.
type memoryGroup struct {
	Name string `json:"name"`
	edgeCounts
	Recall        float64 `json:"recall"`
	PostingRecall float64 `json:"postingRecall"`
	Degree        float64 `json:"degree"`
	// Substitutes counts the edges of the group that are substitutes still
	// in the list they were given to, and SubstitutesTrue those of them
	// that are true edges: among the nearest live neighbors of their vector
	// and outside its posting.
	Substitutes     int `json:"substitutes"`
	SubstitutesTrue int `json:"substitutesTrue"`
}

func (g *memoryGroup) read() {
	g.Recall, g.PostingRecall, g.Degree = g.recall(), g.postingRecall(), g.degree()
}

// memoryEdges is the edge recall of v2.x on the sample of a stage of v6.2:
// of the nearest live neighbors of a vector, those outside its posting are
// its true edges, and the recall is how many of them are in its list.
type memoryEdges struct {
	Sample int         `json:"sample"`
	All    memoryGroup `json:"all"`
	// Groups splits the sample: the vectors inserted in the churn; of the
	// others, those whose list holds a substitute; of the rest, those whose
	// list a search built again; and those left, whose list is the one of
	// the batch build, with the edges it may have lost.
	Groups []memoryGroup `json:"groups,omitempty"`
}

// The groups of memoryEdges, in the order a vector is tried against them.
var memoryGroupNames = []string{
	"inserted in the churn", "with a substitute", "repaired by search", "neither repaired nor with a substitute",
}

// edgesOfSample counts the vectors of the sample as they are now, against
// their nearest live neighbors, in all and by group. inserted marks the
// vectors inserted in the churn, and is nil without one.
func (d *deleteIndex) edgesOfSample(sample []uint64, nearest [][]scored, inserted []bool) (memoryEdges, error) {
	st := d.s.st
	out := memoryEdges{Sample: len(sample), All: memoryGroup{Name: "all"}}
	groups := make([]memoryGroup, len(memoryGroupNames))
	for g := range groups {
		groups[g].Name = memoryGroupNames[g]
	}
	for i, docID := range sample {
		if !d.s.live[docID] {
			return out, errors.Errorf("doc %d of the sample is not live", docID)
		}
		p := st.side.postingOf[docID]
		l := &st.postings.lists[p]
		m := l.member(docID)
		if m < 0 {
			return out, errors.Errorf("doc %d is not in posting %d, where the map has it", docID, p)
		}
		edges := l.edgesOf(m)
		subs := d.substitutesOf(docID, edges)
		g := 3
		switch {
		case inserted != nil && inserted[docID]:
			g = 0
		case len(subs) > 0:
			g = 1
		case d.rebuilt != nil && d.rebuilt[docID]:
			g = 2
		}
		counts := countEdgesIn(st.side.postingOf, p, edges, nearest[i])
		trueSubs := 0
		for _, sub := range subs {
			if st.side.postingOf[sub] != p &&
				slices.ContainsFunc(nearest[i], func(nb scored) bool { return nb.id == uint64(sub) }) {
				trueSubs++
			}
		}
		for _, group := range []*memoryGroup{&out.All, &groups[g]} {
			group.add(counts)
			group.Substitutes += len(subs)
			group.SubstitutesTrue += trueSubs
		}
	}
	out.All.read()
	for g := range groups {
		if groups[g].Vectors > 0 {
			groups[g].read()
			out.Groups = append(out.Groups, groups[g])
		}
	}
	return out, nil
}

// edgesOfBatch counts the vectors of the sample in a batch index, which is
// keyed by slot, against their nearest live neighbors.
func edgesOfBatch(st *state, keys int, sample []uint64, nearest [][]scored) (memoryEdges, error) {
	store := st.postings
	out := memoryEdges{Sample: len(sample), All: memoryGroup{Name: "all"}}
	if store.lists != nil {
		return out, errors.New("the index is keyed by doc id")
	}
	postingOf, slotOf := make([]uint32, keys), make([]uint32, keys)
	for i := range postingOf {
		postingOf[i] = unplaced
	}
	for p := range store.postings() {
		for slot := store.start[p]; slot < store.start[p+1]; slot++ {
			docID := store.docIDs[slot]
			if docID >= uint64(keys) {
				return out, errors.Errorf("doc id %d is past the %d keys", docID, keys)
			}
			postingOf[docID], slotOf[docID] = uint32(p), slot
		}
	}
	for i, docID := range sample {
		p := postingOf[docID]
		if p == unplaced {
			return out, errors.Errorf("doc %d of the sample is not in the index", docID)
		}
		slots := store.edgesOf(slotOf[docID])
		edges := make([]uint32, len(slots))
		for e, endpoint := range slots {
			edges[e] = uint32(store.docIDs[endpoint])
		}
		out.All.add(countEdgesIn(postingOf, p, edges, nearest[i]))
	}
	out.All.read()
	return out, nil
}
