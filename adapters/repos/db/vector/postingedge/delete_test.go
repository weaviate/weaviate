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

// The deletes experiment of v6.0 (bench-report.md, "v6.0: deletes en
// memoria"), over the streaming index of v2.x. A delete marks its vector:
// the search skips a marked member of a posting, so it is never returned
// (state.deleted). What is done about the marked vectors beyond that is the
// cells of the experiment, each on top of the one before:
//
//  1. Nothing: the edges that lead to a deleted vector still have it scored,
//     from its sidecar code, as a hint of where to go. In its variant 1b a
//     deleted member that is still in its posting is a source besides: it
//     is scored and its edges are followed (state.deletedAsSources).
//  2. A deleted endpoint is not scored (state.skipDeletedEndpoints).
//  3. A cleanup pass: a posting with more than a share of its members
//     deleted is written again without them, and one left with fewer live
//     members than a minimum is merged into the posting whose centroid is
//     nearest.
//  4. A refresh pass: a live vector with half or more of its edges leading
//     to deleted vectors builds its list again, by search.
//
// Nothing of it is in the index but the mark the search reads.

// deleteStats counts what the deletes and their cleanup did.
type deleteStats struct {
	Deleted int `json:"deleted"`
	// Rewritten counts the postings written again without their deleted
	// members by the cleanup pass, CopiedLive the live members copied by
	// those, and DroppedDead the deleted members dropped, by those and by
	// the merges and the splits that drop them first.
	Rewritten   int `json:"postingsRewritten"`
	CopiedLive  int `json:"liveMembersCopied"`
	DroppedDead int `json:"deletedMembersDropped"`
	// Merges counts the postings merged into another, MovedLive their live
	// members, and EmptyRetired the postings retired with no live member.
	// MergeSplits counts the postings a merge left past the threshold,
	// which were split.
	Merges       int `json:"merges"`
	MovedLive    int `json:"liveMembersMoved"`
	EmptyRetired int `json:"emptyPostingsRetired"`
	MergeSplits  int `json:"splitsAfterMerge"`
	// SplitRewrites counts the postings an insert left past the threshold
	// that dropped their deleted members before splitting, and SplitsSaved
	// those that no longer had to split.
	SplitRewrites int `json:"rewritesBeforeSplit"`
	SplitsSaved   int `json:"splitsSaved"`
	// DroppedEdges counts the edges that led to a deleted vector and were
	// taken out of their list by the second phase of the maintenance pass,
	// and MergesSkipped the postings under the size of its merges that it
	// left alone, the two together being past the size a merge may make.
	DroppedEdges  int `json:"edgesDropped"`
	MergesSkipped int `json:"mergesSkippedBySize"`
}

// deleteIndex is a streaming index with deletes.
type deleteIndex struct {
	s *streamIndex
	// deleted marks the deleted vectors by doc id: it is the mark the
	// search reads, and it stays once a cleanup drops the vector from its
	// posting, since edges still lead to it.
	deleted []bool
	// rewriteOver is the share of deleted members past which the cleanup
	// writes a posting again, and minLive the live members under which it
	// merges it.
	rewriteOver float64
	minLive     int
	// received marks the postings the last cleanup pass merged another
	// into.
	received map[uint32]bool
	stats    deleteStats
	// The memory of v6.2, which remember sets. lost is, by doc id, how many
	// of its edges a live vector has had taken out with nothing put in their
	// place since a search last built its list. rebuilt marks the vectors
	// whose list a search built again, and substitutes holds, by doc id, the
	// substitutes its list was given since. sub counts what the substitution
	// did, and trial what its rehearsals did, which change nothing.
	lost        []uint16
	rebuilt     []bool
	substitutes map[uint64][]uint32
	sub, trial  substituteStats
}

// newDeleteIndex gives a streaming index the mark of the deletes. Deleted
// endpoints are scored until told otherwise.
func newDeleteIndex(s *streamIndex, rewriteOver float64, minLive int) *deleteIndex {
	d := &deleteIndex{s: s, deleted: make([]bool, len(s.live)), rewriteOver: rewriteOver, minLive: minLive}
	s.st.deleted = d.deleted
	s.st.skipDeletedEndpoints, s.st.deletedAsSources = false, false
	return d
}

// remove deletes a vector: it is marked, and stays in its posting.
func (d *deleteIndex) remove(docID uint64) error {
	s := d.s
	if docID >= uint64(len(s.live)) || !s.live[docID] {
		return errors.Errorf("doc %d is not in the index", docID)
	}
	d.deleted[docID] = true
	s.live[docID] = false
	s.liveCount--
	d.stats.Deleted++
	return nil
}

// scoreDeletedEndpoints tells the search whether an edge that leads to a
// deleted vector has it scored, as a hint, or is left out.
func (d *deleteIndex) scoreDeletedEndpoints(on bool) {
	d.s.st.skipDeletedEndpoints = !on
}

// deletedAreSources tells the search whether a deleted member that is still
// in its posting is scored and has its edges followed, as a source, without
// ever being a candidate: cell 1b.
func (d *deleteIndex) deletedAreSources(on bool) {
	d.s.st.deletedAsSources = on
}

// dropAtSplit has a posting that an insert leaves past the split threshold
// drop its deleted members first: it is split only if it is still past it.
func (d *deleteIndex) dropAtSplit() {
	d.s.beforeSplit = func(p uint32) {
		if d.dead(p) == 0 {
			return
		}
		d.stats.SplitRewrites++
		d.stats.DroppedDead += d.rewrite(p)
		if len(d.s.st.postings.lists[p].docIDs) <= d.s.cfg.threshold {
			d.stats.SplitsSaved++
		}
	}
}

// dead counts the deleted members of a posting.
func (d *deleteIndex) dead(p uint32) int {
	dead := 0
	for _, docID := range d.s.st.postings.lists[p].docIDs {
		if d.deleted[docID] {
			dead++
		}
	}
	return dead
}

// rewrite writes a posting again without its deleted members, each of the
// others with its code and its list of edges, and tells how many it dropped.
func (d *deleteIndex) rewrite(p uint32) int {
	st := d.s.st
	old := st.postings.lists[p]
	words := st.postings.codeWords
	fresh := newPostingList()
	dropped := 0
	for i, docID := range old.docIDs {
		if d.deleted[docID] {
			dropped++
			continue
		}
		fresh.append(docID, old.codes[i*words:(i+1)*words], old.edgesOf(i))
	}
	st.postings.lists[p] = fresh
	return dropped
}

// cleanup is the cleanup pass of cell 3 over the postings there are when it
// starts, in order. First every posting with more than rewriteOver of its
// members deleted is written again without them. Then every posting left
// with fewer than minLive live members is merged into its nearest one. A
// posting that another was merged into in this pass is not looked at.
func (d *deleteIndex) cleanup(ctx context.Context) error {
	s := d.s
	postings := len(s.st.postings.lists)
	for p := range uint32(postings) {
		if s.retired[p] {
			continue
		}
		n, dead := len(s.st.postings.lists[p].docIDs), d.dead(p)
		if n > 0 && float64(dead) > d.rewriteOver*float64(n) {
			d.stats.Rewritten++
			d.stats.CopiedLive += n - dead
			d.stats.DroppedDead += d.rewrite(p)
		}
	}
	d.received = make(map[uint32]bool)
	for p := range uint32(postings) {
		if s.retired[p] || d.received[p] {
			continue
		}
		if len(s.st.postings.lists[p].docIDs)-d.dead(p) >= d.minLive {
			continue
		}
		if err := d.merge(ctx, p); err != nil {
			return errors.Wrapf(err, "merge posting %d", p)
		}
	}
	return nil
}

// merge moves the live members of a posting to the end of the live posting
// whose centroid is nearest to its own, with their codes and their lists of
// edges, and retires it: its centroid leaves the graph. The posting map of
// every member moved follows. No edge changes, since they hold doc ids, and
// the centroid of the posting that takes them in stays where it was. If that
// posting ends up past the split threshold it drops its deleted members and,
// if it is still past it, splits. A posting without live members is retired
// and nothing else.
func (d *deleteIndex) merge(ctx context.Context, p uint32) error {
	s, st := d.s, d.s.st
	d.stats.DroppedDead += d.rewrite(p)
	moving := st.postings.lists[p]
	words := st.postings.codeWords
	target := uint32(unplaced)
	if len(moving.docIDs) == 0 {
		d.stats.EmptyRetired++
	} else {
		var err error
		if target, err = d.nearestPosting(ctx, p); err != nil {
			return err
		}
		into := &st.postings.lists[target]
		for i, docID := range moving.docIDs {
			into.append(docID, moving.codes[i*words:(i+1)*words], moving.edgesOf(i))
			st.side.postingOf[docID] = target
		}
		d.received[target] = true
		d.stats.Merges++
		d.stats.MovedLive += len(moving.docIDs)
	}
	st.postings.lists[p] = newPostingList()
	s.retired[p] = true
	if err := st.centroids.graph.Delete(uint64(p)); err != nil {
		return errors.Wrap(err, "delete the centroid")
	}
	if target == unplaced || len(st.postings.lists[target].docIDs) <= s.cfg.threshold {
		return nil
	}
	d.stats.DroppedDead += d.rewrite(target)
	if len(st.postings.lists[target].docIDs) <= s.cfg.threshold {
		return nil
	}
	before := s.stats.Splits
	if err := s.split(ctx, target); err != nil {
		return errors.Wrapf(err, "split posting %d", target)
	}
	d.stats.MergeSplits += s.stats.Splits - before
	return nil
}

// nearestPosting returns the live posting whose centroid the centroid graph
// finds nearest to that of p, p aside.
func (d *deleteIndex) nearestPosting(ctx context.Context, p uint32) (uint32, error) {
	s, st := d.s, d.s.st
	for limit := 4; limit <= 4096; limit *= 4 {
		ids, err := st.centroids.nearest(ctx, st.centroids.vectors[p], limit)
		if err != nil {
			return 0, err
		}
		for _, id := range ids {
			if uint32(id) != p && int(id) < len(s.retired) && !s.retired[id] {
				return uint32(id), nil
			}
		}
		if len(ids) < limit {
			break
		}
	}
	return 0, errors.Errorf("the centroid graph finds no live posting next to %d", p)
}

// halfDead lists, by doc id, the live vectors with half or more of their
// edges leading to deleted vectors. A vector without edges is not one.
func (d *deleteIndex) halfDead() []uint64 {
	return d.withDeadEdges(1, 2)
}

// withDeadEdges lists, by doc id, the live vectors with at least one edge
// that leads to a deleted vector and with num/den or more of their edges
// leading to one: 0/1 is every vector with a dead edge.
func (d *deleteIndex) withDeadEdges(num, den int) []uint64 {
	var out []uint64
	for p := range d.s.st.postings.lists {
		l := &d.s.st.postings.lists[p]
		for i, docID := range l.docIDs {
			if d.deleted[docID] {
				continue
			}
			edges := l.edgesOf(i)
			dead := 0
			for _, endpoint := range edges {
				if d.deleted[endpoint] {
					dead++
				}
			}
			if dead > 0 && den*dead >= num*len(edges) {
				out = append(out, docID)
			}
		}
	}
	slices.Sort(out)
	return out
}

// checkLists checks that no list of the given vectors leads to a deleted
// one: a search never has a deleted vector among its results, so a list it
// built cannot.
func (d *deleteIndex) checkLists(docIDs []uint64, what string) error {
	st := d.s.st
	for _, docID := range docIDs {
		l := &st.postings.lists[st.side.postingOf[docID]]
		m := l.member(docID)
		if m < 0 {
			return errors.Errorf("doc %d is not in posting %d, where the map has it", docID, st.side.postingOf[docID])
		}
		for _, endpoint := range l.edgesOf(m) {
			if d.deleted[endpoint] {
				return errors.Errorf("STOP: the %s of doc %d gave it an edge to doc %d, which is deleted", what, docID, endpoint)
			}
		}
	}
	return nil
}

// refresh is the refresh pass of cell 4: every vector halfDead lists when it
// starts builds its list of edges again, by search, as the refresh of the
// streaming index does. The search skips the deleted vectors, so no list it
// builds leads to one: that is checked.
func (d *deleteIndex) refresh(ctx context.Context) (refreshStats, error) {
	docIDs := d.halfDead()
	stats, err := d.s.refreshEdges(ctx, docIDs)
	if err != nil {
		return stats, err
	}
	return stats, d.checkLists(docIDs, "refresh")
}

// repair is the first phase of the maintenance pass of v6.1: every live
// vector withDeadEdges lists when it starts builds its list of edges again,
// by search, as the refresh does. While it runs the deleted vectors are
// navigable, as in cell 1b: a deleted member that is still in its posting is
// scored and its edges are followed, and an edge that leads to a deleted
// vector has it scored. None of them is ever a candidate, so none enters a
// list: that is checked. The search goes back to what it was when the phase
// ends.
func (d *deleteIndex) repair(ctx context.Context, num, den int) (refreshStats, error) {
	st := d.s.st
	sources, skip := st.deletedAsSources, st.skipDeletedEndpoints
	st.deletedAsSources, st.skipDeletedEndpoints = true, false
	defer func() { st.deletedAsSources, st.skipDeletedEndpoints = sources, skip }()

	docIDs := d.withDeadEdges(num, den)
	stats, err := d.s.refreshEdges(ctx, docIDs)
	if err != nil {
		return stats, err
	}
	return stats, d.checkLists(docIDs, "repair")
}

// eliminate is the second phase of the maintenance pass of v6.1, over the
// postings there are when it starts, in order. First every posting with a
// deleted member is written again without them. Then every edge that leads
// to a deleted vector is taken out of its list: those of the vectors the
// first phase did not repair. Then the merges: a posting left with fewer
// than mergeUnder members is merged into its nearest one. With sumWithin, it
// is merged only if the two together hold no more than that, and left alone
// otherwise; without it, a posting a merge leaves past the split threshold
// is split, as the cleanup of cell 3 does. A posting that another was merged
// into in this pass is not looked at. When it ends no posting holds a
// deleted vector and no edge leads to one.
func (d *deleteIndex) eliminate(ctx context.Context, mergeUnder, sumWithin int) error {
	s := d.s
	lists := s.st.postings.lists
	postings := len(lists)
	for p := range uint32(postings) {
		if s.retired[p] {
			continue
		}
		if dead := d.dead(p); dead > 0 {
			d.stats.Rewritten++
			d.stats.CopiedLive += len(lists[p].docIDs) - dead
			d.stats.DroppedDead += d.rewrite(p)
		}
	}
	for p := range uint32(postings) {
		if s.retired[p] {
			continue
		}
		l := &s.st.postings.lists[p]
		edges := make([]uint32, 0, len(l.edges))
		starts := make([]uint32, 1, len(l.docIDs)+1)
		for i := range l.docIDs {
			for _, endpoint := range l.edgesOf(i) {
				if d.deleted[endpoint] {
					d.stats.DroppedEdges++
					if d.lost != nil {
						d.lost[l.docIDs[i]]++
					}
					continue
				}
				edges = append(edges, endpoint)
			}
			starts = append(starts, uint32(len(edges)))
		}
		l.edges, l.edgeStart = edges, starts
	}
	d.received = make(map[uint32]bool)
	for p := range uint32(postings) {
		if s.retired[p] || d.received[p] {
			continue
		}
		size := len(s.st.postings.lists[p].docIDs)
		if size >= mergeUnder {
			continue
		}
		if size > 0 && sumWithin > 0 {
			target, err := d.nearestPosting(ctx, p)
			if err != nil {
				return errors.Wrapf(err, "the nearest posting of %d", p)
			}
			if size+len(s.st.postings.lists[target].docIDs) > sumWithin {
				d.stats.MergesSkipped++
				continue
			}
		}
		if err := d.merge(ctx, p); err != nil {
			return errors.Wrapf(err, "merge posting %d", p)
		}
	}
	return d.checkEliminated()
}

// checkEliminated checks what the second phase leaves: no posting holds a
// deleted vector, and no edge leads to one.
func (d *deleteIndex) checkEliminated() error {
	for p := range d.s.st.postings.lists {
		l := &d.s.st.postings.lists[p]
		for _, docID := range l.docIDs {
			if d.deleted[docID] {
				return errors.Errorf("STOP: posting %d still holds doc %d, which is deleted", p, docID)
			}
		}
		for _, endpoint := range l.edges {
			if d.deleted[endpoint] {
				return errors.Errorf("STOP: posting %d still has an edge to doc %d, which is deleted", p, endpoint)
			}
		}
	}
	return nil
}

// deleteCounts is the index as the deletes left it.
type deleteCounts struct {
	Live int `json:"live"`
	// Postings counts the live postings, Members the members that take
	// room in them, the deleted ones included, and LiveMembers the live
	// ones. UnderMin and UnderThreeQuarters count the postings with fewer
	// live members than the minimum of the merges and than three quarters
	// of the target size, and Empty those with none.
	Postings           int `json:"postings"`
	Members            int `json:"members"`
	LiveMembers        int `json:"liveMembers"`
	UnderMin           int `json:"postingsUnderMin"`
	UnderThreeQuarters int `json:"postingsUnderThreeQuarters"`
	Empty              int `json:"postingsEmpty"`
	// Edges counts the edges of the live vectors, DeadEdges those that
	// lead to a deleted vector, and HalfDead the live vectors with half or
	// more of theirs leading to one.
	Edges     int `json:"edges"`
	DeadEdges int `json:"deadEdges"`
	HalfDead  int `json:"vectorsHalfDead"`
}

// counts counts the postings and the edges. target is the posting size the
// index was built for.
func (d *deleteIndex) counts(target int) deleteCounts {
	s := d.s
	out := deleteCounts{Live: s.liveCount}
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		l := &s.st.postings.lists[p]
		live := 0
		for i, docID := range l.docIDs {
			if d.deleted[docID] {
				continue
			}
			live++
			edges := l.edgesOf(i)
			dead := 0
			for _, endpoint := range edges {
				if d.deleted[endpoint] {
					dead++
				}
			}
			out.Edges += len(edges)
			out.DeadEdges += dead
			if len(edges) > 0 && 2*dead >= len(edges) {
				out.HalfDead++
			}
		}
		out.Postings++
		out.Members += len(l.docIDs)
		out.LiveMembers += live
		if live < d.minLive {
			out.UnderMin++
		}
		if 4*live < 3*target {
			out.UnderThreeQuarters++
		}
		if live == 0 {
			out.Empty++
		}
	}
	return out
}

// checkCoverage checks the index with its deletes: every live vector is in
// exactly one posting, the one the map puts it in; a member that is not
// live carries the mark; no vector is in two postings; and a retired posting
// holds nothing and is out of the centroid graph.
func (d *deleteIndex) checkCoverage() error {
	s, st := d.s, d.s.st
	words := st.postings.codeWords
	keys := len(s.live)
	count := make([]uint8, keys)
	if len(s.retired) != len(st.postings.lists) {
		return errors.Errorf("%d postings and %d retired flags", len(st.postings.lists), len(s.retired))
	}
	for p, l := range st.postings.lists {
		n := len(l.docIDs)
		if len(l.edgeStart) != n+1 || len(l.codes) != n*words || int(l.edgeStart[n]) != len(l.edges) {
			return errors.Errorf("posting %d does not hold together: %d members, %d offsets, %d code words, %d edges",
				p, n, len(l.edgeStart), len(l.codes), len(l.edges))
		}
		if s.retired[p] {
			if n > 0 {
				return errors.Errorf("retired posting %d holds %d vectors", p, n)
			}
			if st.centroids.graph.ContainsDoc(uint64(p)) {
				return errors.Errorf("retired posting %d is still in the centroid graph", p)
			}
			continue
		}
		if !st.centroids.graph.ContainsDoc(uint64(p)) {
			return errors.Errorf("live posting %d is not in the centroid graph", p)
		}
		for i, docID := range l.docIDs {
			switch {
			case docID >= uint64(keys):
				return errors.Errorf("posting %d holds doc %d, past the %d keys", p, docID, keys)
			case !s.live[docID] && !d.deleted[docID]:
				return errors.Errorf("posting %d holds doc %d, which is neither live nor marked deleted", p, docID)
			case st.side.postingOf[docID] != uint32(p):
				return errors.Errorf("posting %d holds doc %d, which the map puts in %d", p, docID, st.side.postingOf[docID])
			case st.scanQuantizer == nil && !slices.Equal(l.codes[i*words:(i+1)*words], st.side.code(uint32(docID))):
				return errors.Errorf("doc %d has another code in posting %d than in the sidecar", docID, p)
			}
			if count[docID] < 2 {
				count[docID]++
			}
		}
		for _, endpoint := range l.edges {
			if int(endpoint) >= keys || (!s.live[endpoint] && !d.deleted[endpoint]) {
				return errors.Errorf("posting %d has an edge to doc %d, which was never in the index", p, endpoint)
			}
		}
	}
	live := 0
	for docID, isLive := range s.live {
		switch {
		case isLive && d.deleted[docID]:
			return errors.Errorf("live doc %d is marked deleted", docID)
		case isLive && count[docID] != 1:
			return errors.Errorf("live doc %d is in %d postings", docID, count[docID])
		case count[docID] > 1:
			return errors.Errorf("deleted doc %d is in more than one posting", docID)
		}
		if isLive {
			live++
		}
	}
	if live != s.liveCount {
		return errors.Errorf("%d live docs counted, %d flagged", s.liveCount, live)
	}
	return nil
}
