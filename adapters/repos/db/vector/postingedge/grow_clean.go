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

// The cleanup of the deleted vectors of an index that grows on disk: cell B
// of v6.2 (bench-report.md, "v6.2: reparación en el churn"), which the
// experiments measured in memory. A deleted vector is marked and stays in its
// posting, where it goes on routing (growIndex.remove). A pass takes out for
// good the vectors that were marked when it started:
//
//  1. Every edge of a vector that stays which leads to one that leaves gets a
//     substitute: of the list of the one that leaves, the endpoint nearest
//     the vector, by the distance between their two codes, that is held,
//     outside the posting of the vector and not among its edges already. An
//     edge without one is taken out. No search runs: the lists of those that
//     leave are read once, and the codes are in memory. The substitutes go
//     after the edges that are left, nearest first by that estimate. Their
//     exact distances, which the index keeps for every edge, are computed
//     from the two vectors: the one thing here the experiments did not do.
//     The marked vectors lose their edges to those that leave too, with no
//     substitute.
//  2. Every posting is written again without those that leave: once the first
//     step is done no list leads to them, so a posting can lose its own
//     whenever, and what is on disk between two postings is an index.
//  3. A posting left under mergeUnder members joins the one whose centroid is
//     nearest, if the two together fit in sumWithin.
//
// The steps take one posting after another, and the index is whole between
// two: whoever drives the pass lets searches and inserts in there. A pass
// that stops halfway leaves an index, and the next one starts again.
//
// A member that leaves is taken out of the row of its posting with a
// tombstone, so until the store compacts it away the row holds more values
// than the posting has members, and is read with the tombstones resolved
// (growIndex.fetchMembers).
//
// Only an index with consolidated edges is cleaned: its row of edges is
// written whole. One with a value per list keeps its deleted vectors marked.

// growCleanShare is the share of the vectors of the index that must be
// marked deleted for a pass to be due: the rounds of the churn of v6.2. The
// rule is provisional (integration-plan.md, section 7).
const growCleanShare = 0.03

// growCleanStats counts what the passes over the deleted vectors did.
type growCleanStats struct {
	Passes int `json:"passes"`
	// Removed is the vectors taken out. DeadEdges is the edges of vectors
	// that stay which led to them: Substituted got a substitute and Without
	// did not. Dropped is the edges of marked vectors taken out.
	Removed     int `json:"removed"`
	DeadEdges   int `json:"deadEdges"`
	Substituted int `json:"substituted"`
	Without     int `json:"withoutSubstitute"`
	Dropped     int `json:"droppedFromMarked"`
	// Candidates is the endpoints looked at for a substitute, CodeDistances
	// the distances between two codes, ExactDistances those between two
	// vectors and VectorReads the vectors read for them.
	Candidates     int `json:"candidates"`
	CodeDistances  int `json:"codeDistances"`
	ExactDistances int `json:"exactDistances"`
	VectorReads    int `json:"vectorReads"`
	// What the pass read and wrote: rows of edges, and rows of members.
	RowsRead       int `json:"edgeRowsRead"`
	RowsWritten    int `json:"edgeRowsWritten"`
	MemberRowsRead int `json:"memberRowsRead"`
	DistsRead      int `json:"distsRead"`
	DistsWritten   int `json:"distsWritten"`
	// The merges: postings that joined another, their members, those that
	// did not fit, and the postings retired with no member left.
	Merges        int `json:"merges"`
	Moved         int `json:"movedMembers"`
	MergesSkipped int `json:"mergesSkipped"`
	EmptyRetired  int `json:"emptyRetired"`
}

// wrote counts a write of the index that is not that of a plan: a step of a
// pass that rewrites a posting.
func (g *growIndex) wrote() {
	g.seq++
	g.sinceMerge++
}

// cleanDue tells whether enough vectors are marked deleted for a pass.
func (g *growIndex) cleanDue() bool {
	return g.consolidated() && g.marks != nil && g.deletedCount > 0 &&
		float64(g.deletedCount) >= growCleanShare*float64(g.meta.Live)
}

// readEdges returns the lists of edges of the members of posting p as the
// index holds them now: those of its row with what waits in memory over
// them, in the order of the row and then that of the members that came
// since. old is the row as the store holds it, nil if there is none.
func (g *growIndex) readEdges(p uint32) (old []byte, ids []uint64, lists [][]uint32, err error) {
	if old, err = g.getRow(p); err != nil {
		return nil, nil, nil, errors.Wrapf(err, "read the row of edges of posting %d", p)
	}
	var row growBlock
	if old != nil {
		if err := row.decode(old); err != nil {
			return nil, nil, nil, errors.Wrapf(err, "posting %d", p)
		}
	}
	pend := g.pending[p]
	ids, lists = make([]uint64, 0, len(row.ids)), make([][]uint32, 0, len(row.ids))
	inRow := make(map[uint64]struct{}, len(row.ids))
	for i, id := range row.ids {
		docID := uint64(id)
		inRow[docID] = struct{}{}
		list := row.edges[row.start[i]:row.start[i+1]]
		if pend != nil {
			if waiting, found := pend.lists[docID]; found {
				list = waiting
			}
		}
		ids, lists = append(ids, docID), append(lists, slices.Clone(list))
	}
	if pend != nil {
		for _, docID := range pend.order {
			if _, found := inRow[docID]; !found {
				ids, lists = append(ids, docID), append(lists, slices.Clone(pend.lists[docID]))
			}
		}
	}
	return old, ids, lists, nil
}

// rewriteRow writes the row of edges of posting p whole, over the one the
// store holds, and nothing of the posting waits after it: the distances that
// waited are written first.
func (g *growIndex) rewriteRow(p uint32, old []byte, ids []uint64, lists [][]uint32) error {
	block, err := growEdgesBlock(ids, lists)
	if err != nil {
		return errors.Wrapf(err, "the row of edges of posting %d", p)
	}
	if err := g.putRow(p, old, block); err != nil {
		return errors.Wrapf(err, "write the row of edges of posting %d", p)
	}
	// Its halo too, if too much has been added to it: whoever calls has put
	// in it the codes of the endpoints these lists gained.
	if err := g.trimHalo(p, lists); err != nil {
		return err
	}
	return g.dropPending(p, true)
}

// retirePosting retires a posting that has no member left, or whose members
// were moved: its row of edges and its centroid leave.
func (g *growIndex) retirePosting(p uint32) error {
	if err := g.edgeStore.retire(p); err != nil {
		return err
	}
	if err := g.graphChanges(); err != nil {
		return err
	}
	if err := g.st.centroids.graph.Delete(uint64(p)); err != nil {
		return errors.Wrapf(err, "delete the centroid of posting %d", p)
	}
	g.st.centroids.release(p)
	g.retiredMu.Lock()
	g.retired[p] = true
	g.retiredMu.Unlock()
	g.size[p] = 0
	return nil
}

// nearestPosting returns the live posting whose centroid the centroid graph
// finds nearest to that of p, p aside.
func (g *growIndex) nearestPosting(ctx context.Context, p uint32) (uint32, error) {
	centroids := g.st.centroids
	for limit := 4; limit <= 4096; limit *= 4 {
		centroid, err := centroids.vector(p)
		if err != nil {
			return 0, err
		}
		ids, err := centroids.nearest(ctx, centroid, limit)
		if err != nil {
			return 0, err
		}
		for _, id := range ids {
			if uint32(id) != p && int(id) < len(g.retired) && !g.retired[id] {
				return uint32(id), nil
			}
		}
		if len(ids) < limit {
			break
		}
	}
	return 0, errors.Wrapf(errNoOtherPosting, "next to %d", p)
}

// errNoOtherPosting tells that the centroid graph found no live posting but
// the one it was asked about.
var errNoOtherPosting = errors.New("the centroid graph finds no other live posting")

// growClean is a pass over the deleted vectors of an index.
type growClean struct {
	g                     *growIndex
	mergeUnder, sumWithin int
	// leaving marks the vectors the pass takes out, those marked deleted
	// when it started and not taken out yet, and lists has the list of
	// edges of each as its posting held it then.
	leaving []bool
	lists   map[uint32][]uint32
	// The pass goes through its steps in order, and next is how far the
	// current one is: a doc id while it reads the lists and while it takes
	// the vectors out, a posting otherwise. mergeable is the postings there
	// were when the merges started, and received those another joined.
	phase     int
	next      int
	mergeable int
	received  map[uint32]bool
	stats     growCleanStats

	dp     diskPosting
	raw    []byte
	own    []float32
	target []float32
	// halo and into are the halos of the postings a step is at, where the
	// postings of the index carry one (grow_halo.go).
	halo, into growHalo
}

// code returns the routing code of a doc: the one in memory, or the one read
// from its bucket by an index that keeps none.
func (c *growClean) code(docID uint64) ([]uint64, error) {
	code, read, err := c.g.routingCode(docID)
	if read > 0 {
		c.g.stats.RoutingReads.add(1, read)
	}
	return code, err
}

const (
	cleanGather = iota
	cleanSubstitute
	cleanRemove
	cleanMerge
	cleanDone
)

// beginClean starts a pass over the vectors that are marked deleted now. A
// posting left under mergeUnder members joins its nearest one if the two fit
// in sumWithin, 0 for no merges. The caller holds what a search holds.
func (g *growIndex) beginClean(mergeUnder, sumWithin int) (*growClean, error) {
	switch {
	case g.marks == nil:
		return nil, errors.New("the index was opened without deletes")
	case !g.consolidated():
		return nil, errors.New("the edges of the index are a value per list: its deleted vectors stay marked")
	}
	return &growClean{
		g: g, mergeUnder: mergeUnder, sumWithin: sumWithin, leaving: slices.Clone(g.deleted),
		lists: make(map[uint32][]uint32, g.deletedCount), received: make(map[uint32]bool),
	}, nil
}

// gathers tells whether the pass is still reading the lists of the vectors
// it takes out, which changes nothing: the caller holds what a search holds
// for step. From then on it writes, and nobody else reads or writes the
// index during a step.
func (c *growClean) gathers() bool { return c.phase == cleanGather }

// room has the marks of the pass reach every doc id the index has now.
func (c *growClean) room() {
	for len(c.leaving) < len(c.g.live) {
		c.leaving = append(c.leaving, false)
	}
}

// step takes the pass further by up to the given number of postings, and
// tells whether it is done.
func (c *growClean) step(ctx context.Context, postings int) (done bool, err error) {
	if c.gathers() {
		return c.advanceBy(ctx, postings)
	}
	// What it writes of the postings of a step is one step of the index.
	err = c.g.step(func() error {
		var err error
		done, err = c.advanceBy(ctx, postings)
		return err
	})
	return done && err == nil, err
}

// advanceBy is step, inside the step of the index of those that write.
func (c *growClean) advanceBy(ctx context.Context, postings int) (bool, error) {
	c.room()
	// A step that reads does not go on into those that write: whoever
	// drives the pass holds the index otherwise for them.
	gathering := c.gathers()
	for postings > 0 && c.phase != cleanDone && c.gathers() == gathering {
		var worked bool
		var err error
		switch c.phase {
		case cleanGather:
			worked, err = c.gather()
		case cleanSubstitute:
			worked, err = c.substitute()
		case cleanRemove:
			worked, err = c.remove()
		case cleanMerge:
			worked, err = c.merge(ctx)
		}
		if err != nil {
			return false, err
		}
		if worked {
			postings--
		}
	}
	if c.phase == cleanDone {
		c.stats.Passes = 1
		c.g.cleaned.add(&c.stats)
		c.stats = growCleanStats{}
		return true, nil
	}
	return false, nil
}

// advance moves the pass to its next step.
func (c *growClean) advance() {
	c.phase++
	c.next = 0
	if c.phase == cleanMerge {
		c.mergeable = c.g.meta.Postings
		if c.sumWithin <= 0 {
			c.phase = cleanDone
		}
	}
}

// gather reads the row of edges of the posting of the next vector that
// leaves whose list it has not read, and keeps the lists of all those of
// that posting. It tells whether it read one.
func (c *growClean) gather() (bool, error) {
	g := c.g
	for ; c.next < len(c.leaving); c.next++ {
		docID := uint32(c.next)
		if !c.leaving[docID] {
			continue
		}
		if _, read := c.lists[docID]; read {
			continue
		}
		p := g.st.side.postingOf[docID]
		_, ids, lists, err := g.readEdges(p)
		if err != nil {
			return false, err
		}
		c.stats.RowsRead++
		for i, id := range ids {
			if c.leaving[id] {
				c.lists[uint32(id)] = lists[i]
			}
		}
		if _, read := c.lists[docID]; !read {
			return false, errors.Errorf("doc %d, a member of posting %d, has no list of edges", docID, p)
		}
		return true, nil
	}
	c.advance()
	return false, nil
}

// substitute gives the edges of the next posting that lead to vectors that
// leave their substitutes, and writes its row of edges again if any list
// changed. It tells whether it looked at a posting.
func (c *growClean) substitute() (bool, error) {
	g := c.g
	for ; c.next < g.meta.Postings; c.next++ {
		if !g.retired[c.next] {
			break
		}
	}
	if c.next >= g.meta.Postings {
		c.advance()
		return false, nil
	}
	p := uint32(c.next)
	c.next++
	old, ids, lists, err := g.readEdges(p)
	if err != nil {
		return false, err
	}
	c.stats.RowsRead++
	type change struct {
		docID uint64
		dists []float32
	}
	var changes []change
	// The substitutes of the posting and their codes, for its halo.
	var entered []uint32
	var enteredCodes [][]uint64
	for i, docID := range ids {
		list := lists[i]
		dead := 0
		for _, endpoint := range list {
			if c.leaving[endpoint] {
				dead++
			}
		}
		if dead == 0 {
			continue
		}
		stored, read, err := g.distsOf(p, docID)
		if err != nil {
			return false, err
		}
		if read > 0 {
			c.stats.DistsRead++
		}
		if len(stored) != len(list) {
			return false, errors.Errorf("doc %d has %d edges and %d distances", docID, len(list), len(stored))
		}
		kept, keptDists := make([]uint32, 0, len(list)), make([]float32, 0, len(list))
		for e, endpoint := range list {
			if !c.leaving[endpoint] {
				kept, keptDists = append(kept, endpoint), append(keptDists, stored[e])
			}
		}
		if c.leaving[docID] || g.deleted[docID] {
			// A marked vector: nobody is given it as a neighbor again, and
			// its list is only where substitutes come from.
			c.stats.Dropped += dead
			lists[i] = kept
			changes = append(changes, change{docID, keptDists})
			continue
		}
		c.stats.DeadEdges += dead
		subs, codes, without, err := c.substitutesFor(docID, p, list, kept)
		if err != nil {
			return false, err
		}
		entered, enteredCodes = append(entered, subs...), append(enteredCodes, codes...)
		c.stats.Substituted += len(subs)
		c.stats.Without += without
		if len(subs) > 0 {
			// The distance the index keeps for an edge is the exact one.
			if c.raw, c.own, err = g.exact(docID, c.raw, c.own); err != nil {
				return false, err
			}
			c.stats.VectorReads++
			for _, sub := range subs {
				if c.raw, c.target, err = g.exact(uint64(sub), c.raw, c.target); err != nil {
					return false, err
				}
				dist, err := g.provider.SingleDist(c.own, c.target)
				if err != nil {
					return false, err
				}
				c.stats.VectorReads++
				c.stats.ExactDistances++
				kept, keptDists = append(kept, sub), append(keptDists, dist)
			}
		}
		lists[i] = kept
		changes = append(changes, change{docID, keptDists})
	}
	if len(changes) == 0 {
		return true, nil
	}
	// A write of the index: what a search of an insert kept of this posting
	// before it is not its lists any more.
	g.wrote()
	g.listSeq[p] = g.seq
	if g.haloed() && len(entered) > 0 {
		// The halo of the posting gets the codes of the substitutes it does
		// not have: an endpoint that came from the list of a vector of another
		// posting. Before its row of edges is written, which may have the halo
		// written whole with what these lists need.
		bytes, err := g.fetchHalo(p, &c.halo)
		if err != nil {
			return false, err
		}
		g.stats.HaloReads.add(1, bytes)
		err = g.addHalo(p, &c.halo, entered, func(i int) ([]uint64, error) { return enteredCodes[i], nil })
		if err != nil {
			return false, err
		}
	}
	if err := g.rewriteRow(p, old, ids, lists); err != nil {
		return false, err
	}
	c.stats.RowsWritten++
	for _, changed := range changes {
		if err := g.dists.Put(growDocKey(changed.docID), growDistsValue(changed.dists)); err != nil {
			return false, errors.Wrapf(err, "write the distances of the edges of doc %d", changed.docID)
		}
		c.stats.DistsWritten++
	}
	return true, nil
}

// substitutesFor returns a substitute for each edge of a vector of posting p
// that leads to one that leaves, where there is one, nearest first by the
// distance between two codes: of the list of the one that leaves, the
// endpoint nearest the vector that is held and not marked, is not the vector,
// is outside p, and is neither among the edges the vector keeps nor among
// the substitutes it was just given. codes has the routing code of every
// substitute, and without counts the edges left with none.
func (c *growClean) substitutesFor(docID uint64, p uint32, list, kept []uint32) (subs []uint32, codes [][]uint64,
	without int, err error,
) {
	g := c.g
	side := g.st.side
	own, err := c.code(docID)
	if err != nil {
		return nil, nil, 0, err
	}
	var dists []float32
	for _, endpoint := range list {
		if !c.leaving[endpoint] {
			continue
		}
		candidates, read := c.lists[endpoint]
		if !read {
			return nil, nil, 0, errors.Errorf("the list of edges of doc %d, which leaves, was not read", endpoint)
		}
		var (
			best     uint32
			bestCode []uint64
			bestDist float32
			found    bool
		)
		for _, candidate := range candidates {
			c.stats.Candidates++
			if int(candidate) >= len(g.live) || !g.live[candidate] || g.deleted[candidate] || c.leaving[candidate] ||
				uint64(candidate) == docID || side.postingOf[candidate] == p ||
				slices.Contains(kept, candidate) || slices.Contains(subs, candidate) {
				continue
			}
			code, err := c.code(uint64(candidate))
			if err != nil {
				return nil, nil, 0, err
			}
			dist, err := g.st.quantizer.between(own, code)
			if err != nil {
				return nil, nil, 0, errors.Wrapf(err, "the distance from doc %d to doc %d", docID, candidate)
			}
			c.stats.CodeDistances++
			if !found || dist < bestDist {
				best, bestCode, bestDist, found = candidate, code, dist, true
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
		codes = slices.Insert(codes, at, bestCode)
		dists = slices.Insert(dists, at, bestDist)
	}
	if len(kept)+len(subs) > g.meta.Neighbors {
		return nil, nil, 0, errors.Errorf("STOP: doc %d would have %d edges, more than the %d of a list", docID,
			len(kept)+len(subs), g.meta.Neighbors)
	}
	return subs, codes, without, nil
}

// remove takes out of the posting of the next vector that leaves all those
// of that posting that leave, for good. It tells whether it wrote a posting.
func (c *growClean) remove() (bool, error) {
	g := c.g
	for ; c.next < len(c.leaving); c.next++ {
		if c.leaving[c.next] {
			break
		}
	}
	if c.next >= len(c.leaving) {
		c.advance()
		return false, nil
	}
	p := g.st.side.postingOf[c.next]
	if err := g.fetchMembers(p, &c.dp); err != nil {
		return false, err
	}
	c.stats.MemberRowsRead++
	old, ids, lists, err := g.readEdges(p)
	if err != nil {
		return false, err
	}
	c.stats.RowsRead++
	stayIDs, stayLists := make([]uint64, 0, len(ids)), make([][]uint32, 0, len(ids))
	for i, docID := range ids {
		if c.leaving[docID] {
			continue
		}
		for _, endpoint := range lists[i] {
			if c.leaving[endpoint] {
				return false, errors.Errorf("STOP: doc %d still has an edge to doc %d, which leaves", docID, endpoint)
			}
		}
		stayIDs, stayLists = append(stayIDs, docID), append(stayLists, lists[i])
	}
	// A write of the index that takes vectors out: a search of an insert
	// that ran before it may hold one of them as a neighbor.
	g.wrote()
	g.listSeq[p], g.removedSeq = g.seq, g.seq
	if err := g.rewriteRow(p, old, stayIDs, stayLists); err != nil {
		return false, err
	}
	c.stats.RowsWritten++
	words := g.meta.ScanCodeWords
	for i, docID := range c.dp.docIDs {
		if !c.leaving[docID] {
			continue
		}
		member := growMemberValue(docID, c.dp.codes[i*words:(i+1)*words])
		if err := g.forget(p, docID, member); err != nil {
			return false, err
		}
		c.leaving[docID] = false
		c.stats.Removed++
	}
	if c.leaving[c.next] {
		return false, errors.Errorf("doc %d is not in the row of posting %d, where the map has it", c.next, p)
	}
	if g.size[p] == 0 {
		if err := g.retirePosting(p); err != nil {
			return false, err
		}
		c.stats.EmptyRetired++
	}
	return true, nil
}

// forget takes a marked vector out of the index for good: out of the row of
// members of its posting, of the map, of the routing codes and of the marks,
// with the distances of its edges and the slot of its vector. Its list of
// edges is the caller's to take out of the row of edges, and nothing may
// lead to it any more. Its doc id is free to be given again.
func (g *growIndex) forget(p uint32, docID uint64, member []byte) error {
	key := growDocKey(docID)
	if err := g.postings.SetDeleteSingle(diskKey(p, diskBlockMembers), member); err != nil {
		return errors.Wrapf(err, "take doc %d out of posting %d", docID, p)
	}
	if err := g.posts.Delete(key); err != nil {
		return errors.Wrapf(err, "take doc %d out of the posting map", docID)
	}
	if err := g.routing.Delete(key); err != nil {
		return errors.Wrapf(err, "take out the routing code of doc %d", docID)
	}
	if err := g.dists.Delete(key); err != nil {
		return errors.Wrapf(err, "take out the distances of the edges of doc %d", docID)
	}
	if err := g.marks.Delete(key); err != nil {
		return errors.Wrapf(err, "take out the mark of doc %d", docID)
	}
	delete(g.lost, uint64(docID))
	if g.slots != nil {
		var none [4]byte
		if err := g.writeFile(growTargetSlots, none[:], int64(docID)*4); err != nil {
			return errors.Wrapf(err, "free the slot of the vector of doc %d", docID)
		}
		g.slotOf[docID] = diskNoSlot
	}
	if err := g.setBuilt(docID, 0); err != nil {
		return err
	}
	g.live[docID], g.deleted[docID] = false, false
	g.st.side.postingOf[docID] = unplaced
	g.size[p]--
	g.meta.Live--
	g.deletedCount--
	return nil
}

// merge has the next posting left under mergeUnder members join the one
// whose centroid is nearest, if the two fit in sumWithin: its members go to
// the end of that one, with their lists, and it is retired. A posting another
// joined in this pass is not looked at. It tells whether it merged one.
func (c *growClean) merge(ctx context.Context) (bool, error) {
	g := c.g
	for ; c.next < c.mergeable; c.next++ {
		p := uint32(c.next)
		if g.retired[p] || c.received[p] || int(g.size[p]) >= c.mergeUnder {
			continue
		}
		target, err := g.nearestPosting(ctx, p)
		if errors.Is(err, errNoOtherPosting) {
			// The only posting there is has nobody to join.
			c.stats.MergesSkipped++
			continue
		}
		if err != nil {
			return false, errors.Wrapf(err, "the nearest posting of %d", p)
		}
		if int(g.size[p])+int(g.size[target]) > c.sumWithin {
			c.stats.MergesSkipped++
			continue
		}
		c.next++
		if err := c.join(p, target); err != nil {
			return false, errors.Wrapf(err, "merge posting %d into %d", p, target)
		}
		return true, nil
	}
	c.advance()
	return false, nil
}

// join moves the members of posting p to the end of target, with their codes
// and their lists of edges, and retires p. No edge changes, since they hold
// doc ids, and the centroid of target stays where it was.
func (c *growClean) join(p, target uint32) error {
	g := c.g
	if err := g.fetchMembers(p, &c.dp); err != nil {
		return err
	}
	c.stats.MemberRowsRead++
	_, ids, lists, err := g.readEdges(p)
	if err != nil {
		return err
	}
	old, intoIDs, intoLists, err := g.readEdges(target)
	if err != nil {
		return err
	}
	c.stats.RowsRead += 2
	byDoc := make(map[uint64][]uint32, len(ids))
	for i, docID := range ids {
		byDoc[docID] = lists[i]
	}
	words := g.meta.ScanCodeWords
	members := make([][]byte, len(c.dp.docIDs))
	for i, docID := range c.dp.docIDs {
		list, found := byDoc[docID]
		if !found {
			return errors.Errorf("doc %d, a member of posting %d, has no list of edges", docID, p)
		}
		members[i] = growMemberValue(docID, c.dp.codes[i*words:(i+1)*words])
		intoIDs, intoLists = append(intoIDs, docID), append(intoLists, list)
	}
	// A write of the index: the members of p are in another posting after it.
	g.wrote()
	g.listSeq[p], g.listSeq[target] = g.seq, g.seq
	if g.haloed() {
		// The lists that move lead where they led, and the codes of their
		// endpoints are in the halo of the posting they come from: those the
		// halo of the one that receives them does not have go into it. The
		// halo of p is read while p is a posting, since the store may drop the
		// rows of one that is retired whenever it likes, and the entries go
		// in before the row of the one that receives is written, which may
		// have its halo written whole with what its lists need.
		read, err := g.fetchHalo(p, &c.halo)
		if err != nil {
			return err
		}
		g.stats.HaloReads.add(1, read)
		if read, err = g.fetchHalo(target, &c.into); err != nil {
			return err
		}
		g.stats.HaloReads.add(1, read)
		var endpoints []uint32
		for _, list := range lists {
			endpoints = append(endpoints, list...)
		}
		err = g.addHalo(target, &c.into, endpoints, func(i int) ([]uint64, error) {
			code := c.halo.find(endpoints[i])
			if code == nil {
				return nil, errors.Errorf("STOP: doc %d, where an edge of posting %d leads, is not in its halo", endpoints[i], p)
			}
			return code, nil
		})
		if err != nil {
			return err
		}
	}
	// What waited of p has its distances written and leaves with its row.
	if err := g.retirePosting(p); err != nil {
		return err
	}
	if err := g.postings.SetAdd(diskKey(target, diskBlockMembers), members); err != nil {
		return errors.Wrapf(err, "write the members of posting %d", target)
	}
	if err := g.rewriteRow(target, old, intoIDs, intoLists); err != nil {
		return err
	}
	c.stats.RowsWritten++
	for _, docID := range c.dp.docIDs {
		if err := g.posts.Put(growDocKey(docID), growUint32Value(target)); err != nil {
			return errors.Wrap(err, "write the posting map")
		}
		g.st.side.postingOf[docID] = target
	}
	g.size[target] += uint32(len(c.dp.docIDs))
	g.meta.Merges++
	c.received[target] = true
	c.stats.Merges++
	c.stats.Moved += len(c.dp.docIDs)
	return nil
}

func (s *growCleanStats) add(o *growCleanStats) {
	s.Passes += o.Passes
	s.Removed += o.Removed
	s.DeadEdges += o.DeadEdges
	s.Substituted += o.Substituted
	s.Without += o.Without
	s.Dropped += o.Dropped
	s.Candidates += o.Candidates
	s.CodeDistances += o.CodeDistances
	s.ExactDistances += o.ExactDistances
	s.VectorReads += o.VectorReads
	s.RowsRead += o.RowsRead
	s.RowsWritten += o.RowsWritten
	s.MemberRowsRead += o.MemberRowsRead
	s.DistsRead += o.DistsRead
	s.DistsWritten += o.DistsWritten
	s.Merges += o.Merges
	s.Moved += o.Moved
	s.MergesSkipped += o.MergesSkipped
	s.EmptyRetired += o.EmptyRetired
}

// clean runs a pass over the vectors marked deleted with nothing running next
// to it: what the experiments and the tests call.
func (g *growIndex) clean(ctx context.Context, mergeUnder, sumWithin int) error {
	pass, err := g.beginClean(mergeUnder, sumWithin)
	if err != nil {
		return err
	}
	for done := false; !done; {
		if done, err = pass.step(ctx, 1<<20); err != nil {
			return err
		}
	}
	return nil
}
