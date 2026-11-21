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
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"

	"github.com/pkg/errors"
)

// The refresh of the edges of an index that grows on disk, on doubling
// (bench-report.md, "v2.4: refresco al doblar, en línea"), which the
// experiments measured in memory. The edges of a vector are those of the
// neighbors it had when they were built: by the batch build of the base, by
// its insert, or by its last refresh. As the index grows, nearer vectors come
// in that it has no edge to but for the ones that entered its list as inverse
// edges. Every time the index doubles, a pass builds again the edges of the
// vectors whose edges were built when the index held half of what it holds
// now, or less.
//
// Refreshing a vector repeats the edge step of its insert: the search for its
// neighbors, which enters through its own posting and only reads, and then
// its writes, its list and its place in the lists of its neighbors. The
// vector stays where it is.
//
// The index keeps, by doc id, how many vectors it held when the edges of
// each were last built: in memory, and in a file of its directory that a
// vector writes its entry of.

// growBuiltFile holds, for every doc id, how many vectors the index held
// when its edges were last built, 0 for a doc id it does not hold.
const growBuiltFile = "built.u32"

// growRefreshStats counts what the refreshes of edges did.
type growRefreshStats struct {
	Passes    int `json:"passes"`
	Refreshed int `json:"refreshed"`
	// SelfMissing counts the vectors their own search did not return.
	SelfMissing int `json:"selfMissing"`
	// ListsChanged counts the vectors whose list is not the one they had, and
	// EdgesBefore, EdgesAfter and EdgesKept the edges of all the refreshed
	// vectors before, after, and in both lists.
	ListsChanged int `json:"listsChanged"`
	EdgesBefore  int `json:"edgesBefore"`
	EdgesAfter   int `json:"edgesAfter"`
	EdgesKept    int `json:"edgesKept"`
	// The work of their searches and of their inverse edges, counted as the
	// inserts count theirs and apart from them.
	SearchLoads   int `json:"searchLoads"`
	SearchRounds  int `json:"searchRounds"`
	VectorReads   int `json:"vectorReads"`
	InverseChecks int `json:"inverseChecks"`
	InverseEdges  int `json:"inverseEdges"`
}

// held is how many vectors the index holds that are not marked deleted.
func (g *growIndex) held() int {
	return g.meta.Live - g.deletedCount
}

// writeBuilt writes the file of when the edges of every vector were built,
// whole.
func writeBuilt(path string, builtAt []uint32) error {
	raw := make([]byte, 4*len(builtAt))
	for docID, built := range builtAt {
		binary.LittleEndian.PutUint32(raw[4*docID:], built)
	}
	return os.WriteFile(path, raw, 0o644)
}

// openBuilt reads when the edges of every vector were built, and opens the
// file to be written. An index that was written without it gets one in which
// every vector it holds has edges of now: it does not know better, and the
// next doubling is counted from here.
func (g *growIndex) openBuilt() error {
	path := filepath.Join(g.dir, growBuiltFile)
	builtAt := make([]uint32, g.meta.Keys)
	raw, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		raw = nil
	case err != nil:
		return errors.Wrap(err, "read when the edges were built")
	}
	for docID := range min(len(raw)/4, len(builtAt)) {
		builtAt[docID] = binary.LittleEndian.Uint32(raw[4*docID:])
	}
	unknown := false
	for docID, held := range g.live {
		switch {
		case !held:
			builtAt[docID] = 0
		case builtAt[docID] == 0:
			builtAt[docID] = uint32(g.meta.Live)
			unknown = true
		}
	}
	if raw == nil || unknown {
		if err := writeBuilt(path, builtAt); err != nil {
			return errors.Wrap(err, "write when the edges were built")
		}
	}
	if g.meta.RefreshedAt == 0 {
		g.meta.RefreshedAt = g.meta.Live
	}
	if g.built, err = os.OpenFile(path, os.O_RDWR, 0o644); err != nil {
		return errors.Wrap(err, "open when the edges were built")
	}
	g.builtAt = builtAt
	return nil
}

// setBuilt records that the edges of a vector were built now, with the
// vectors the index holds, or with none for a doc id that left.
func (g *growIndex) setBuilt(docID uint64, held int) error {
	var entry [4]byte
	binary.LittleEndian.PutUint32(entry[:], uint32(held))
	if _, err := g.built.WriteAt(entry[:], int64(docID)*4); err != nil {
		return errors.Wrapf(err, "write when the edges of doc %d were built", docID)
	}
	g.builtAt[docID] = uint32(held)
	return nil
}

// refreshDue tells whether the index holds twice the vectors it held when
// the last refresh of its edges started, or when it was written.
func (g *growIndex) refreshDue() bool {
	return g.built != nil && g.held() >= 2*max(g.meta.RefreshedAt, 1)
}

// stale tells whether the edges of a vector were built when the index held
// half the given number of vectors, or fewer.
func (g *growIndex) stale(docID uint64, held int) bool {
	return g.holds(docID) && 2*int(g.builtAt[docID]) <= held
}

// staleDocs lists the vectors a refresh takes now, by doc id.
func (g *growIndex) staleDocs() []uint64 {
	held := g.held()
	var stale []uint64
	for docID := range g.live {
		if g.stale(uint64(docID), held) {
			stale = append(stale, uint64(docID))
		}
	}
	return stale
}

// planRefresh is the part of the refresh of a vector that writes nothing:
// the search for its neighbors, one more than an insert asks for since it
// finds the vector itself, entering through its own posting. It leaves in the
// plan what the search of an insert leaves. It can run next to other
// searches, as long as nothing is being written.
func (g *growIndex) planRefresh(ctx context.Context, plan *growPlan, docID uint64) error {
	if !g.holds(docID) {
		return errors.Errorf("doc %d is not in the index", docID)
	}
	_, vector, err := g.exact(docID, nil, nil)
	if err != nil {
		return err
	}
	plan.docID, plan.vector = docID, vector
	plan.kept = plan.kept[:0]
	clear(plan.keptAt)
	clear(plan.captured)
	plan.vectorReads = 1
	plan.entry = []uint64{uint64(g.st.side.postingOf[docID])}
	plan.assignTime = 0

	search := g.searcher()
	search.loaded = plan.keep
	search.ef = g.insertEF
	plan.searched = diskSearchStats{}
	plan.neighbors, plan.dists, err = search.run(context.WithValue(ctx, growPlanKey{}, plan), vector, g.meta.Neighbors+1,
		plan.entry, &plan.searched)
	if err != nil {
		return errors.Wrapf(err, "search the neighbors of doc %d", docID)
	}
	plan.selfMissing = true
	if at := slices.Index(plan.neighbors, docID); at >= 0 {
		plan.neighbors, plan.dists = slices.Delete(plan.neighbors, at, at+1), slices.Delete(plan.dists, at, at+1)
		plan.selfMissing = false
	}
	keep := min(len(plan.neighbors), g.meta.Neighbors)
	plan.neighbors, plan.dists = plan.neighbors[:keep], plan.dists[:keep]
	return nil
}

// applyRefresh is the part of the refresh of a vector that writes: its list
// becomes what an insert writes, of the neighbors its search found those
// outside its posting, and it takes its place in the edges of those
// neighbors as a new vector does, except where it has one already. The
// vector stays where it is. A vector that left the index since its search is
// left alone.
func (g *growIndex) applyRefresh(plan *growPlan) error {
	docID, vector := plan.docID, plan.vector
	if !g.holds(docID) {
		return nil
	}
	// The posting of the vector as it is now: a split may have moved it
	// since its search.
	p := g.st.side.postingOf[docID]
	g.kept, g.keptAt, g.captured = plan.kept, plan.keptAt, plan.captured
	defer func() { plan.kept = g.kept }()
	g.touched = g.touched[:0]
	// The inverse edges of a refresh are counted apart from those of the
	// inserts.
	out := &g.refreshed
	checks, entered := g.stats.InverseChecks, g.stats.InverseEdges
	defer func() {
		out.InverseChecks += g.stats.InverseChecks - checks
		out.InverseEdges += g.stats.InverseEdges - entered
		g.stats.InverseChecks, g.stats.InverseEdges = checks, entered
	}()
	out.SearchLoads += plan.searched.Postings
	out.SearchRounds += plan.searched.Rounds
	out.VectorReads += plan.vectorReads
	if plan.selfMissing {
		out.SelfMissing++
	}

	// A neighbor that was marked deleted or left since the search is not one.
	neighbors, dists := plan.neighbors[:0:0], plan.dists[:0:0]
	for i, u := range plan.neighbors {
		if g.holds(u) {
			neighbors, dists = append(neighbors, u), append(dists, plan.dists[i])
		}
	}
	edges := make([]uint32, 0, len(neighbors))
	edgeDists := make([]float32, 0, len(neighbors))
	for _, u := range neighbors {
		if g.st.side.postingOf[u] == p {
			continue
		}
		to, err := g.vectorOfNeighbor(u)
		if err != nil {
			return err
		}
		dist, err := g.provider.SingleDist(vector, to)
		if err != nil {
			return err
		}
		edges, edgeDists = append(edges, uint32(u)), append(edgeDists, dist)
	}
	old, err := g.listOf(p, docID)
	if err != nil {
		return err
	}
	old = slices.Clone(old)
	out.EdgesBefore += len(old)
	out.EdgesAfter += len(edges)
	for _, endpoint := range edges {
		if slices.Contains(old, endpoint) {
			out.EdgesKept++
		}
	}
	if !slices.Equal(old, edges) {
		if err := g.edgeStore.replace(p, docID, old, edges, edgeDists); err != nil {
			return err
		}
		// What the refresh kept of the posting is no longer its list.
		delete(g.keptAt, p)
		out.ListsChanged++
	}
	if err := g.setBuilt(docID, g.held()); err != nil {
		return err
	}
	if err := g.enterNeighbors(docID, vector, p, neighbors, dists); err != nil {
		return err
	}
	if g.consolidated() {
		if err := g.edgeStore.settle(); err != nil {
			return err
		}
	}
	out.Refreshed++
	return nil
}

// refreshOne refreshes the edges of one vector, its search and then its
// writes, with nothing running next to it.
func (g *growIndex) refreshOne(ctx context.Context, docID uint64) error {
	plan := g.own
	plan.work = nil
	if err := g.planRefresh(ctx, plan, docID); err != nil {
		return err
	}
	return g.applyRefresh(plan)
}

// growRefresh is a pass that refreshes the edges of the vectors that were
// stale when it started.
type growRefresh struct {
	g *growIndex
	// held is the vectors the index held when the pass started: a vector is
	// refreshed if its edges were built with half of them or fewer.
	held int
	next int
}

// beginRefresh starts a pass. The caller holds what a search holds.
func (g *growIndex) beginRefresh() (*growRefresh, error) {
	if g.built == nil {
		return nil, errors.New("the index does not keep when the edges of its vectors were built")
	}
	return &growRefresh{g: g, held: g.held()}, nil
}

// nextStale returns the next vector the pass refreshes, and false when there
// is none left. The caller holds what a search holds.
func (r *growRefresh) nextStale() (uint64, bool) {
	g := r.g
	for ; r.next < len(g.live); r.next++ {
		if g.stale(uint64(r.next), r.held) {
			docID := uint64(r.next)
			r.next++
			return docID, true
		}
	}
	return 0, false
}

// finish ends the pass: the next one is due when the index holds twice what
// it held when this one started. Nobody else writes the index meanwhile.
func (r *growRefresh) finish() {
	r.g.meta.RefreshedAt = r.held
	r.g.refreshed.Passes++
}

// refreshStale refreshes every vector that is stale now, one after another by
// doc id, with nothing running next to it: what the experiments and the
// tests call.
func (g *growIndex) refreshStale(ctx context.Context) error {
	pass, err := g.beginRefresh()
	if err != nil {
		return err
	}
	for {
		docID, found := pass.nextStale()
		if !found {
			break
		}
		if err := g.refreshOne(ctx, docID); err != nil {
			return errors.Wrapf(err, "refresh the edges of doc %d", docID)
		}
	}
	pass.finish()
	return nil
}
