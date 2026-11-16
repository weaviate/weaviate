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
	"math"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// The streaming experiment of v2.0 (bench-report.md, "v2.0: streaming en
// memoria"). An index is built in batch over part of a corpus, converted to
// the representation keyed by doc id, and grown by inserting the rest one
// vector at a time:
//
//   - a new vector goes to the posting of the nearest centroid the centroid
//     graph finds, which need not be the nearest one;
//   - its edges come from a search of the index for it: of the neighbors it
//     finds, those outside the vector's posting, as the batch builder keeps
//     them;
//   - a posting that grows past a threshold splits in two with the 2-means of
//     the batch partition, and only the posting map changes: no edge does.
//
// Two mechanisms can be switched on, the controls of the experiment: inverse
// edges, where a new vector enters the edges of a neighbor whose worst edge
// it beats, and edge repair, where the members of a split posting search
// their edges again.
//
// Nothing searches while an insert runs, so an insert is complete, posting
// and map written, before any search can see the vector. checkCoverage
// checks that every vector is where the map says, and only there.

// unplaced is the posting of a doc id that is in no posting.
const unplaced = math.MaxUint32

type streamConfig struct {
	// threshold is the size past which a posting splits.
	threshold int
	// inverse lets a new vector into the edges of its neighbors (H1).
	inverse bool
	// repair has the members of a split posting search their edges again
	// (H2).
	repair bool
	// siblings has the members of either half of a split take their nearest
	// neighbors in the other half, by brute force over the old posting's
	// members, without a search (cell 5).
	siblings bool
	// assignOnly assigns and splits and does nothing else: no search and no
	// edges. It is how the threshold is calibrated.
	assignOnly bool
	// seed seeds the 2-means of the splits.
	seed uint64
}

// streamStats counts what the inserts did. The times are indicative: they
// are taken around each phase of every insert.
type streamStats struct {
	Inserted int `json:"inserted"`
	// NonNearest counts the inserts whose posting was not that of the
	// nearest live centroid.
	NonNearest int `json:"nonNearest"`
	Splits     int `json:"splits"`
	// SplitsFailed counts the splits that would have left one side empty.
	// The posting stays as it is and tries again on its next insert.
	SplitsFailed int `json:"splitsFailed"`
	// InverseEdges counts the edges new vectors took in their neighbors'
	// lists.
	InverseEdges int `json:"inverseEdges"`
	// Repaired counts the vectors whose edges a split had them search again.
	Repaired int `json:"repaired"`
	// SiblingEdges counts the edges the members of split postings took into
	// the other half, and SiblingDistances the exact distances that took.
	SiblingEdges     int `json:"siblingEdges"`
	SiblingDistances int `json:"siblingDistances"`

	AssignTime  time.Duration `json:"assignNs"`
	NearestTime time.Duration `json:"nearestCheckNs"` // only the report needs it
	EdgeTime    time.Duration `json:"edgeSearchNs"`
	WriteTime   time.Duration `json:"writeNs"`
	InverseTime time.Duration `json:"inverseNs"`
	SplitTime   time.Duration `json:"splitNs"`
	RepairTime  time.Duration `json:"repairNs"`
	SiblingTime time.Duration `json:"siblingNs"`
}

// streamIndex grows a built index by inserts.
type streamIndex struct {
	ix  *Index
	st  *state
	cfg streamConfig
	// vectorOf returns the full-precision vector of a doc id, normalized as
	// the index stores it.
	vectorOf func(docID uint64) []float32
	// neighbors is the number of nearest neighbors an edge search keeps,
	// the batch builder's.
	neighbors int
	live      []bool // by doc id
	liveCount int
	retired   []bool // by posting id: split, and out of the centroid graph
	// capacity is how many postings the scratch of a search holds.
	capacity int
	stats    streamStats
	sums     []float64
	l2       distancer.Provider
	// onWrite, when set, is called once an insert has written its vector,
	// with its posting and edges, and before anything else changes: no
	// inverse edge, no split. It is outside the times of the insert.
	onWrite func(docID uint64, p uint32, edges []uint32) error
}

// newStreamIndex takes over a built index: its state is converted to the
// representation keyed by doc id, for the doc ids [0, keys). The centroid
// graph is the build's, and grows with the splits. The index searches the
// same before and after.
func newStreamIndex(ix *Index, keys int, cfg streamConfig, vectorOf func(uint64) []float32) (*streamIndex, error) {
	batch := ix.state.Load()
	switch {
	case batch == nil:
		return nil, ErrNotBuilt
	case batch.postings.lists != nil:
		return nil, errors.New("the index is keyed by doc id already")
	case batch.scanQuantizer != nil:
		return nil, errors.New("postings with codes of their own are not supported")
	case cfg.threshold < 2:
		return nil, errors.Errorf("a split threshold of %d leaves nothing to split", cfg.threshold)
	}
	store, words := batch.postings, batch.postings.codeWords
	side := &sidecar{codeWords: words, codes: make([]uint64, keys*words), postingOf: make([]uint32, keys)}
	for i := range side.postingOf {
		side.postingOf[i] = unplaced
	}
	s := &streamIndex{
		ix: ix, cfg: cfg, vectorOf: vectorOf, neighbors: ix.uc.Edges,
		live: make([]bool, keys), sums: make([]float64, batch.dims), l2: distancer.NewL2SquaredProvider(),
	}

	lists := make([]postingList, store.postings())
	for p := range lists {
		from, to := store.start[p], store.start[p+1]
		lists[p] = newPostingList()
		for slot := from; slot < to; slot++ {
			docID := store.docIDs[slot]
			if docID >= uint64(keys) {
				return nil, errors.Errorf("doc id %d is past the %d keys", docID, keys)
			}
			edges := store.edgesOf(slot)
			keyed := make([]uint32, len(edges))
			for e, endpoint := range edges {
				keyed[e] = uint32(store.docIDs[endpoint])
			}
			code := batch.side.code(slot)
			lists[p].append(docID, code, keyed)
			copy(side.codes[int(docID)*words:], code)
			side.postingOf[docID] = uint32(p)
			s.live[docID] = true
			s.liveCount++
		}
	}
	s.retired = make([]bool, len(lists))
	// Every split adds two postings, and there is at most one split per
	// insert.
	s.capacity = len(lists) + 2*keys

	st := &state{
		dims:      batch.dims,
		quantizer: batch.quantizer,
		centroids: batch.centroids,
		postings:  &postingStore{codeWords: words, lists: lists},
		side:      side,
		visited:   visited.NewPool(keys),
		build:     batch.build,
	}
	capacity := s.capacity
	st.scratch.New = func() any { return newSearchScratch(capacity) }
	s.st = st
	ix.state.Store(st)
	return s, nil
}

func newPostingList() postingList {
	return postingList{edgeStart: []uint32{0}}
}

func (l *postingList) append(docID uint64, code []uint64, edges []uint32) {
	l.docIDs = append(l.docIDs, docID)
	l.codes = append(l.codes, code...)
	l.edges = append(l.edges, edges...)
	l.edgeStart = append(l.edgeStart, uint32(len(l.edges)))
}

func (l *postingList) edgesOf(i int) []uint32 {
	return l.edges[l.edgeStart[i]:l.edgeStart[i+1]]
}

// member returns the position of a doc id in the posting, -1 if it is not in
// it.
func (l *postingList) member(docID uint64) int {
	return slices.Index(l.docIDs, docID)
}

// setEdges replaces the edges of member i.
func (l *postingList) setEdges(i int, edges []uint32) {
	from, to := l.edgeStart[i], l.edgeStart[i+1]
	if int(to-from) == len(edges) {
		copy(l.edges[from:to], edges)
		return
	}
	rest := slices.Clone(l.edges[to:])
	l.edges = append(append(l.edges[:from], edges...), rest...)
	delta := int64(len(edges)) - int64(to-from)
	for j := i + 1; j < len(l.edgeStart); j++ {
		l.edgeStart[j] = uint32(int64(l.edgeStart[j]) + delta)
	}
}

// insert adds one vector by the incremental path. The vector must be
// normalized as the index stores it. The search parameters of the edge
// search are the index's own: the caller sets them.
func (s *streamIndex) insert(ctx context.Context, docID uint64, vector []float32) error {
	if docID >= uint64(len(s.live)) {
		return errors.Errorf("doc id %d is past the %d keys", docID, len(s.live))
	}
	if s.live[docID] {
		return errors.Errorf("doc %d is in the index already", docID)
	}
	st := s.st

	clock := time.Now()
	entry, err := st.centroids.nearest(ctx, vector, 1)
	if err != nil {
		return err
	}
	if len(entry) == 0 {
		return errors.New("the centroid graph found no centroid")
	}
	p := uint32(entry[0])
	if int(p) >= len(s.retired) || s.retired[p] {
		return errors.Errorf("the centroid graph returned posting %d, which is not live", p)
	}
	s.stats.AssignTime += time.Since(clock)

	clock = time.Now()
	if nearest, err := s.nearestCentroid(vector); err != nil {
		return err
	} else if nearest != p {
		s.stats.NonNearest++
	}
	s.stats.NearestTime += time.Since(clock)

	var neighbors []uint64
	var dists []float32
	if !s.cfg.assignOnly {
		clock = time.Now()
		neighbors, dists, err = s.ix.search(ctx, vector, s.neighbors, nil, nil)
		if err != nil {
			return errors.Wrapf(err, "search the neighbors of doc %d", docID)
		}
		s.stats.EdgeTime += time.Since(clock)
	}

	clock = time.Now()
	code := st.quantizer.Encode(vector)
	words := st.side.codeWords
	copy(st.side.codes[int(docID)*words:(int(docID)+1)*words], code)
	edges := s.outside(neighbors, p)
	st.postings.lists[p].append(docID, code, edges)
	st.side.postingOf[docID] = p
	s.live[docID] = true
	s.liveCount++
	s.stats.Inserted++
	s.stats.WriteTime += time.Since(clock)

	if s.onWrite != nil {
		if err := s.onWrite(docID, p, edges); err != nil {
			return err
		}
	}

	if s.cfg.inverse {
		clock = time.Now()
		if err := s.enterNeighbors(docID, p, neighbors, dists); err != nil {
			return err
		}
		s.stats.InverseTime += time.Since(clock)
	}
	if len(st.postings.lists[p].docIDs) > s.cfg.threshold {
		if err := s.split(ctx, p); err != nil {
			return errors.Wrapf(err, "split posting %d", p)
		}
	}
	return nil
}

// outside keeps the doc ids that are not in posting p, in order: the edges of
// a vector in p.
func (s *streamIndex) outside(docIDs []uint64, p uint32) []uint32 {
	edges := make([]uint32, 0, len(docIDs))
	for _, id := range docIDs {
		if s.st.side.postingOf[id] != p {
			edges = append(edges, uint32(id))
		}
	}
	return edges
}

// nearestCentroid returns the live centroid nearest to the vector, by brute
// force over all of them.
func (s *streamIndex) nearestCentroid(vector []float32) (uint32, error) {
	best, bestDist := uint32(unplaced), float32(math.Inf(1))
	for p, centroid := range s.st.centroids.vectors {
		if s.retired[p] {
			continue
		}
		dist, err := s.l2.SingleDist(vector, centroid)
		if err != nil {
			return 0, err
		}
		if dist < bestDist {
			best, bestDist = uint32(p), dist
		}
	}
	return best, nil
}

// enterNeighbors gives the new vector, in posting p, a place in the edges of
// each of its neighbors outside p whose worst edge it beats: it takes the
// place its distance gives it, the list stays nearest first, and the worst
// edge goes. A neighbor without edges takes none. dists are the exact
// distances from the new vector to its neighbors.
func (s *streamIndex) enterNeighbors(docID uint64, p uint32, neighbors []uint64, dists []float32) error {
	for i, u := range neighbors {
		q := s.st.side.postingOf[u]
		if q == p {
			continue
		}
		l := &s.st.postings.lists[q]
		m := l.member(u)
		if m < 0 {
			return errors.Errorf("doc %d is not in posting %d, where the map has it", u, q)
		}
		edges := l.edgesOf(m)
		if len(edges) == 0 {
			continue
		}
		from := s.vectorOf(u)
		edgeDists := make([]float32, len(edges))
		for e, endpoint := range edges {
			dist, err := s.ix.provider.SingleDist(from, s.vectorOf(uint64(endpoint)))
			if err != nil {
				return err
			}
			edgeDists[e] = dist
		}
		if dists[i] >= edgeDists[len(edges)-1] {
			continue
		}
		at := len(edges) - 1
		for at > 0 && edgeDists[at-1] > dists[i] {
			at--
		}
		updated := make([]uint32, 0, len(edges))
		updated = append(updated, edges[:at]...)
		updated = append(updated, uint32(docID))
		updated = append(updated, edges[at:len(edges)-1]...)
		l.setEdges(m, updated)
		s.stats.InverseEdges++
	}
	return nil
}

// split parts posting p in two with the 2-means of the batch partition over
// its members' full vectors. The two means become two new postings and
// centroids, every member goes to the nearer of them, and p is retired: its
// centroid leaves the graph, its list is left empty. Edges stay as they are,
// unless the members repair theirs.
func (s *streamIndex) split(ctx context.Context, p uint32) error {
	clock := time.Now()
	st := s.st
	old := st.postings.lists[p]
	n := len(old.docIDs)
	vectors := make([][]float32, n)
	members := make([]uint32, n)
	for i, docID := range old.docIDs {
		vectors[i] = s.vectorOf(docID)
		members[i] = uint32(i)
	}
	left, right, err := bisect(vectors, st.dims, cluster{members: members, leaves: 2, path: 1},
		s.cfg.seed^uint64(s.stats.Splits+s.stats.SplitsFailed))
	if err != nil {
		// A 2-means that leaves one side empty is counted, not fatal: the
		// posting tries again on its next insert.
		s.stats.SplitsFailed++
		s.stats.SplitTime += time.Since(clock)
		return nil
	}
	means := [2][]float32{mean(vectors, left.members, s.sums), mean(vectors, right.members, s.sums)}
	sideOf := make([]int, n)
	sizes := [2]int{}
	for i, vec := range vectors {
		da, err := s.l2.SingleDist(vec, means[0])
		if err != nil {
			return err
		}
		db, err := s.l2.SingleDist(vec, means[1])
		if err != nil {
			return err
		}
		if db < da {
			sideOf[i] = 1
		}
		sizes[sideOf[i]]++
	}
	if sizes[0] == 0 || sizes[1] == 0 {
		s.stats.SplitsFailed++
		s.stats.SplitTime += time.Since(clock)
		return nil
	}

	first := uint32(len(st.postings.lists))
	if int(first)+2 > s.capacity {
		return errors.Errorf("posting %d would pass the capacity of %d postings", first+1, s.capacity)
	}
	children := [2]postingList{newPostingList(), newPostingList()}
	words := st.side.codeWords
	for i, docID := range old.docIDs {
		children[sideOf[i]].append(docID, old.codes[i*words:(i+1)*words], old.edgesOf(i))
		st.side.postingOf[docID] = first + uint32(sideOf[i])
	}
	st.postings.lists = append(st.postings.lists, children[0], children[1])
	st.postings.lists[p] = newPostingList()
	s.retired = append(s.retired, false, false)
	s.retired[p] = true

	centroids := st.centroids
	centroids.vectors = append(centroids.vectors, means[0], means[1])
	for c := range 2 {
		if err := centroids.graph.Add(ctx, uint64(first)+uint64(c), means[c]); err != nil {
			return errors.Wrap(err, "add a centroid")
		}
	}
	if err := centroids.graph.Delete(uint64(p)); err != nil {
		return errors.Wrap(err, "delete the centroid")
	}
	s.stats.Splits++
	s.stats.SplitTime += time.Since(clock)

	if s.cfg.repair && !s.cfg.assignOnly {
		clock = time.Now()
		for c := range uint32(2) {
			if err := s.repairEdges(ctx, first+c); err != nil {
				return err
			}
		}
		s.stats.RepairTime += time.Since(clock)
	}
	if s.cfg.siblings && !s.cfg.assignOnly {
		clock = time.Now()
		if err := s.linkSiblings(first, first+1); err != nil {
			return err
		}
		s.stats.SiblingTime += time.Since(clock)
	}
	return nil
}

// linkSiblings gives every member of either half of a split its nearest
// neighbors in the other half. They were posting mates, so the old posting's
// member list holds them all and no search is needed: the exact distances
// between the halves are computed once, the member's current edges get
// theirs, and of both the nearest are kept, nearest first. Current edges
// lead out of the old posting, so none of them is in the other half.
func (s *streamIndex) linkSiblings(a, b uint32) error {
	lists := s.st.postings.lists
	halves := [2]*postingList{&lists[a], &lists[b]}
	na, nb := len(halves[0].docIDs), len(halves[1].docIDs)
	across := make([]float32, na*nb)
	for i, x := range halves[0].docIDs {
		from := s.vectorOf(x)
		for j, y := range halves[1].docIDs {
			dist, err := s.ix.provider.SingleDist(from, s.vectorOf(y))
			if err != nil {
				return err
			}
			across[i*nb+j] = dist
		}
	}
	s.stats.SiblingDistances += na * nb

	ids := [2]uint32{a, b}
	var nearest topK
	for h := range 2 {
		l, other := halves[h], halves[1-h]
		for i, docID := range l.docIDs {
			nearest.reset(s.neighbors)
			from := s.vectorOf(docID)
			for _, endpoint := range l.edgesOf(i) {
				dist, err := s.ix.provider.SingleDist(from, s.vectorOf(uint64(endpoint)))
				if err != nil {
					return err
				}
				nearest.push(uint64(endpoint), dist)
				s.stats.SiblingDistances++
			}
			for j, sibling := range other.docIDs {
				if h == 0 {
					nearest.push(sibling, across[i*nb+j])
				} else {
					nearest.push(sibling, across[j*nb+i])
				}
			}
			kept := nearest.sorted()
			edges := make([]uint32, len(kept))
			for e, near := range kept {
				edges[e] = uint32(near.id)
				if s.st.side.postingOf[near.id] == ids[1-h] {
					s.stats.SiblingEdges++
				}
			}
			l.setEdges(i, edges)
		}
	}
	return nil
}

// repairEdges has every member of posting q search its edges again, as an
// insert does: of the nearest neighbors a search finds, itself left out, those
// outside q.
func (s *streamIndex) repairEdges(ctx context.Context, q uint32) error {
	l := &s.st.postings.lists[q]
	for i, docID := range l.docIDs {
		ids, _, err := s.ix.search(ctx, s.vectorOf(docID), s.neighbors+1, nil, nil)
		if err != nil {
			return errors.Wrapf(err, "search the neighbors of doc %d", docID)
		}
		if at := slices.Index(ids, docID); at >= 0 {
			ids = slices.Delete(ids, at, at+1)
		}
		l.setEdges(i, s.outside(ids[:min(len(ids), s.neighbors)], q))
		s.stats.Repaired++
	}
	return nil
}

// checkCoverage checks the invariant of the inserts: every live vector is in
// exactly one posting, the one the map gives it, with its code; no posting
// holds anything else; every edge leads to a live vector outside its own
// posting; and the centroid graph holds the live postings and none of the
// retired ones.
func (s *streamIndex) checkCoverage() error {
	st := s.st
	words := st.side.codeWords
	count := make([]uint8, len(s.live))
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
			if docID >= uint64(len(s.live)) || !s.live[docID] {
				return errors.Errorf("posting %d holds doc %d, which is not live", p, docID)
			}
			if st.side.postingOf[docID] != uint32(p) {
				return errors.Errorf("posting %d holds doc %d, which the map puts in %d", p, docID, st.side.postingOf[docID])
			}
			if !slices.Equal(l.codes[i*words:(i+1)*words], st.side.code(uint32(docID))) {
				return errors.Errorf("doc %d has another code in posting %d than in the sidecar", docID, p)
			}
			count[docID]++
		}
		for _, endpoint := range l.edges {
			if int(endpoint) >= len(s.live) || !s.live[endpoint] {
				return errors.Errorf("posting %d has an edge to doc %d, which is not live", p, endpoint)
			}
			// An edge never leads into its own posting: it is made to one
			// outside, and a split moves the source and its posting mates,
			// never the endpoint, into postings of their own.
			if st.side.postingOf[endpoint] == uint32(p) {
				return errors.Errorf("posting %d has an edge into itself, to doc %d", p, endpoint)
			}
		}
	}
	live := 0
	for docID, isLive := range s.live {
		switch {
		case isLive && count[docID] != 1:
			return errors.Errorf("live doc %d is in %d postings", docID, count[docID])
		case !isLive && st.side.postingOf[docID] != unplaced:
			return errors.Errorf("doc %d is not live, and the map puts it in %d", docID, st.side.postingOf[docID])
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

// cleanCentroidGraph removes the deleted centroids from the graph for good.
func (s *streamIndex) cleanCentroidGraph() error {
	return s.st.centroids.graph.CleanUpTombstonedNodes(func() bool { return false })
}

// postingSizes returns the size of every live posting.
func (s *streamIndex) postingSizes() []int {
	var sizes []int
	for p, l := range s.st.postings.lists {
		if !s.retired[p] {
			sizes = append(sizes, len(l.docIDs))
		}
	}
	return sizes
}

// edgeCounts are the edge recall counts of some vectors against their true
// edges: of the neighbors nearest a vector among the live vectors, those
// outside its posting, as the batch diagnostic counts them
// (sampleEdgeTruth). At posting level, the true target postings are those
// the true edges lead to, and the edges cover the ones their own endpoints
// lead to.
type edgeCounts struct {
	Vectors          int `json:"vectors"`
	Edges            int `json:"edges"`
	Wanted           int `json:"wanted"`
	Captured         int `json:"captured"`
	PostingsWanted   int `json:"postingsWanted"`
	PostingsCaptured int `json:"postingsCaptured"`
}

func (c edgeCounts) recall() float64 {
	return float64(c.Captured) / float64(max(c.Wanted, 1))
}

func (c edgeCounts) postingRecall() float64 {
	return float64(c.PostingsCaptured) / float64(max(c.PostingsWanted, 1))
}

func (c edgeCounts) degree() float64 {
	return float64(c.Edges) / float64(max(c.Vectors, 1))
}

func (c *edgeCounts) add(o edgeCounts) {
	c.Vectors += o.Vectors
	c.Edges += o.Edges
	c.Wanted += o.Wanted
	c.Captured += o.Captured
	c.PostingsWanted += o.PostingsWanted
	c.PostingsCaptured += o.PostingsCaptured
}

// countEdges counts one vector in posting p with the given edges against its
// nearest live neighbors, with the posting map as it is.
func (s *streamIndex) countEdges(p uint32, edges []uint32, nearest []scored) edgeCounts {
	postingOf := s.st.side.postingOf
	c := edgeCounts{Vectors: 1, Edges: len(edges)}
	covered := map[uint32]bool{}
	for _, endpoint := range edges {
		covered[postingOf[endpoint]] = true
	}
	targets := map[uint32]bool{}
	for _, nb := range nearest {
		target := postingOf[nb.id]
		if target == p {
			continue
		}
		c.Wanted++
		if slices.Contains(edges, uint32(nb.id)) {
			c.Captured++
		}
		targets[target] = true
	}
	for target := range targets {
		c.PostingsWanted++
		if covered[target] {
			c.PostingsCaptured++
		}
	}
	return c
}

// nearestLive returns the neighbors nearest a vector among the live vectors,
// itself left out, nearest first, by brute force.
func (s *streamIndex) nearestLive(docID uint64, nearest *topK) ([]scored, error) {
	from := s.vectorOf(docID)
	nearest.reset(s.neighbors)
	for candidate, isLive := range s.live {
		if !isLive || uint64(candidate) == docID {
			continue
		}
		dist, err := s.ix.provider.SingleDist(from, s.vectorOf(uint64(candidate)))
		if err != nil {
			return nil, err
		}
		nearest.push(uint64(candidate), dist)
	}
	return nearest.sorted(), nil
}

// edgeRecall counts the given live vectors as they are now: their edges,
// their postings and their true edges among the live vectors.
func (s *streamIndex) edgeRecall(ctx context.Context, logger logrus.FieldLogger, docIDs []uint64) (edgeCounts, error) {
	st := s.st
	counts := make([]edgeCounts, parallelWorkers())
	err := parallelChunks(ctx, logger, len(docIDs), 1, func(worker, start, end int) error {
		var nearest topK
		for _, docID := range docIDs[start:end] {
			if !s.live[docID] {
				return errors.Errorf("doc %d is not live", docID)
			}
			found, err := s.nearestLive(docID, &nearest)
			if err != nil {
				return err
			}
			p := st.side.postingOf[docID]
			l := &st.postings.lists[p]
			counts[worker].add(s.countEdges(p, l.edgesOf(l.member(docID)), found))
		}
		return nil
	})
	var total edgeCounts
	for _, c := range counts {
		total.add(c)
	}
	return total, err
}

// edgeRecallAtInsert counts a vector the moment its insert has written it,
// against the live vectors of that moment: the guard of the edge builder. Its
// brute force runs in parallel.
func (s *streamIndex) edgeRecallAtInsert(ctx context.Context, logger logrus.FieldLogger, docID uint64, p uint32,
	edges []uint32,
) (edgeCounts, error) {
	from := s.vectorOf(docID)
	nearest := make([]topK, parallelWorkers())
	err := parallelChunks(ctx, logger, len(s.live), 4096, func(worker, start, end int) error {
		top := &nearest[worker]
		if top.limit == 0 {
			top.reset(s.neighbors)
		}
		for candidate := start; candidate < end; candidate++ {
			if !s.live[candidate] || uint64(candidate) == docID {
				continue
			}
			dist, err := s.ix.provider.SingleDist(from, s.vectorOf(uint64(candidate)))
			if err != nil {
				return err
			}
			top.push(uint64(candidate), dist)
		}
		return nil
	})
	if err != nil {
		return edgeCounts{}, err
	}
	var merged topK
	merged.reset(s.neighbors)
	for i := range nearest {
		for _, e := range nearest[i].items {
			merged.push(e.id, e.dist)
		}
	}
	return s.countEdges(p, edges, merged.sorted()), nil
}
