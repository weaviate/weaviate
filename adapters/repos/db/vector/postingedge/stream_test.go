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
	"path/filepath"
	"slices"
	"sort"
	"sync/atomic"
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
// Three mechanisms can be switched on, alone or together: inverse edges,
// where a new vector enters the edges of a neighbor whose worst edge it
// beats, edge repair, where the members of a split posting search their edges
// again, and sibling repair, where they take their nearest neighbors in the
// other half of the split without a search. The first two are the controls
// of the experiment, the third its cell 5, and inverse edges with sibling
// repair its cell 6.
//
// Nothing searches while an insert runs, so an insert is complete, posting
// and map written, before any search can see the vector. checkCoverage
// checks that every vector is where the map says, and only there.

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
	// followGraph tells that the centroid graph belongs to another index,
	// one that grows in step with this one and makes the same splits: this
	// one asks the graph and never changes it. It is how the index that grows
	// on disk (v5.0, grow_test.go) is checked against this one, insert by
	// insert: the graph draws its levels at random, so two graphs grown apart
	// are not the same graph.
	followGraph bool
	// centroidDistances, when set, is the count of the distances the
	// centroid graph of the index computes: the index was created with a
	// countingProvider over it (Config.centroidProvider). The inserts read it
	// to tell what the graph took for each of their steps.
	centroidDistances *atomic.Int64
}

// countingProvider counts the distances a provider computes: one per
// SingleDist, and one per Distance of the distancers it hands out. The count
// is shared by its copies.
type countingProvider struct {
	distancer.Provider
	count *atomic.Int64
}

func (p countingProvider) SingleDist(a, b []float32) (float32, error) {
	p.count.Add(1)
	return p.Provider.SingleDist(a, b)
}

func (p countingProvider) New(vec []float32) distancer.Distancer {
	return countingDistancer{Distancer: p.Provider.New(vec), count: p.count}
}

type countingDistancer struct {
	distancer.Distancer
	count *atomic.Int64
}

func (d countingDistancer) Distance(vec []float32) (float32, error) {
	d.count.Add(1)
	return d.Distancer.Distance(vec)
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
	// lists: each is a write to the list of another vector.
	InverseEdges int `json:"inverseEdges"`
	// InverseChecks counts the neighbors' lists the inverse edges read to
	// decide, and InverseDistances the exact distances that took: those of
	// every edge of each list, where a stored distance of the worst edge
	// would do.
	InverseChecks    int `json:"inverseChecks"`
	InverseDistances int `json:"inverseDistances"`
	// InverseAlready counts the neighbors that had the vector among their
	// edges already, and were left as they were. A new vector is in no list:
	// only a refresh finds any.
	InverseAlready int `json:"inverseAlready,omitempty"`
	// InverseWrites counts the inserts by the inverse edges each wrote:
	// InverseWrites[w] inserts wrote w. InversePostings counts them by the
	// distinct postings those lists live in, which is what an insert writes
	// where the edges are stored with their posting. Both are one longer
	// than the neighbors of an insert, and nil without inverse edges.
	InverseWrites   []int `json:"inverseWrites,omitempty"`
	InversePostings []int `json:"inversePostings,omitempty"`
	// Repaired counts the vectors whose edges a split had them search again.
	Repaired int `json:"repaired"`
	// SiblingEdges counts the edges the members of split postings took into
	// the other half, SiblingDistances the exact distances that took, and
	// SiblingLists the edge lists that changed.
	SiblingEdges     int `json:"siblingEdges"`
	SiblingDistances int `json:"siblingDistances"`
	SiblingLists     int `json:"siblingLists"`
	// SearchLoads, SearchRounds, SearchScanned, SearchEndpoints and
	// SearchRescored add up the work of the edge search of every insert: the
	// postings it loaded, its load rounds after the entry postings, the
	// posting members and the edge endpoints it scored by code, and the
	// candidates it compared at full precision. With postings on disk the
	// loads are what an insert reads. The searches of a repair are not in.
	SearchLoads     int `json:"searchLoads"`
	SearchRounds    int `json:"searchRounds"`
	SearchScanned   int `json:"searchScanned"`
	SearchEndpoints int `json:"searchEndpoints"`
	SearchRescored  int `json:"searchRescored"`
	// SearchLoadsHist counts the inserts by the postings their edge search
	// loaded: SearchLoadsHist[l] inserts loaded l. It is nil until an insert
	// searches.
	SearchLoadsHist []int `json:"searchLoadsHist,omitempty"`
	// SplitDistances counts the full-precision distances of the splits: those
	// of each 2-means, failed ones included, and the two of every member to
	// the two means.
	SplitDistances int64 `json:"splitDistances"`
	// The distances the centroid graph computed, by what for. They are
	// counted only with streamConfig.centroidDistances, and are zero without:
	// AssignDistances for the search that gives an insert its posting, which
	// is where its edge search enters too, SplitGraphDistances for the two
	// centroids a split adds and the one it deletes, and CleanDistances for
	// the cleanups of the deleted ones. Up to v2.2 the edge search of an
	// insert asked the graph again for its entry postings, and the data of
	// those runs has that second search as searchCentroidDistances.
	AssignDistances     int64 `json:"assignDistances"`
	SplitGraphDistances int64 `json:"splitGraphDistances"`
	CleanDistances      int64 `json:"cleanDistances"`

	AssignTime  time.Duration `json:"assignNs"`
	NearestTime time.Duration `json:"nearestCheckNs"` // only the report needs it
	EdgeTime    time.Duration `json:"edgeSearchNs"`
	WriteTime   time.Duration `json:"writeNs"`
	InverseTime time.Duration `json:"inverseNs"`
	SplitTime   time.Duration `json:"splitNs"`
	RepairTime  time.Duration `json:"repairNs"`
	SiblingTime time.Duration `json:"siblingNs"`
}

// clone returns the stats with counts of their own: what a checkpoint keeps
// while the inserts go on.
func (s streamStats) clone() streamStats {
	s.InverseWrites = slices.Clone(s.InverseWrites)
	s.InversePostings = slices.Clone(s.InversePostings)
	s.SearchLoadsHist = slices.Clone(s.SearchLoadsHist)
	return s
}

// countSearch adds the work of the edge search of one insert.
func (s *streamStats) countSearch(work SearchStats) {
	s.SearchLoads += work.PostingsLoaded
	s.SearchRounds += work.Rounds
	s.SearchScanned += work.VectorsScanned
	s.SearchEndpoints += work.EndpointsScored
	s.SearchRescored += work.Rescored
	if grow := work.PostingsLoaded + 1 - len(s.SearchLoadsHist); grow > 0 {
		s.SearchLoadsHist = append(s.SearchLoadsHist, make([]int, grow)...)
	}
	s.SearchLoadsHist[work.PostingsLoaded]++
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
	// written holds the postings the inverse edges of one insert wrote.
	written []uint32
	// builtAt is, by doc id, how many vectors were live when the edges of
	// the vector were last built: by the batch build, by its insert, itself
	// counted, or by its last refresh. An inverse edge that enters its list
	// does not count: it does not build the list again.
	builtAt []uint32
	// onRefresh, when set, is called with the list a refresh gives its
	// vector, before the inverse edges of that refresh. The list is the
	// caller's to keep.
	onRefresh func(docID uint64, edges []uint32)
	// traceRefresh, when set, gives the hook the search of a refresh calls
	// with every posting it loads (SearchStats.loadHook), nil for none.
	traceRefresh func(docID uint64) func(sc *searchScratch, post *posting, cut float32)
	// onRebuild, when set, is called by rebuildEdges with the live postings
	// the batch rule reads the candidates of every vector from. probes is
	// only valid during the call.
	onRebuild func(docID uint64, probes []uint32)
	// beforeSplit, when set, is called with a posting that an insert left
	// past the threshold, before it is split: it may make the posting
	// smaller, and the posting is split only if it is still past it. The
	// deletes experiment drops the deleted members of the posting there
	// (delete_test.go).
	beforeSplit func(p uint32)
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
	case cfg.threshold < 2:
		return nil, errors.Errorf("a split threshold of %d leaves nothing to split", cfg.threshold)
	}
	// The postings keep the codes the scan reads, and the sidecar those the
	// endpoints are ranked by: the same ones, unless the postings have codes
	// of their own (state.scanQuantizer).
	store, words, sideWords := batch.postings, batch.postings.codeWords, batch.side.codeWords
	side := &sidecar{codeWords: sideWords, codes: make([]uint64, keys*sideWords), postingOf: make([]uint32, keys)}
	for i := range side.postingOf {
		side.postingOf[i] = unplaced
	}
	s := &streamIndex{
		ix: ix, cfg: cfg, vectorOf: vectorOf, neighbors: ix.uc.Edges,
		live: make([]bool, keys), sums: make([]float64, batch.dims), l2: distancer.NewL2SquaredProvider(),
	}
	if cfg.inverse {
		s.stats.InverseWrites = make([]int, s.neighbors+1)
		s.stats.InversePostings = make([]int, s.neighbors+1)
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
			lists[p].append(docID, store.codes[int(slot)*words:(int(slot)+1)*words], keyed)
			copy(side.codes[int(docID)*sideWords:], batch.side.code(slot))
			side.postingOf[docID] = uint32(p)
			s.live[docID] = true
			s.liveCount++
		}
	}
	s.retired = make([]bool, len(lists))
	s.builtAt = make([]uint32, keys)
	for docID, live := range s.live {
		if live {
			s.builtAt[docID] = uint32(s.liveCount)
		}
	}
	// Every split adds two postings, and there is at most one split per
	// insert.
	s.capacity = len(lists) + 2*keys

	st := &state{
		dims:          batch.dims,
		quantizer:     batch.quantizer,
		scanQuantizer: batch.scanQuantizer,
		centroids:     batch.centroids,
		postings:      &postingStore{codeWords: words, lists: lists},
		side:          side,
		visited:       visited.NewPool(keys),
		build:         batch.build,
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

// centroidCount returns the distances the centroid graph has computed so
// far, 0 when they are not counted.
func (s *streamIndex) centroidCount() int64 {
	if s.cfg.centroidDistances == nil {
		return 0
	}
	return s.cfg.centroidDistances.Load()
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
	counted := s.centroidCount()
	// The graph is asked once: the nearest centroid is the posting of the
	// vector, and what it returns is where the search for its edges enters,
	// as many postings as any search of the index enters through.
	entry, err := st.centroids.nearest(ctx, vector, max(int(s.ix.entryPostings.Load()), 1))
	if err != nil {
		return err
	}
	s.stats.AssignDistances += s.centroidCount() - counted
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
		var work SearchStats
		neighbors, dists, err = s.ix.searchFrom(ctx, vector, s.neighbors, nil, entry, &work)
		if err != nil {
			return errors.Wrapf(err, "search the neighbors of doc %d", docID)
		}
		// SpreadTime is what a search spends on its stats alone.
		s.stats.EdgeTime += time.Since(clock) - work.SpreadTime
		s.stats.countSearch(work)
	}

	clock = time.Now()
	code := st.quantizer.Encode(vector)
	words := st.side.codeWords
	copy(st.side.codes[int(docID)*words:(int(docID)+1)*words], code)
	scanCode := code
	if st.scanQuantizer != nil {
		scanCode = st.scanQuantizer.Encode(vector)
	}
	edges := s.outside(neighbors, p)
	st.postings.lists[p].append(docID, scanCode, edges)
	st.side.postingOf[docID] = p
	s.live[docID] = true
	s.liveCount++
	s.builtAt[docID] = uint32(s.liveCount)
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
	if len(st.postings.lists[p].docIDs) > s.cfg.threshold && s.beforeSplit != nil {
		s.beforeSplit(p)
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
// edge goes. A neighbor without edges takes none, and neither does one that
// has the vector among its edges already, which only a vector that was in
// the index before can find. dists are the exact distances from the vector to
// its neighbors.
func (s *streamIndex) enterNeighbors(docID uint64, p uint32, neighbors []uint64, dists []float32) error {
	writes := 0
	s.written = s.written[:0]
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
		if slices.Contains(edges, uint32(docID)) {
			s.stats.InverseAlready++
			continue
		}
		s.stats.InverseChecks++
		s.stats.InverseDistances += len(edges)
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
		writes++
		if !slices.Contains(s.written, q) {
			s.written = append(s.written, q)
		}
	}
	s.stats.InverseWrites[writes]++
	s.stats.InversePostings[len(s.written)]++
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
	left, right, computed, err := bisect(vectors, st.dims, cluster{members: members, leaves: 2, path: 1},
		s.cfg.seed^uint64(s.stats.Splits+s.stats.SplitsFailed))
	s.stats.SplitDistances += int64(computed)
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
	s.stats.SplitDistances += int64(2 * n)
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
	words := st.postings.codeWords
	for i, docID := range old.docIDs {
		children[sideOf[i]].append(docID, old.codes[i*words:(i+1)*words], old.edgesOf(i))
		st.side.postingOf[docID] = first + uint32(sideOf[i])
	}
	st.postings.lists = append(st.postings.lists, children[0], children[1])
	st.postings.lists[p] = newPostingList()
	s.retired = append(s.retired, false, false)
	s.retired[p] = true

	counted := s.centroidCount()
	if !s.cfg.followGraph {
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
	}
	s.stats.SplitGraphDistances += s.centroidCount() - counted
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
			if !slices.Equal(edges, l.edgesOf(i)) {
				s.stats.SiblingLists++
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
		s.builtAt[docID] = uint32(s.liveCount)
		s.stats.Repaired++
	}
	return nil
}

// refreshStats counts what a refresh of the edges of some vectors did.
type refreshStats struct {
	Refreshed int `json:"refreshed"`
	// SelfMissing counts the vectors their own search did not return.
	SelfMissing int `json:"selfMissing"`
	// ListsChanged counts the vectors whose list is not the one they had.
	// EdgesBefore, EdgesAfter and EdgesKept count the edges of all the
	// refreshed vectors before, after, and in both lists.
	ListsChanged int `json:"listsChanged"`
	EdgesBefore  int `json:"edgesBefore"`
	EdgesAfter   int `json:"edgesAfter"`
	EdgesKept    int `json:"edgesKept"`
	// Work is the work of the searches and of the inverse edges, counted as
	// the inserts count theirs and apart from them. Nothing else in it is
	// set: a refresh assigns, writes and splits nothing.
	Work streamStats   `json:"work"`
	Time time.Duration `json:"ns"`
}

// refreshEdges refreshes the edges of the given vectors, one after another
// in the order given, and returns what that took. The counts of the inserts
// are left as they were.
func (s *streamIndex) refreshEdges(ctx context.Context, docIDs []uint64) (refreshStats, error) {
	var out refreshStats
	inserts := s.stats
	defer func() { s.stats = inserts }()
	s.stats = streamStats{}
	if s.cfg.inverse {
		s.stats.InverseWrites = make([]int, s.neighbors+1)
		s.stats.InversePostings = make([]int, s.neighbors+1)
	}
	begin := time.Now()
	for _, docID := range docIDs {
		if err := s.refresh(ctx, docID, &out); err != nil {
			return out, err
		}
	}
	out.Time = time.Since(begin)
	out.Work = s.stats
	return out, nil
}

// refresh repeats the edge step of the insert of a vector that is in the
// index. Its edge search enters through its own posting, where the search of
// an insert enters through the posting its vector was given. Its list
// becomes what an insert writes: of the nearest neighbors the search finds,
// itself left out, those outside its posting. And it takes its place in the
// edges of those neighbors as a new vector does, except where it has one
// already. The vector stays where it is: no posting, member, code or
// centroid changes. The search parameters are the index's own, as for an
// insert, and the work is counted in the stats of the index.
func (s *streamIndex) refresh(ctx context.Context, docID uint64, out *refreshStats) error {
	if docID >= uint64(len(s.live)) || !s.live[docID] {
		return errors.Errorf("doc %d is not in the index", docID)
	}
	p := s.st.side.postingOf[docID]
	l := &s.st.postings.lists[p]
	m := l.member(docID)
	if m < 0 {
		return errors.Errorf("doc %d is not in posting %d, where the map has it", docID, p)
	}

	// One more than an insert asks for: the search finds the vector itself.
	var work SearchStats
	if s.traceRefresh != nil {
		work.loadHook = s.traceRefresh(docID)
	}
	ids, dists, err := s.ix.searchFrom(ctx, s.vectorOf(docID), s.neighbors+1, nil, []uint64{uint64(p)}, &work)
	if err != nil {
		return errors.Wrapf(err, "search the neighbors of doc %d", docID)
	}
	s.stats.countSearch(work)
	if at := slices.Index(ids, docID); at >= 0 {
		ids, dists = slices.Delete(ids, at, at+1), slices.Delete(dists, at, at+1)
	} else {
		out.SelfMissing++
	}
	ids, dists = ids[:min(len(ids), s.neighbors)], dists[:min(len(dists), s.neighbors)]

	edges := s.outside(ids, p)
	old := l.edgesOf(m)
	out.EdgesBefore += len(old)
	out.EdgesAfter += len(edges)
	for _, endpoint := range edges {
		if slices.Contains(old, endpoint) {
			out.EdgesKept++
		}
	}
	if !slices.Equal(old, edges) {
		l.setEdges(m, edges)
		out.ListsChanged++
	}
	s.builtAt[docID] = uint32(s.liveCount)
	if s.onRefresh != nil {
		s.onRefresh(docID, edges)
	}
	if s.cfg.inverse {
		if err := s.enterNeighbors(docID, p, ids, dists); err != nil {
			return err
		}
	}
	out.Refreshed++
	return nil
}

// staleEdges returns, of the given vectors and in their order, those whose
// edges were last built when the index held half the vectors it holds now,
// or fewer: the ones a refresh on doubling takes.
func (s *streamIndex) staleEdges(docIDs []uint64) []uint64 {
	var stale []uint64
	for _, docID := range docIDs {
		if s.live[docID] && 2*int(s.builtAt[docID]) <= s.liveCount {
			stale = append(stale, docID)
		}
	}
	return stale
}

// edgeSnapshot is a copy of the edges of every posting, and of when the
// edges of every vector were built.
type edgeSnapshot struct {
	edges, starts [][]uint32
	builtAt       []uint32
	digest        uint64
}

// edgesDigest folds every live vector and its edges, in the order the
// postings hold them, into one number: two indexes with the same members in
// the same places have the same digest only if every list is the same, bar
// a collision.
func (s *streamIndex) edgesDigest() uint64 {
	h := uint64(14695981039346656037)
	mix := func(v uint64) {
		h ^= v
		h *= 1099511628211
	}
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		l := &s.st.postings.lists[p]
		for i, docID := range l.docIDs {
			edges := l.edgesOf(i)
			mix(docID)
			mix(uint64(len(edges)))
			for _, endpoint := range edges {
				mix(uint64(endpoint))
			}
		}
	}
	return h
}

// snapshotEdges copies the edges of every vector as they are.
func (s *streamIndex) snapshotEdges() edgeSnapshot {
	lists := s.st.postings.lists
	snap := edgeSnapshot{
		edges: make([][]uint32, len(lists)), starts: make([][]uint32, len(lists)),
		builtAt: slices.Clone(s.builtAt), digest: s.edgesDigest(),
	}
	for p := range lists {
		snap.edges[p] = slices.Clone(lists[p].edges)
		snap.starts[p] = slices.Clone(lists[p].edgeStart)
	}
	return snap
}

// restoreEdges puts the edges of a snapshot back. The postings must hold the
// members they held then, in the same places: nothing was inserted or split
// since. It checks that the index ends with the lists of the snapshot.
func (s *streamIndex) restoreEdges(snap edgeSnapshot) error {
	lists := s.st.postings.lists
	if len(snap.edges) != len(lists) {
		return errors.Errorf("the snapshot holds %d postings, the index %d", len(snap.edges), len(lists))
	}
	for p := range lists {
		if len(snap.starts[p]) != len(lists[p].docIDs)+1 {
			return errors.Errorf("posting %d holds %d members, the snapshot has the edges of %d",
				p, len(lists[p].docIDs), len(snap.starts[p])-1)
		}
	}
	for p := range lists {
		lists[p].edges = slices.Clone(snap.edges[p])
		lists[p].edgeStart = slices.Clone(snap.starts[p])
	}
	copy(s.builtAt, snap.builtAt)
	if got := s.edgesDigest(); got != snap.digest {
		return errors.Errorf("the restored edges have digest %x, the snapshot %x: the members changed since", got, snap.digest)
	}
	return nil
}

// replaceEdges gives every live vector the edges listOf returns for it.
func (s *streamIndex) replaceEdges(listOf func(docID uint64) []uint32) {
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		l := &s.st.postings.lists[p]
		edges := make([]uint32, 0, len(l.edges))
		starts := make([]uint32, 1, len(l.docIDs)+1)
		for _, docID := range l.docIDs {
			edges = append(edges, listOf(docID)...)
			starts = append(starts, uint32(len(edges)))
		}
		l.edges, l.edgeStart = edges, starts
	}
}

// incomingEdges counts, for every posting, the edges of all the live vectors
// that end at one of its members.
func (s *streamIndex) incomingEdges() []int {
	incoming := make([]int, len(s.st.postings.lists))
	postingOf := s.st.side.postingOf
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		for _, endpoint := range s.st.postings.lists[p].edges {
			incoming[postingOf[endpoint]]++
		}
	}
	return incoming
}

// repeatedEdges counts the live vectors whose edges hold the same endpoint
// more than once. No step of the index should leave any, and checkCoverage
// does not look for them.
func (s *streamIndex) repeatedEdges() int {
	repeated := 0
	for p := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		l := &s.st.postings.lists[p]
		for i := range l.docIDs {
			edges := l.edgesOf(i)
			for e := 1; e < len(edges); e++ {
				if slices.Contains(edges[:e], edges[e]) {
					repeated++
					break
				}
			}
		}
	}
	return repeated
}

// checkCoverage checks the invariant of the inserts: every live vector is in
// exactly one posting, the one the map gives it, with its code; no posting
// holds anything else; every edge leads to a live vector outside its own
// posting; and the centroid graph holds the live postings and none of the
// retired ones.
func (s *streamIndex) checkCoverage() error {
	st := s.st
	words := st.postings.codeWords
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
			// With codes of their own the postings and the sidecar hold
			// different codes of the vector.
			if st.scanQuantizer == nil && !slices.Equal(l.codes[i*words:(i+1)*words], st.side.code(uint32(docID))) {
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
	counted := s.centroidCount()
	err := s.st.centroids.graph.CleanUpTombstonedNodes(func() bool { return false })
	s.stats.CleanDistances += s.centroidCount() - counted
	return err
}

// rebuiltEdges is what rebuilding the edges took, in the counts of a batch
// build, and what its exact assignment saw of the partition on the way.
type rebuiltEdges struct {
	Vectors  int `json:"vectors"`
	Postings int `json:"postings"`
	Edges    int `json:"edges"`
	// NotNearest counts the vectors whose posting is not that of their
	// nearest centroid: what a reassignment would move. OutsideProbes counts
	// those whose posting is not even among the probe nearest; it took the
	// place of the farthest of them.
	NotNearest    int `json:"notNearest"`
	OutsideProbes int `json:"outsideProbes"`
	// The work of the edge builder, as BuildStats counts it, and the
	// distances of the brute-force assignment that gives it its probes.
	PostingsProbed     int64 `json:"postingsProbed"`
	CandidatesScored   int64 `json:"candidatesScored"`
	CandidatesRescored int64 `json:"candidatesRescored"`
	AssignDistances    int64 `json:"assignDistances"`
}

// rebuildEdges gives every live vector the edges the batch builder gives it
// over the partition as it is. The postings, their members, their centroids,
// the codes and the centroid graph stay: only the edge lists change. It is
// the diagnostic of v2.2 (bench-report.md, "Diagnóstico: aristas o
// partición"): what an index grown by inserts gets back with it was lost in
// its edges, and what it does not, in its partition.
//
// The builder is the batch one, with its inputs: the candidates of a vector
// are the members of the postings of its probe nearest live centroids, found
// by brute force, ranked by code, the best rescue re-ranked exactly, and of
// the nearest neighbors those outside the vector's posting become its edges.
// A batch build puts a vector in the posting of its nearest centroid, so its
// own posting leads its probes; here it may not, and a vector whose posting
// is not among its probes at all gets it in place of the farthest one.
func (s *streamIndex) rebuildEdges(ctx context.Context, logger logrus.FieldLogger, probe, rescue int,
) (rebuiltEdges, error) {
	st := s.st
	// The live postings, numbered as a build numbers them, without gaps.
	compact := make([]uint32, len(st.postings.lists))
	var alive []uint32
	var centroids [][]float32
	for p := range st.postings.lists {
		compact[p] = noCentroid
		if s.retired[p] {
			continue
		}
		compact[p] = uint32(len(alive))
		alive = append(alive, uint32(p))
		centroids = append(centroids, st.centroids.vectors[p])
	}
	// The live vectors in doc id order: the input of the build.
	docIDs := make([]uint64, 0, s.liveCount)
	inputOf := make([]uint32, len(s.live))
	for docID, live := range s.live {
		if live {
			inputOf[docID] = uint32(len(docIDs))
			docIDs = append(docIDs, uint64(docID))
		}
	}
	if len(docIDs) == 0 {
		return rebuiltEdges{}, errors.New("no live vector to rebuild the edges of")
	}
	vectors := make([][]float32, len(docIDs))
	for i, docID := range docIDs {
		vectors[i] = s.vectorOf(docID)
	}

	assigned, err := assign(ctx, logger, vectors, centroids, probe)
	if err != nil {
		return rebuiltEdges{}, errors.Wrap(err, "the nearest centroids of every vector")
	}
	out := rebuiltEdges{
		Vectors: len(docIDs), Postings: len(alive),
		AssignDistances: int64(len(docIDs)) * int64(len(centroids)),
	}
	for i, docID := range docIDs {
		own := compact[st.side.postingOf[docID]]
		if own == noCentroid {
			return rebuiltEdges{}, errors.Errorf("doc %d is in posting %d, which is not live", docID, st.side.postingOf[docID])
		}
		if assigned.primary[i] == own {
			continue
		}
		out.NotNearest++
		if probes := assigned.probesOf(i); !slices.Contains(probes, own) {
			out.OutsideProbes++
			probes[len(probes)-1] = own
		}
		assigned.primary[i] = own
	}
	if s.onRebuild != nil {
		width := min(probe, assigned.width)
		probes := make([]uint32, 0, width)
		for i, docID := range docIDs {
			probes = probes[:0]
			for _, c := range assigned.probesOf(i)[:width] {
				if c != noCentroid {
					probes = append(probes, alive[c])
				}
			}
			s.onRebuild(docID, probes)
		}
	}

	// The builder works by slot: the vectors in posting order, with their
	// codes, which are the ones the postings hold.
	layout := newSlotLayout(assigned.primary, len(centroids))
	words := st.side.codeWords
	codes := make([]uint64, len(docIDs)*words)
	for slot, input := range layout.order {
		copy(codes[slot*words:(slot+1)*words], st.side.code(uint32(docIDs[input])))
	}
	edgeStart, edges, work, err := buildEdges(ctx, logger, edgeInput{
		vectors:   vectors,
		layout:    layout,
		assign:    assigned,
		side:      &sidecar{codeWords: words, codes: codes, postingOf: postingOfSlots(layout.start)},
		quantizer: st.quantizer,
		provider:  s.ix.provider,
		probe:     probe,
		neighbors: s.neighbors,
		rescue:    rescue,
	})
	if err != nil {
		return rebuiltEdges{}, errors.Wrap(err, "build the edges")
	}
	out.Edges = len(edges)
	out.PostingsProbed, out.CandidatesScored, out.CandidatesRescored = work.probed, work.scored, work.rescored

	// Back to the lists: every member's new edges, by doc id.
	for _, p := range alive {
		l := &st.postings.lists[p]
		rebuilt := make([]uint32, 0, len(l.edges))
		starts := make([]uint32, 1, len(l.docIDs)+1)
		for _, docID := range l.docIDs {
			slot := layout.slotOf[inputOf[docID]]
			for _, endpoint := range edges[edgeStart[slot]:edgeStart[slot+1]] {
				rebuilt = append(rebuilt, uint32(docIDs[layout.order[endpoint]]))
			}
			starts = append(starts, uint32(len(rebuilt)))
		}
		l.edges, l.edgeStart = rebuilt, starts
	}
	for _, docID := range docIDs {
		s.builtAt[docID] = uint32(s.liveCount)
	}
	return out, nil
}

// rebuildCentroidGraph replaces the centroid graph with one built at once
// over the live centroids, as a batch build builds its own: the centroids
// stay the same vectors under the same posting ids, and the graph that grew
// with the splits, with its deleted nodes cleaned up along the way, is gone.
func (s *streamIndex) rebuildCentroidGraph(ctx context.Context) error {
	st := s.st
	var alive []uint64
	for p := range st.postings.lists {
		if !s.retired[p] {
			alive = append(alive, uint64(p))
		}
	}
	// An empty graph, which reads the centroids it is given afterwards.
	fresh, err := newCentroidIndex(ctx, &s.ix.cfg, filepath.Join(s.ix.dir(), "rebuilt"), nil)
	if err != nil {
		return err
	}
	fresh.vectors = st.centroids.vectors
	err = parallelChunks(ctx, s.ix.cfg.Logger, len(alive), 16, func(_, start, end int) error {
		for _, p := range alive[start:end] {
			if err := fresh.graph.Add(ctx, p, fresh.vectors[p]); err != nil {
				return errors.Wrapf(err, "add centroid %d", p)
			}
		}
		return nil
	})
	if err != nil {
		_ = fresh.shutdown(ctx)
		return err
	}
	old := st.centroids
	st.centroids = fresh
	return old.shutdown(ctx)
}

// cohortDegree counts the edges of the live vectors of one cohort: those
// whose place in the order they entered the index in is in [From, To).
type cohortDegree struct {
	From    int `json:"from"`
	To      int `json:"to"`
	Vectors int `json:"vectors"`
	Edges   int `json:"edges"`
}

// degreeByCohort counts the edges of every live vector, by cohort. rank is
// the place of every doc id in the order the vectors entered the index in,
// and bounds, ascending, part that order: cohort i holds the places in
// [bounds[i], bounds[i+1]). A vector placed outside the bounds is not
// counted.
func (s *streamIndex) degreeByCohort(rank []uint32, bounds []int) []cohortDegree {
	if len(bounds) < 2 {
		return nil
	}
	out := make([]cohortDegree, len(bounds)-1)
	for i := range out {
		out[i].From, out[i].To = bounds[i], bounds[i+1]
	}
	for p, l := range s.st.postings.lists {
		if s.retired[p] {
			continue
		}
		for i, docID := range l.docIDs {
			// The last bound at or before the place of the vector.
			c := sort.SearchInts(bounds, int(rank[docID])+1) - 1
			if c < 0 || c >= len(out) {
				continue
			}
			out[c].Vectors++
			out[c].Edges += int(l.edgeStart[i+1] - l.edgeStart[i])
		}
	}
	return out
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
	return countEdgesIn(s.st.side.postingOf, p, edges, nearest)
}

// countEdgesIn counts one vector in posting p with the given edges against its
// nearest live neighbors, with the given posting map.
func countEdgesIn(postingOf []uint32, p uint32, edges []uint32, nearest []scored) edgeCounts {
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
