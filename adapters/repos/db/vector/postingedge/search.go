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
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var inf32 = float32(math.Inf(1))

// searchScratch is the per-search working memory, pooled per built index.
type searchScratch struct {
	// rank is the best estimate among the known edge endpoints of a posting,
	// +Inf while none is known. An unloaded posting is ranked by it.
	rank []float32
	// loaded marks the postings already loaded, or picked for the round
	// being loaded.
	loaded []bool
	// frontier lists the postings with a finite rank. Loaded ones stay in it
	// until the next selection sweeps them out.
	frontier []uint32
	// touched lists the loaded postings, to reset rank and loaded cheaply.
	touched []uint32
	// batch is the round being selected, best ranked first.
	batch []scored
	// stop holds the ef best candidates, its worst is the stop threshold.
	stop topK
	// pool holds the candidates the result is drawn from.
	pool topK
	// result is the final top k.
	result topK
	// endpoints marks the edge endpoints already scored, by slot.
	endpoints *visited.SparseSet
	// unscored lists the endpoints of the posting being loaded that still
	// have to be scored, and unscoredDist receives their estimates.
	unscored     []uint32
	unscoredDist []float32
	// memberDist holds the estimates of the members of the posting being
	// loaded, in posting order.
	memberDist []float32
}

func newSearchScratch(postings int) *searchScratch {
	sc := &searchScratch{
		rank:   make([]float32, postings),
		loaded: make([]bool, postings),
	}
	for i := range sc.rank {
		sc.rank[i] = inf32
	}
	return sc
}

func (st *state) borrowScratch() *searchScratch {
	sc := st.scratch.Get().(*searchScratch)
	sc.endpoints = st.visited.Borrow()
	return sc
}

func (st *state) returnScratch(sc *searchScratch) {
	for _, p := range sc.frontier {
		sc.rank[p] = inf32
	}
	for _, p := range sc.touched {
		sc.rank[p] = inf32
		sc.loaded[p] = false
	}
	sc.frontier = sc.frontier[:0]
	sc.touched = sc.touched[:0]
	sc.batch = sc.batch[:0]
	st.visited.Return(sc.endpoints)
	sc.endpoints = nil
	st.scratch.Put(sc)
}

// selectBatch picks the up to limit best-ranked unloaded postings into
// sc.batch, best first, and sweeps the loaded postings out of the frontier.
func (sc *searchScratch) selectBatch(limit int) {
	sc.batch = sc.batch[:0]
	kept := 0
	for _, p := range sc.frontier {
		if sc.loaded[p] {
			continue
		}
		sc.frontier[kept] = p
		kept++

		entry := scored{id: uint64(p), dist: sc.rank[p]}
		switch {
		case len(sc.batch) < limit:
			sc.batch = append(sc.batch, entry)
		case worse(sc.batch[len(sc.batch)-1], entry):
			sc.batch[len(sc.batch)-1] = entry
		default:
			continue
		}
		for i := len(sc.batch) - 1; i > 0 && worse(sc.batch[i-1], sc.batch[i]); i-- {
			sc.batch[i-1], sc.batch[i] = sc.batch[i], sc.batch[i-1]
		}
	}
	sc.frontier = sc.frontier[:kept]
}

// markLoaded takes a posting out of the ranking for the rest of the search.
func (sc *searchScratch) markLoaded(p uint32) {
	sc.loaded[p] = true
	sc.touched = append(sc.touched, p)
}

// SearchByVector returns the k nearest vectors to the query, nearest first.
func (ix *Index) SearchByVector(ctx context.Context, vector []float32, k int,
	allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return ix.search(ctx, vector, k, allow, nil)
}

// SearchByVectorWithStats is SearchByVector that also reports the work the
// search did. Collecting the stats costs a few clock reads per posting.
func (ix *Index) SearchByVectorWithStats(ctx context.Context, vector []float32, k int,
	allow helpers.AllowList,
) ([]uint64, []float32, SearchStats, error) {
	var stats SearchStats
	ids, dists, err := ix.search(ctx, vector, k, allow, &stats)
	return ids, dists, stats, err
}

func (ix *Index) search(ctx context.Context, vector []float32, k int, allow helpers.AllowList,
	stats *SearchStats,
) ([]uint64, []float32, error) {
	st := ix.state.Load()
	if st == nil {
		return nil, nil, ErrNotBuilt
	}
	if allow != nil {
		return nil, nil, errors.Wrap(ErrNotSupported, "filtered search")
	}
	if k < 1 {
		return nil, nil, errors.Errorf("k must be positive, got %d", k)
	}
	if len(vector) != st.dims {
		return nil, nil, errors.Wrapf(common.ErrWrongDimensions,
			"query has length %d, the index holds vectors of length %d", len(vector), st.dims)
	}
	// Every estimate against such a query is NaN: it would come back with
	// no results and no error.
	if err := checkValues(vector); err != nil {
		return nil, nil, errors.Wrap(err, "query")
	}

	query := ix.normalized(vector)
	ef := max(int(ix.ef.Load()), k)
	rescoreLimit := int(ix.rescoreLimit.Load())
	entries := int(ix.entryPostings.Load())
	batch := int(ix.loadBatch.Load())
	margin := math.Float32frombits(ix.sourceMargin.Load())

	sc := st.borrowScratch()
	defer st.returnScratch(sc)

	// The result is drawn from the best max(rescoreLimit, k) candidates.
	// The stop threshold is the ef-th best one, which only needs a heap of
	// its own when ef differs from that.
	poolSize := max(rescoreLimit, k)
	sc.pool.reset(poolSize)
	stop := &sc.pool
	if ef != poolSize {
		sc.stop.reset(ef)
		stop = &sc.stop
	}
	search := st.newPostingSearch(sc, query, stop, margin, stats)

	var clock time.Time
	if stats != nil {
		clock = time.Now()
	}
	entryIDs, err := st.centroids.nearest(ctx, query, entries)
	if err != nil {
		return nil, nil, err
	}
	if stats != nil {
		stats.CentroidTime = time.Since(clock)
	}
	for _, p := range entryIDs {
		sc.markLoaded(uint32(p))
	}
	for _, p := range entryIDs {
		if err := search.load(uint32(p)); err != nil {
			return nil, nil, err
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if stats != nil {
			clock = time.Now()
		}
		sc.selectBatch(batch)
		if stats != nil {
			stats.SelectTime += time.Since(clock)
		}
		// Stop once no known unloaded posting holds an endpoint closer
		// than the ef-th candidate: nothing reachable can enter the top ef.
		if len(sc.batch) == 0 || sc.batch[0].dist >= stop.worst() {
			break
		}
		if stats != nil {
			stats.Rounds++
		}
		// All of the round is marked before any of it is loaded, so that
		// edges between postings of the same round are not scored.
		for _, pick := range sc.batch {
			sc.markLoaded(uint32(pick.id))
		}
		for _, pick := range sc.batch {
			if err := search.load(uint32(pick.id)); err != nil {
				return nil, nil, err
			}
		}
	}
	if stats != nil && stats.trace != nil {
		stats.trace.finish(sc)
	}

	candidates := sc.pool.sorted()
	if rescoreLimit == 0 {
		return topResults(candidates, k)
	}

	if stats != nil {
		stats.Rescored = len(candidates)
		clock = time.Now()
		stats.RescoredPostings, stats.RescoredElsewhere = st.rescoreSpread(sc, candidates)
		stats.SpreadTime = time.Since(clock)
		clock = time.Now()
	}
	if err := ix.rescore(ctx, query, candidates); err != nil {
		return nil, nil, err
	}
	sc.result.reset(k)
	for _, cand := range candidates {
		sc.result.push(cand.id, cand.dist)
	}
	if stats != nil {
		stats.RescoreTime = time.Since(clock)
	}
	return topResults(sc.result.sorted(), k)
}

// topResults copies the first k of the sorted candidates out of the scratch.
func topResults(sorted []scored, k int) ([]uint64, []float32, error) {
	sorted = sorted[:min(k, len(sorted))]
	ids := make([]uint64, len(sorted))
	dists := make([]float32, len(sorted))
	for i, e := range sorted {
		ids[i] = e.id
		dists[i] = e.dist
	}
	return ids, dists, nil
}

// postingSearch is the state of one search while it loads postings.
type postingSearch struct {
	st *state
	sc *searchScratch
	// scan estimates the distance to the members of a posting from the codes
	// in the posting, estimator the distance to edge endpoints from their
	// sidecar codes. They are one and the same unless the postings have
	// codes of their own.
	scan      codeEstimator
	estimator codeEstimator
	stop      *topK
	margin    float32 // source margin, 0 for none
	stats     *SearchStats
}

// newPostingSearch prepares the search of one query. With codes of their own
// in the postings the query is encoded twice, once per codec.
func (st *state) newPostingSearch(sc *searchScratch, query []float32, stop *topK, margin float32,
	stats *SearchStats,
) postingSearch {
	search := postingSearch{
		st:        st,
		sc:        sc,
		estimator: st.quantizer.NewEstimator(query),
		stop:      stop,
		margin:    margin,
		stats:     stats,
	}
	search.scan = search.estimator
	if st.scanQuantizer != nil {
		search.scan = st.scanQuantizer.NewEstimator(query)
	}
	return search
}

// sourceCut is the estimate beyond which a scanned vector is too far from
// the query for its edges to be followed: the source margin past the stop
// threshold. There is no cut without a margin, or while the search has not
// found ef candidates yet.
func (s *postingSearch) sourceCut() float32 {
	if s.margin <= 0 {
		return inf32
	}
	return s.stop.worst() + s.margin
}

// load scans a posting into the candidate set and ranks the postings its
// edges lead to. The posting must already be marked as loaded.
func (s *postingSearch) load(p uint32) error {
	sc := s.sc
	post := s.st.postings.load(p)
	words := s.st.postings.codeWords

	var clock time.Time
	if s.stats != nil {
		clock = time.Now()
	}

	separateStop := s.stop != &sc.pool
	sc.memberDist = sc.memberDist[:0]
	for i, docID := range post.docIDs {
		dist, err := s.scan.Distance(post.codes[i*words : (i+1)*words])
		if err != nil {
			return errors.Wrapf(err, "estimate distance to doc %d", docID)
		}
		sc.memberDist = append(sc.memberDist, dist)
		sc.pool.push(docID, dist)
		if separateStop {
			sc.stop.push(docID, dist)
		}
	}

	// Every member within the source cut is a source: the endpoints of all
	// its edges are scored, not only those of the best candidates. The cut
	// is taken after the scan, which may have tightened the threshold. An
	// endpoint is scored once per search, and not at all if its posting is
	// already loaded.
	side := s.st.side
	cut := s.sourceCut()

	if s.stats != nil {
		s.stats.ScanTime += time.Since(clock)
		s.stats.PostingsLoaded++
		s.stats.VectorsScanned += len(post.docIDs)
		// Counted in a pass of its own, outside the timed sections, so
		// that the loop below stays as it runs without stats.
		for i := range post.docIDs {
			if sc.endpoints.Visited(uint64(post.first) + uint64(i)) {
				s.stats.ScannedAgain++
			}
			edges := post.edgesOf(i)
			if sc.memberDist[i] > cut {
				s.stats.SourcesCut++
				s.stats.EdgesCut += len(edges)
				continue
			}
			s.stats.EdgesFollowed += len(edges)
			for _, endpoint := range edges {
				if sc.loaded[side.postingOf[endpoint]] {
					s.stats.EndpointsInLoaded++
				}
			}
		}
		if tr := s.stats.trace; tr != nil {
			tr.picked(sc, p)
			tr.predict(sc, side.postingOf, &post, cut)
		}
		clock = time.Now()
	}

	sc.collectEndpoints(side.postingOf, &post, cut)

	// Scored as one batch, then folded into the ranks of their postings.
	if cap(sc.unscoredDist) < len(sc.unscored) {
		sc.unscoredDist = make([]float32, len(sc.unscored))
	}
	dists := sc.unscoredDist[:len(sc.unscored)]
	if err := side.score(s.estimator, sc.unscored, dists); err != nil {
		return err
	}
	sc.rankPostings(side.postingOf, dists)

	if s.stats != nil {
		s.stats.ExpandTime += time.Since(clock)
		s.stats.EndpointsScored += len(sc.unscored)
		if tr := s.stats.trace; tr != nil {
			tr.scored(sc, side.postingOf, dists)
		}
	}
	return nil
}

// collectEndpoints lists in sc.unscored the endpoints the sources of a loaded
// posting lead to that have to be scored: those outside the loaded postings,
// not scored before, in the order of the sources and of their edges. Each
// is marked scored as it is listed. The sources are the members within the
// cut, whose estimates are in sc.memberDist.
func (sc *searchScratch) collectEndpoints(postingOf []uint32, post *posting, cut float32) {
	sc.unscored = sc.unscored[:0]
	for i, dist := range sc.memberDist {
		if dist > cut {
			continue
		}
		for _, endpoint := range post.edgesOf(i) {
			if sc.loaded[postingOf[endpoint]] || sc.endpoints.CheckAndVisit(uint64(endpoint)) {
				continue
			}
			sc.unscored = append(sc.unscored, endpoint)
		}
	}
}

// rankPostings folds the estimates of the endpoints in sc.unscored into the
// ranks of their postings, in order: a posting keeps the best estimate.
func (sc *searchScratch) rankPostings(postingOf []uint32, dists []float32) {
	for i, endpoint := range sc.unscored {
		dist := dists[i]
		target := postingOf[endpoint]
		if dist < sc.rank[target] {
			if sc.rank[target] == inf32 {
				sc.frontier = append(sc.frontier, target)
			}
			sc.rank[target] = dist
		}
	}
}

// rescoreSpread tells where the candidates of the final rescore live: in how
// many of the loaded postings, and how many of them in none. It walks the
// loaded postings, so it is only for searches that collect stats.
func (st *state) rescoreSpread(sc *searchScratch, candidates []scored) (postings, elsewhere int) {
	wanted := make(map[uint64]struct{}, len(candidates))
	for _, cand := range candidates {
		wanted[cand.id] = struct{}{}
	}
	found := 0
	for _, p := range sc.touched {
		holds := false
		for _, docID := range st.postings.load(p).docIDs {
			if _, ok := wanted[docID]; ok {
				found++
				holds = true
			}
		}
		if holds {
			postings++
		}
	}
	return postings, len(candidates) - found
}

// rescore replaces the estimated distance of every candidate with the exact
// one. Like the HNSW rescore it fans out within the query's concurrency
// budget, which pays off when the vectors are read from disk.
func (ix *Index) rescore(ctx context.Context, query []float32, candidates []scored) error {
	view := ix.cfg.GetViewThunk()
	defer view.ReleaseView()

	workers := max(1, min(concurrency.BudgetFromCtx(ctx, ix.rescoreConcurrency),
		ix.rescoreConcurrency, len(candidates)))
	if workers == 1 {
		for i := range candidates {
			dist, err := ix.exactDistance(ctx, query, candidates[i].id, view)
			if err != nil {
				return err
			}
			candidates[i].dist = dist
		}
		return nil
	}

	eg := enterrors.NewErrorGroupWrapper(ix.logger)
	for worker := range workers {
		eg.Go(func() error {
			for i := worker; i < len(candidates); i += workers {
				if err := ctx.Err(); err != nil {
					return errors.Wrap(err, "rescore")
				}
				dist, err := ix.exactDistance(ctx, query, candidates[i].id, view)
				if err != nil {
					return err
				}
				candidates[i].dist = dist
			}
			return nil
		})
	}
	return eg.Wait()
}

// exactDistance reads the full-precision vector of a doc and returns its
// distance to the query. A vector that cannot be read fails the search:
// without deletes there is no legitimate reason for it to be missing.
func (ix *Index) exactDistance(ctx context.Context, query []float32, docID uint64,
	view common.BucketView,
) (float32, error) {
	container := ix.tempVectors.Get(len(query))
	defer ix.tempVectors.Put(container)

	vec, err := ix.cfg.TempVectorForIDWithViewThunk(ctx, docID, container, view)
	if err != nil {
		return 0, errors.Wrapf(err, "get vector of doc %d", docID)
	}
	if len(vec) != len(query) {
		return 0, errors.Errorf("vector of doc %d has length %d, expected %d", docID, len(vec), len(query))
	}
	if ix.cosine {
		// vec is the pooled container the thunk filled, as in HNSW.
		distancer.NormalizeInPlace(vec)
	}
	return ix.provider.SingleDist(query, vec)
}
