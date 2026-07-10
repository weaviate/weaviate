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

package hfresh

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

const (
	// minimum max distance to use when pruning
	pruningMinMaxDistance = 0.1
	flatSearchCutoff      = 5_000

	// defaultRescoreConcurrency bounds how many full-vector fetches one
	// query's rescore phase issues in parallel. Overridable via
	// HFRESH_RESCORE_CONCURRENCY (1 = sequential).
	defaultRescoreConcurrency = 16

	// defaultRescoreMin is the default minimum number of candidates rescored
	// before the adaptive cutoff is trusted. The effective floor is
	// max(4*k, rescoreMin). Overridable via HFRESH_RESCORE_MIN.
	//
	// Calibrated on dbpedia-1M (1536d) and snowflake-8.7M (768d): 128 holds
	// recall within noise (±0.05pp) of a full rescore at every searchProbe
	// while keeping most of the adaptive win (+32% QPS at probe 16 on
	// dbpedia); 64 trails full rescore by 0.1-0.2pp at high probes.
	defaultRescoreMin = 128

	// defaultRescoreMarginFactor scales the worst observed overestimation
	// (estimate - exact) into the safety margin added to the k-th exact
	// distance before the adaptive cutoff fires. Overridable via
	// HFRESH_RESCORE_MARGIN_FACTOR.
	defaultRescoreMarginFactor = 2.0
)

type fetchVectorFunc func(ctx context.Context, id uint64) ([]float32, error)

// rescoreCandidates computes exact distances for the RQ1 top candidates in q
// and returns the k best. Full vectors are fetched with bounded parallelism;
// when the index has view-based access configured, all fetches of one query
// share a single consistent bucket view instead of acquiring one each.
//
// The rescore runs as a single streaming worker pool with no per-batch
// barriers: workers claim candidate indices from a shared atomic counter in
// ascending (estimate) order and fetch/rescore them independently. Shared
// state (the top-k set, the worst observed overestimation, the completed
// count and the cutoff threshold) lives behind one mutex; it is touched only
// a few hundred times per query, so the contention is negligible.
//
// With adaptive rescore enabled, a worker skips (and publishes a stop signal
// for) the remaining candidates once the next candidate's estimate provably
// cannot enter the top k: candidates[i].Distance > kthExact +
// factor*max(maxOverestimation, 0). The cutoff is only trusted after both
// (a) at least max(4*k, rescoreMin) candidates have been rescored and (b) the
// top-k set is full.
//
// Streaming makes the exact skip point nondeterministic: fetches already in
// flight when the cutoff fires still complete and are folded in. That only
// ever ADDS information — the resulting top-k can only be equal-or-better
// informed than the strictest (barrier-based) cutoff — so recall is never
// harmed by the nondeterminism itself; only the count of skipped candidates
// varies slightly between runs.
func (h *HFresh) rescoreCandidates(ctx context.Context, query []float32, q *ResultSet, k int, st *searchStats) (*ResultSet, error) {
	// candidates arrive sorted by estimated distance, ascending
	candidates := make([]Result, q.Len())
	copy(candidates, q.data)

	var fetchFor func(worker int) fetchVectorFunc
	if h.getViewThunk != nil && h.vectorForIDWithView != nil {
		view := h.getViewThunk()
		defer view.ReleaseView()

		// one reusable read buffer per worker
		buffers := make([]common.VectorSlice, max(h.rescoreConcurrency, 1))
		for i := range buffers {
			buffers[i].Buff8 = make([]byte, 8)
		}
		fetchFor = func(w int) fetchVectorFunc {
			return func(ctx context.Context, id uint64) ([]float32, error) {
				return h.vectorForIDWithView(ctx, id, &buffers[w], view)
			}
		}
	} else {
		fetchFor = func(int) fetchVectorFunc {
			return fetchVectorFunc(h.vectorForId)
		}
	}

	concurrency := max(min(h.rescoreConcurrency, len(candidates)), 1)
	workerStats := make([]searchStats, concurrency)

	rescored := NewResultSet(k)
	minRescore := max(4*k, h.rescoreMin)

	var (
		mu sync.Mutex
		// worst observed overestimation of the RQ1 estimate: how far above
		// the exact distance an estimate has been seen to land. A candidate
		// can only be wrongly skipped if its estimate overshoots by more than
		// the margin derived from this.
		maxOverestimation float32
		completed         int     // candidates rescored so far
		threshold         float32 // cutoff, valid only once thresholdReady
		thresholdReady    bool
	)

	// next hands out candidate indices in ascending order; stop is published
	// by the first worker whose claimed candidate falls past the cutoff, and
	// observed by every other worker so they exit promptly.
	var next atomic.Int64
	var stop atomic.Bool

	work := func(w int) error {
		fetch := fetchFor(w)
		ws := &workerStats[w]

		for {
			if h.adaptiveRescore && stop.Load() {
				return nil
			}

			i := int(next.Add(1)) - 1
			if i >= len(candidates) {
				return nil
			}

			if h.adaptiveRescore {
				mu.Lock()
				skip := thresholdReady && candidates[i].Distance > threshold
				mu.Unlock()
				if skip {
					// All later candidates have an even larger estimate, so
					// they fail the same bound. Signal the pool to wind down.
					stop.Store(true)
					return nil
				}
			}

			vec, err := fetch(ctx, candidates[i].ID)
			if err != nil {
				// The object may have been deleted between the posting scan
				// and the rescore step (race condition). Skip stale entries
				// gracefully.
				var notFound storobj.ErrNotFound
				if errors.As(err, &notFound) {
					ws.RescoreNotFound++
					continue
				}
				return err
			}
			ws.RescoreFetched++
			vec = h.normalizeVec(vec)
			dist, err := h.distancer.distancer.SingleDist(query, vec)
			if err != nil {
				return err
			}

			mu.Lock()
			rescored.Insert(candidates[i].ID, dist)
			if over := candidates[i].Distance - dist; over > maxOverestimation {
				maxOverestimation = over
			}
			completed++
			if completed >= minRescore && rescored.Len() >= k {
				threshold = rescored.data[k-1].Distance + h.rescoreMarginFactor*max(maxOverestimation, 0)
				thresholdReady = true
			}
			mu.Unlock()
		}
	}

	if concurrency == 1 {
		if err := work(0); err != nil {
			return nil, err
		}
	} else {
		eg := enterrors.NewErrorGroupWrapper(h.logger)
		for w := range concurrency {
			eg.Go(func() error {
				return work(w)
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
	}

	var fetched, notFound int
	for w := range concurrency {
		fetched += int(workerStats[w].RescoreFetched)
		notFound += int(workerStats[w].RescoreNotFound)
		st.add(&workerStats[w])
	}
	st.RescoreSkipped = uint32(len(candidates) - fetched - notFound)

	return rescored, nil
}

func (h *HFresh) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	vector = h.normalizeVec(vector)

	// this must run before any search path reads the quantizer or distancer,
	// including flatSearch, which uses the distancer
	_, quantizer := h.loadQuantizer()
	if quantizer == nil {
		return nil, nil, nil
	}

	if allowList != nil && allowList.Len() < flatSearchCutoff {
		return h.flatSearch(ctx, vector, k, allowList)
	}

	// k > rescoreLimit must not cap the result set at rescoreLimit results
	// (issue #277). Any rework of this path (e.g. decoupled routing/rerank
	// budgets) must preserve max(k, rescoreLimit) semantics for the
	// candidate depth.
	rescoreLimit := max(k, int(h.rescoreLimit))
	queryDistancer := quantizer.NewDistancer(vector)

	var selectedCentroids []uint64

	// st collects this query's phase timings and IO counters; it always
	// exists (cheap stack counters) but is only aggregated when profiling
	// is enabled, and only exported when metrics are enabled.
	var st searchStats
	phaseStart := time.Now()

	// If k is larger than the configured number of candidates, use k as the candidate number
	// to enlarge the search space.
	candidateCentroidNum := max(k, int(h.searchProbe))

	nAllowList := allowList
	if allowList != nil {
		nAllowList = h.wrapAllowList(ctx, allowList)
		defer nAllowList.Close()
	}
	centroids, err := h.Centroids.Search(vector, candidateCentroidNum, nAllowList)
	if err != nil {
		return nil, nil, err
	}
	st.Centroid = time.Since(phaseStart)
	if len(centroids.data) == 0 {
		return nil, nil, nil
	}

	q := NewResultSet(rescoreLimit)

	// compute the max distance to filter out candidates that are too far away
	maxDist := centroids.data[0].Distance * h.config.MaxDistanceRatio

	// filter out candidates that are too far away or have no vectors
	phaseStart = time.Now()
	selectedCentroids = make([]uint64, 0, candidateCentroidNum)
	for i := 0; i < len(centroids.data) && len(selectedCentroids) < candidateCentroidNum; i++ {
		if maxDist > pruningMinMaxDistance && centroids.data[i].Distance > maxDist {
			continue
		}
		count, err := h.PostingSizes.Get(ctx, centroids.data[i].ID)
		if err != nil {
			return nil, nil, err
		}
		if count == 0 {
			continue
		}

		selectedCentroids = append(selectedCentroids, centroids.data[i].ID)
	}
	st.Filter = time.Since(phaseStart)
	st.PostingsRequested = uint32(len(selectedCentroids))

	// Read the selected postings with bounded parallelism and scan each one
	// as soon as it arrives, overlapping the scan with the remaining reads.
	// A single consumer preserves the visited-set and merge-enqueue semantics
	// of the sequential scan; final results are unaffected by arrival order
	// since the rescore phase recomputes exact distances.
	phaseStart = time.Now()
	postingCh, wait := h.PostingStore.MultiGetStreamWithStats(ctx, selectedCentroids, &st)

	visited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(visited)

	var decompressBuf []uint64
	var scanDur time.Duration

	for res := range postingCh {
		p := res.Posting
		if p == nil { // posting nil if not found
			continue
		}

		scanStart := time.Now()

		// keep track of the posting size
		postingSize := len(p)
		st.Candidates += uint32(len(p))

		for _, v := range p {
			id := v.ID()
			// skip deleted vectors
			deleted, err := h.VersionMap.IsDeleted(context.Background(), id)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to check if vector %d is deleted", id)
			}
			if deleted {
				postingSize--
				st.Deleted++
				continue
			}

			// skip duplicates
			if visited.CheckAndVisit(id) {
				st.Duplicates++
				continue
			}

			// skip vectors that are not in the allow list
			if allowList != nil && !allowList.Contains(id) {
				st.AllowlistSkipped++
				continue
			}

			decompressBuf = quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
			dist, err := queryDistancer.Distance(decompressBuf)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
			}

			q.Insert(id, dist)
		}

		// if the posting size is lower than the configured minimum,
		// enqueue a merge operation
		if postingSize < int(h.minPostingSize) {
			err = h.taskQueue.EnqueueMerge(selectedCentroids[res.Index])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", selectedCentroids[res.Index])
			}
		}

		scanDur += time.Since(scanStart)
	}
	if err := wait(); err != nil {
		return nil, nil, err
	}

	// Scan is pure scan time; Read is the remainder of the pipeline window,
	// i.e. time the consumer spent waiting on posting reads.
	st.Scan = scanDur
	st.Read = time.Since(phaseStart) - scanDur

	phaseStart = time.Now()
	rescored, err := h.rescoreCandidates(ctx, vector, q, k, &st)
	if err != nil {
		return nil, nil, err
	}
	st.Rescore = time.Since(phaseStart)

	ids := make([]uint64, rescored.Len())
	dists := make([]float32, rescored.Len())
	i := 0
	for id, dist := range rescored.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}

	st.Results = uint32(len(ids))
	h.profiler.record(&st)
	h.metrics.SearchPhases(&st)

	return ids, dists, nil
}

func (h *HFresh) SearchByVectorDistance(
	ctx context.Context,
	vector []float32,
	targetDistance float32,
	maxLimit int64,
	allow helpers.AllowList,
) ([]uint64, []float32, error) {
	searchParams := common.NewSearchByDistParams(0, common.DefaultSearchByDistInitialLimit, common.DefaultSearchByDistInitialLimit, maxLimit)
	var resultIDs []uint64
	var resultDist []float32

	recursiveSearch := func() (bool, error) {
		totalLimit := searchParams.TotalLimit()
		ids, dist, err := h.SearchByVector(ctx, vector, totalLimit, allow)
		if err != nil {
			return false, errors.Wrap(err, "vector search")
		}

		// if there is less results than given limit search can be stopped
		shouldContinue := len(ids) >= totalLimit

		// ensures the indexes aren't out of range
		offsetCap := searchParams.OffsetCapacity(ids)
		totalLimitCap := searchParams.TotalLimitCapacity(ids)

		if offsetCap == totalLimitCap {
			return false, nil
		}

		ids, dist = ids[offsetCap:totalLimitCap], dist[offsetCap:totalLimitCap]
		for i := range ids {
			if aboveThresh := dist[i] <= targetDistance; aboveThresh ||
				floatcomp.InDelta(float64(dist[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dist[i])
			} else {
				// as soon as we encounter a certainty which
				// is below threshold, we can stop searching
				shouldContinue = false
				break
			}
		}

		return shouldContinue, nil
	}

	var shouldContinue bool
	var err error
	for shouldContinue, err = recursiveSearch(); shouldContinue && err == nil; {
		searchParams.Iterate()
		if searchParams.MaxLimitReached() {
			h.logger.
				WithField("action", "unlimited_vector_search").
				Warnf("maximum search limit of %d results has been reached",
					searchParams.MaximumSearchLimit())
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}

	return resultIDs, resultDist, nil
}

type Result struct {
	ID       uint64
	Distance float32
}

// ResultSet maintains the k smallest elements by distance in a sorted array.
// It creates a fixed-size array of length k and inserts new elements in sorted order.
// It performs about 3x faster than the priority queue approach, as it avoids
// the overhead of heap operations and memory allocations.
type ResultSet struct {
	data []Result
	k    int
}

func NewResultSet(k int) *ResultSet {
	return &ResultSet{
		data: make([]Result, 0, k),
		k:    k,
	}
}

// Insert adds a new element, maintaining only k smallest elements by distance
func (ks *ResultSet) Insert(id uint64, dist float32) {
	item := Result{ID: id, Distance: dist}

	// If array isn't full yet, just insert in sorted position
	if len(ks.data) < ks.k {
		pos := ks.searchByDistance(dist)
		ks.data = append(ks.data, Result{})
		copy(ks.data[pos+1:], ks.data[pos:])
		ks.data[pos] = item
		return
	}

	// If array is full, only insert if distance is smaller than max (last element)
	if dist < ks.data[ks.k-1].Distance {
		pos := ks.searchByDistance(dist)
		// Shift elements to the right and insert
		copy(ks.data[pos+1:], ks.data[pos:ks.k-1])
		ks.data[pos] = item
	}
}

// searchByDistance finds the insertion position for a given distance
func (ks *ResultSet) searchByDistance(dist float32) int {
	left, right := 0, len(ks.data)
	for left < right {
		mid := (left + right) / 2
		if ks.data[mid].Distance < dist {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

func (ks *ResultSet) Len() int {
	return len(ks.data)
}

func (ks *ResultSet) Iter() iter.Seq2[uint64, float32] {
	return func(yield func(uint64, float32) bool) {
		for _, item := range ks.data {
			if !yield(item.ID, item.Distance) {
				break
			}
		}
	}
}

func (ks *ResultSet) Reset(k int) {
	ks.data = ks.data[:0]
	if cap(ks.data) < k {
		ks.data = make([]Result, 0, k)
	}
	ks.k = k
}
