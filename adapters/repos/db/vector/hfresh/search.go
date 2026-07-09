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
	"encoding/binary"
	"iter"
	"math"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

const (
	// minimum max distance to use when pruning
	pruningMinMaxDistance = 0.1
	flatSearchCutoff      = 5_000

	// The intermediate rescore stage re-ranks the RQ1 top-M against the
	// full stored FDEs before the MaxSim rescore. M is derived from
	// rerankBudget rather than exposed as a knob: the recall study's
	// cascade sensitivity (X5) shows M = 6 x the default rerankBudget
	// (~2048) sits within ~0.3pp of re-ranking the whole scanned pool,
	// while 1024 is the smallest pool measured to stay within ~0.7pp.
	intermediateRescoreFactor  = 6
	intermediateRescoreMinPool = 1024
)

func (h *HFresh) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	// Normalize before any search path to ensure consistent distance calculations
	vector = h.normalizeVec(vector)

	if !h.muvera.Load() && allowList != nil && allowList.Len() < flatSearchCutoff {
		return h.flatSearch(ctx, vector, k, allowList)
	}

	// The candidate pool must be at least as large as the requested k:
	// rescoreLimit is a quality floor for the RQ1 candidate depth, not a cap
	// on results. Using rescoreLimit alone silently capped searches with
	// k > rescoreLimit at rescoreLimit results (issue #277). Any rework of
	// this path (e.g. decoupled routing/rerank budgets) must preserve
	// max(k, rescoreLimit) semantics for the candidate depth.
	rescoreLimit := max(k, int(h.rescoreLimit))
	if h.quantizer == nil {
		if atomic.LoadUint32(&h.dims) == 0 {
			return nil, nil, nil
		}
		return nil, nil, errors.New("quantizer not initialized")
	}
	queryDistancer := h.quantizer.NewDistancer(vector)

	var selectedCentroids []uint64
	var postings []Posting

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
	if len(centroids.data) == 0 {
		return nil, nil, nil
	}

	q := NewResultSet(rescoreLimit)

	// compute the max distance to filter out candidates that are too far away
	maxDist := centroids.data[0].Distance * h.config.MaxDistanceRatio

	// filter out candidates that are too far away or have no vectors
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

	// read all the selected postings
	postings, err = h.PostingStore.MultiGet(ctx, selectedCentroids)
	if err != nil {
		return nil, nil, err
	}

	visited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(visited)

	var decompressBuf []uint64

	for i, p := range postings {
		if p == nil { // posting nil if not found
			continue
		}

		// keep track of the posting size
		postingSize := len(p)

		for _, v := range p {
			id := v.ID()
			// skip deleted vectors
			deleted, err := h.VersionMap.IsDeleted(context.Background(), id)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to check if vector %d is deleted", id)
			}
			if deleted {
				postingSize--
				continue
			}

			// skip duplicates
			if visited.CheckAndVisit(id) {
				continue
			}

			// skip vectors that are not in the allow list
			if allowList != nil && !allowList.Contains(id) {
				continue
			}

			decompressBuf = h.quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
			dist, err := queryDistancer.Distance(decompressBuf)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
			}

			q.Insert(id, dist)
		}

		// if the posting size is lower than the configured minimum,
		// enqueue a merge operation
		if postingSize < int(h.minPostingSize) {
			err = h.taskQueue.EnqueueMerge(selectedCentroids[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", selectedCentroids[i])
			}
		}
	}

	if h.muvera.Load() {
		ids := make([]uint64, 0, q.Len())
		dists := make([]float32, 0, q.Len())
		for id, dist := range q.Iter() {
			ids = append(ids, id)
			dists = append(dists, dist)
		}
		return ids, dists, nil
	}

	rescored := NewResultSet(k)
	for id := range q.Iter() {
		vec, err := h.vectorForId(ctx, id)
		if err != nil {
			// The object may have been deleted between the posting scan and the
			// rescore step (race condition). Skip stale entries gracefully.
			var notFound storobj.ErrNotFound
			if errors.As(err, &notFound) {
				continue
			}
			return nil, nil, err
		}
		vec = h.normalizeVec(vec)
		dist, err := h.distancer.distancer.SingleDist(vector, vec)
		if err != nil {
			return nil, nil, err
		}
		rescored.Insert(id, dist)
	}

	ids := make([]uint64, rescored.Len())
	dists := make([]float32, rescored.Len())
	i := 0
	for id, dist := range rescored.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}

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

// Insert adds a new element, maintaining only k smallest elements by
// (distance, id). Ties on distance are broken by ascending id so that result
// ordering is deterministic regardless of insertion order.
func (ks *ResultSet) Insert(id uint64, dist float32) {
	item := Result{ID: id, Distance: dist}

	// If array isn't full yet, just insert in sorted position
	if len(ks.data) < ks.k {
		pos := ks.searchByDistance(dist, id)
		ks.data = append(ks.data, Result{})
		copy(ks.data[pos+1:], ks.data[pos:])
		ks.data[pos] = item
		return
	}

	// If array is full, only insert if (dist, id) sorts before the max (last element)
	last := ks.data[ks.k-1]
	if dist < last.Distance || (dist == last.Distance && id < last.ID) {
		pos := ks.searchByDistance(dist, id)
		// Shift elements to the right and insert
		copy(ks.data[pos+1:], ks.data[pos:ks.k-1])
		ks.data[pos] = item
	}
}

// searchByDistance finds the insertion position for a given (distance, id) pair
func (ks *ResultSet) searchByDistance(dist float32, id uint64) int {
	left, right := 0, len(ks.data)
	for left < right {
		mid := (left + right) / 2
		if ks.data[mid].Distance < dist ||
			(ks.data[mid].Distance == dist && ks.data[mid].ID < id) {
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

func (h *HFresh) SearchByMultiVector(ctx context.Context, vectors [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	if !h.muvera.Load() {
		return nil, nil, ErrMuveraNotEnabled
	}

	// The muvera encoder is initialized by the first AddMulti (or restored
	// from persisted metadata at startup). Until then its projection
	// matrices are nil and EncodeQuery would panic (issue #275). dims is
	// only set after the encoder is initialized and persisted — atomically,
	// unlike muveraEncoder.Dimensions() — so a non-zero value guarantees the
	// encoder is ready without racing a concurrent first insert.
	if atomic.LoadUint32(&h.dims) == 0 {
		return nil, nil, ErrMuveraNotInitialized
	}

	// cosine requires normalized tokens for both the FDE encoding and the
	// MaxSim rescore (issue #276)
	vectors = h.normalizeMultiVec(vectors)

	// Encode query vectors into FDE (Fast Dense Encoding) representation
	queryFDE := h.muveraEncoder.EncodeQuery(vectors)

	routingBudget, rerankBudget := h.muveraSearchBudgets(k)

	candidateIDs, err := h.searchByFDE(ctx, queryFDE, routingBudget, rerankBudget, allow)
	if err != nil {
		return nil, nil, errors.Wrap(err, "muvera candidate search")
	}

	return h.computeLateInteraction(ctx, vectors, k, candidateIDs)
}

// muveraSearchBudgets computes the decoupled budgets for a multi-vector
// search:
//   - routingBudget (centroid selection and posting scan breadth) is
//     max(k, searchProbe). searchProbe defaults to 256 at schema parse time
//     and is respected in BOTH directions: a low explicit probe genuinely
//     reduces routing work (latency/recall knob), it is not floored by
//     rescoreLimit. This matches the pre-decoupling behavior, where the
//     centroid count was max(k, searchProbe).
//   - rerankBudget (RQ1 candidate depth for the MaxSim rescore) is
//     max(k, rescoreLimit).
//
// Both budgets must include the user-requested k: rescoreLimit is a quality
// floor for the RQ1 candidate depth, not a cap on results. Budgets below k
// silently capped searches with k > rescoreLimit at rescoreLimit results
// (issue #277). Any further rework of this path must preserve max(k, ...)
// semantics for both budgets.
func (h *HFresh) muveraSearchBudgets(k int) (routingBudget, rerankBudget int) {
	searchProbe := int(atomic.LoadUint32(&h.searchProbe))
	rescoreLimit := int(atomic.LoadUint32(&h.rescoreLimit))
	return max(k, searchProbe), max(k, rescoreLimit)
}

// searchByFDE performs FDE-based search with explicitly decoupled budgets.
//
// Parameters:
//   - queryFDE: the FDE-encoded query vector
//   - routingBudget: number of centroids to select (controls posting coverage)
//   - rerankBudget: number of candidates to keep after RQ1 ranking (controls MaxSim depth)
//   - allow: optional allow list for filtering
//
// This separation allows independent tuning of routing breadth vs reranking depth.
// The routing stage (centroid selection, posting scan) is controlled only by routingBudget.
// The ranking stage (RQ1 top-N, candidates for late interaction) is controlled only by rerankBudget.
//
// Posting Expansion:
// After the initial RQ1 ranking, the top candidates' associated postings are identified
// using a reverse map (docID -> postingIDs). Additional postings are scanned to discover
// more candidates that may have been missed by the initial centroid selection.
func (h *HFresh) searchByFDE(
	ctx context.Context,
	queryFDE []float32,
	routingBudget int,
	rerankBudget int,
	allowList helpers.AllowList,
) ([]uint64, error) {
	queryFDE = h.normalizeVec(queryFDE)
	if h.quantizer == nil {
		if atomic.LoadUint32(&h.dims) == 0 {
			return nil, nil
		}
		return nil, errors.New("quantizer not initialized")
	}
	queryDistancer := h.quantizer.NewDistancer(queryFDE)

	// Step 1: Centroid selection - controlled by routingBudget only
	nAllowList := allowList
	if allowList != nil {
		nAllowList = h.wrapAllowList(ctx, allowList)
		defer nAllowList.Close()
	}

	centroids, err := h.Centroids.Search(queryFDE, routingBudget, nAllowList)
	if err != nil {
		return nil, err
	}
	if len(centroids.data) == 0 {
		return nil, nil
	}

	// Step 2: Filter centroids by distance and posting existence
	maxDist := centroids.data[0].Distance * h.config.MaxDistanceRatio
	selectedCentroids := make([]uint64, 0, routingBudget)
	for i := 0; i < len(centroids.data) && len(selectedCentroids) < routingBudget; i++ {
		if maxDist > pruningMinMaxDistance && centroids.data[i].Distance > maxDist {
			continue
		}
		count, err := h.PostingSizes.Get(ctx, centroids.data[i].ID)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			continue
		}
		selectedCentroids = append(selectedCentroids, centroids.data[i].ID)
	}

	// Step 3: Scan postings from selected centroids
	postings, err := h.PostingStore.MultiGet(ctx, selectedCentroids)
	if err != nil {
		return nil, err
	}

	visited := h.visitedPool.Borrow()
	defer h.visitedPool.Return(visited)

	// Step 4: RQ1 approximate ranking - controlled by rerankBudget only
	// This is the key decoupling: routingBudget affects centroid count,
	// rerankBudget affects how many candidates we keep after RQ1 scoring.
	// With the intermediate rescore stage the RQ1 pool is widened to M so
	// that 1-bit ranking errors can be corrected against the full FDEs
	// before the top-rerankBudget cut (see docs/hfresh-intermediate-rescore.md).
	poolSize := rerankBudget
	if !h.disableIntermediateRescore {
		poolSize = max(intermediateRescoreFactor*rerankBudget, intermediateRescoreMinPool)
	}
	q := NewResultSet(poolSize)

	var decompressBuf []uint64

	for i, p := range postings {
		if p == nil {
			continue
		}

		postingSize := len(p)

		for _, v := range p {
			id := v.ID()

			deleted, err := h.VersionMap.IsDeleted(ctx, id)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to check if vector %d is deleted", id)
			}
			if deleted {
				postingSize--
				continue
			}

			if visited.CheckAndVisit(id) {
				continue
			}

			if allowList != nil && !allowList.Contains(id) {
				continue
			}

			decompressBuf = h.quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
			dist, err := queryDistancer.Distance(decompressBuf)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
			}

			q.Insert(id, dist)
		}

		if postingSize < int(h.minPostingSize) {
			if err := h.taskQueue.EnqueueMerge(selectedCentroids[i]); err != nil {
				return nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", selectedCentroids[i])
			}
		}
	}

	if h.disableIntermediateRescore {
		// test seam: previous pipeline shape, RQ1 top-rerankBudget straight
		// to MaxSim
		ids := make([]uint64, 0, q.Len())
		for id := range q.Iter() {
			ids = append(ids, id)
		}
		return ids, nil
	}

	// Step 5: intermediate rescore - re-rank the RQ1 top-M against the full
	// stored FDEs and keep the top rerankBudget for late interaction
	return h.rescoreFDECandidates(queryFDE, q, rerankBudget)
}

// rescoreFDECandidates re-ranks RQ1 candidates against the full FDEs stored
// in the muvera bucket, mirroring the single-vector path's rescore step.
// The query FDE arrives normalized (cosine) or raw (l2); stored FDEs are
// the raw encoder output, so the cosine case folds the document norm into
// the dot product instead of materializing normalized copies (same pattern
// as maxSimScoreCosine, two SIMD dot products per candidate, allocation-free
// apart from the bucket read).
//
// Extension point for the pending RQn-as-truth decision: only this method
// changes (a code bucket read plus an RQ distancer instead of the float
// bucket plus the fold); the pipeline shape stays as is.
func (h *HFresh) rescoreFDECandidates(queryFDE []float32, candidates *ResultSet, rerankBudget int) ([]uint64, error) {
	bucket := h.store.Bucket(h.id + "_muvera_vectors")
	if bucket == nil {
		return nil, errors.New("intermediate rescore: muvera vectors bucket not found")
	}

	cosine := h.config.DistanceProvider.Type() == "cosine-dot"
	rescored := NewResultSet(rerankBudget)
	keyBuf := make([]byte, 8)
	var fdeBuf []float32

	for id := range candidates.Iter() {
		binary.BigEndian.PutUint64(keyBuf, id)
		raw, err := bucket.Get(keyBuf)
		if err != nil {
			return nil, errors.Wrapf(err, "intermediate rescore: fde for vector %d", id)
		}
		if len(raw) == 0 {
			// deleted between the posting scan and this stage; skip stale
			// entries gracefully, like the single-vector rescore does
			continue
		}
		fdeBuf = float32SliceInto(fdeBuf, raw)

		var dist float32
		if cosine {
			negDot, err := dotProvider.SingleDist(queryFDE, fdeBuf)
			if err != nil {
				return nil, errors.Wrapf(err, "intermediate rescore: distance for vector %d", id)
			}
			negNormSq, err := dotProvider.SingleDist(fdeBuf, fdeBuf)
			if err != nil {
				return nil, errors.Wrapf(err, "intermediate rescore: norm for vector %d", id)
			}
			if negNormSq < 0 {
				inv := float32(1 / math.Sqrt(float64(-negNormSq)))
				dist = 1 + negDot*inv
				if dist < 0 {
					// mirror the cosine-dot provider's clamp of negative
					// rounding artifacts
					dist = 0
				}
			} else {
				dist = 1 // zero-norm FDE: orthogonal by convention
			}
		} else {
			dist, err = h.config.DistanceProvider.SingleDist(queryFDE, fdeBuf)
			if err != nil {
				return nil, errors.Wrapf(err, "intermediate rescore: distance for vector %d", id)
			}
		}

		rescored.Insert(id, dist)
	}

	ids := make([]uint64, 0, rescored.Len())
	for id := range rescored.Iter() {
		ids = append(ids, id)
	}
	return ids, nil
}

// float32SliceInto decodes little-endian float32 bytes into dst, reusing its
// capacity when possible.
func float32SliceInto(dst []float32, raw []byte) []float32 {
	n := len(raw) / 4
	if cap(dst) < n {
		dst = make([]float32, n)
	} else {
		dst = dst[:n]
	}
	byteops.CopyBytesToSlice(dst, raw)
	return dst
}

func (h *HFresh) SearchByMultiVectorDistance(ctx context.Context, vectors [][]float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	if !h.muvera.Load() {
		return nil, nil, ErrMuveraNotEnabled
	}

	searchParams := common.NewSearchByDistParams(0, common.DefaultSearchByDistInitialLimit, common.DefaultSearchByDistInitialLimit, maxLimit)
	var resultIDs []uint64
	var resultDist []float32

	recursiveSearch := func() (bool, error) {
		totalLimit := searchParams.TotalLimit()
		ids, dists, err := h.SearchByMultiVector(ctx, vectors, totalLimit, allow)
		if err != nil {
			return false, errors.Wrap(err, "multi-vector search")
		}

		shouldContinue := len(ids) >= totalLimit

		offsetCap := searchParams.OffsetCapacity(ids)
		totalLimitCap := searchParams.TotalLimitCapacity(ids)
		if offsetCap == totalLimitCap {
			return false, nil
		}

		ids, dists = ids[offsetCap:totalLimitCap], dists[offsetCap:totalLimitCap]
		for i := range ids {
			if dists[i] <= targetDistance ||
				floatcomp.InDelta(float64(dists[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dists[i])
			} else {
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
				WithField("action", "unlimited_multi_vector_search").
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

func (h *HFresh) QueryMultiVectorDistancer(queryVectors [][]float32) common.QueryVectorDistancer {
	if !h.muvera.Load() {
		return common.QueryVectorDistancer{
			DistanceFunc: func(uint64) (float32, error) {
				return 0, ErrMuveraNotEnabled
			},
		}
	}

	queryVectors = h.normalizeMultiVec(queryVectors)
	distFunc := func(id uint64) (float32, error) {
		docVectors, err := h.multivectorForIdThunk(h.ctx, id)
		if err != nil {
			return 0, errors.Wrapf(err, "get multi-vector for id %d", id)
		}
		return h.maxSimScore(queryVectors, docVectors)
	}

	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}

func (h *HFresh) computeLateInteraction(ctx context.Context, queryVectors [][]float32, k int, candidateIDs []uint64) ([]uint64, []float32, error) {
	results := NewResultSet(k)

	for _, id := range candidateIDs {
		docVectors, err := h.multivectorForIdThunk(ctx, id)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "get multi-vector for id %d", id)
		}
		score, err := h.maxSimScore(queryVectors, docVectors)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "compute maxsim for id %d", id)
		}
		results.Insert(id, score)
	}

	ids := make([]uint64, results.Len())
	dists := make([]float32, results.Len())
	i := 0
	for id, dist := range results.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}

	return ids, dists, nil
}

// maxSimScore computes the MaxSim score between query and document multi-vectors.
// For each query token, it finds the minimum distance to any document token, then
// sums across all query tokens. Lower score = more similar.
// Callers must pass query tokens already normalized (normalizeMultiVec) when
// the configured distance is cosine; document tokens are handled here.
func (h *HFresh) maxSimScore(queryVectors, docVectors [][]float32) (float32, error) {
	if h.config.DistanceProvider.Type() == "cosine-dot" {
		return h.maxSimScoreCosine(queryVectors, docVectors)
	}

	var score float32
	for _, queryToken := range queryVectors {
		d := h.config.DistanceProvider.New(queryToken)
		minDist := float32(math.MaxFloat32)
		for _, docToken := range docVectors {
			dist, err := d.Distance(docToken)
			if err != nil {
				return 0, err
			}
			if dist < minDist {
				minDist = dist
			}
		}
		score += minDist
	}
	return score, nil
}

var dotProvider = distancer.NewDotProductProvider()

// maxSimScoreCosine computes the cosine MaxSim by folding each document
// token's inverse norm into the dot product: dist(q̂, d) = 1 - dot(q̂, d)/‖d‖,
// with query tokens pre-normalized by the caller. This avoids materializing
// normalized copies of every candidate's tokens on the rescore hot path,
// which visits ~rescoreLimit documents per query — a separate normalization
// pass measurably regressed query latency (issue #276 follow-up). Zero-norm
// tokens keep an inverse norm of 0, matching distancer.Normalize, which
// leaves zero vectors untouched (distance 1).
func (h *HFresh) maxSimScoreCosine(queryVectors [][]float32, docVectors [][]float32) (float32, error) {
	invNorms := make([]float32, len(docVectors))
	for j, docToken := range docVectors {
		negNormSq, err := dotProvider.SingleDist(docToken, docToken)
		if err != nil {
			return 0, err
		}
		if negNormSq < 0 {
			invNorms[j] = float32(1 / math.Sqrt(float64(-negNormSq)))
		}
	}

	var score float32
	for _, queryToken := range queryVectors {
		d := dotProvider.New(queryToken)
		minDist := float32(math.MaxFloat32)
		for j, docToken := range docVectors {
			negDot, err := d.Distance(docToken)
			if err != nil {
				return 0, err
			}
			dist := 1 + negDot*invNorms[j]
			if dist < 0 {
				// mirror the cosine-dot provider, which clamps negative
				// rounding artifacts to zero
				dist = 0
			}
			if dist < minDist {
				minDist = dist
			}
		}
		score += minDist
	}
	return score, nil
}
