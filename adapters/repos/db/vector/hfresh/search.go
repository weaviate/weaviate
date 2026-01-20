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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

const (
	// minimum max distance to use when pruning
	pruningMinMaxDistance = 0.1
)

func (h *HFresh) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	rescoreLimit := int(h.rescoreLimit)
	vector = h.normalizeVec(vector)
	queryVector := NewAnonymousVector(h.quantizer.CompressedBytes(h.quantizer.Encode(vector)))

	var selectedCentroids []uint64
	var postings []Posting

	// If k is larger than the configured number of candidates, use k as the candidate number
	// to enlarge the search space.
	candidateCentroidNum := max(k, int(h.searchProbe))

	nAllowList := allowList
	if allowList != nil {
		nAllowList = h.wrapAllowList(ctx, allowList)
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
		count, err := h.PostingMap.CountVectorIDs(ctx, centroids.data[i].ID)
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
			if visited.Visited(id) {
				continue
			}

			// skip vectors that are not in the allow list
			if allowList != nil && !allowList.Contains(id) {
				continue
			}

			dist, err := v.Distance(h.distancer, queryVector)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
			}

			visited.Visit(id)
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

	rescored := NewResultSet(k)
	for id := range q.Iter() {
		vec, err := h.vectorForId(ctx, id)
		if err != nil {
			return nil, nil, err
		}
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
