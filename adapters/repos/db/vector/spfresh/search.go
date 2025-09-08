//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"context"
	"iter"
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

func (s *SPFresh) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	vector = distancer.Normalize(vector)

	queryVector := s.Quantizer.Encode(vector)

	var selected []uint64
	var postings []*Posting

	// If k is larger than the configured number of candidates, use k as the candidate number
	// to enlarge the search space.
	candidateNum := max(k, s.UserConfig.InternalPostingCandidates)

	centroids, err := s.SPTAG.Search(queryVector, candidateNum)
	if err != nil {
		return nil, nil, err
	}

	q := NewKSmallest(k)

	if s.UserConfig.PruningStrategy == SizeBasedPruningStrategy {
		// compute the max distance to filter out candidates that are too far away
		maxDist := centroids[0].Distance * s.UserConfig.MaxDistanceRatio

		// filter out candidates that are too far away or have no posting size
		selected = make([]uint64, 0, s.UserConfig.InternalPostingCandidates)
		for i := 0; i < len(centroids) && len(selected) < s.UserConfig.InternalPostingCandidates; i++ {
			if (maxDist > 0.1 && centroids[i].Distance > maxDist) || s.PostingSizes.Get(centroids[i].ID) == 0 {
				continue
			}

			selected = append(selected, centroids[i].ID)
		}

		// read all the selected postings
		postings, err = s.Store.MultiGet(ctx, selected)
		if err != nil {
			return nil, nil, err
		}

		visited := s.visitedPool.Borrow()
		defer s.visitedPool.Return(visited)

		for i, p := range postings {
			if p == nil { // posting nil if not found
				continue
			}

			// keep track of the posting size
			postingSize := p.Len()

			for _, v := range p.Iter() {
				id := v.ID()
				// skip deleted vectors
				if s.VersionMap.IsDeleted(id) {
					postingSize--
					continue
				}

				// skip duplicates
				if visited.Visited(id) {
					continue
				}

				// skip vectors that are not in the allow list.
				// if the allow list is nil, allow all vectors.
				if allowList != nil && !allowList.Contains(id) {
					continue
				}

				visited.Visit(id)

				dist, err := s.Quantizer.DistanceBetweenCompressedVectors(v, queryVector)
				if err != nil {
					return nil, nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
				}

				q.Insert(id, dist)
			}

			// if the posting size is lower than the configured minimum,
			// enqueue a merge operation
			if postingSize < int(s.UserConfig.MinPostingSize) {
				err = s.enqueueMerge(ctx, selected[i])
				if err != nil {
					return nil, nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", selected[i])
				}
			}
		}
	} else {
		sort.Slice(centroids, func(i, j int) bool {
			return centroids[i].Distance-s.SPTAG.Get(centroids[i].ID).Radius < centroids[j].Distance-s.SPTAG.Get(centroids[j].ID).Radius
		})

		q := NewKSmallest(k)

		for _, centroid := range centroids {
			if centroid.Distance-s.SPTAG.Get(centroid.ID).Radius > q.Max() {
				// if the distance is larger than the max distance in the k smallest,
				// we can stop searching
				break
			}

			p, err := s.Store.Get(ctx, centroid.ID)
			if err != nil {
				return nil, nil, err
			}

			// keep track of the posting size
			postingSize := p.Len()

			for _, v := range p.Iter() {
				id := v.ID()
				// skip deleted vectors
				if s.VersionMap.IsDeleted(id) {
					postingSize--
					continue
				}

				// skip vectors that are not in the allow list
				if !allowList.Contains(id) {
					continue
				}

				dist, err := s.Quantizer.DistanceBetweenCompressedVectors(v, queryVector)
				if err != nil {
					return nil, nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
				}

				q.Insert(id, dist)
			}

			// if the posting size is lower than the configured minimum,
			// enqueue a merge operation
			if postingSize < int(s.UserConfig.MinPostingSize) {
				err = s.enqueueMerge(ctx, centroid.ID)
				if err != nil {
					return nil, nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", centroid.ID)
				}
			}
		}
	}

	results := make([]uint64, k)
	dists := make([]float32, k)
	i := 0
	for id, dist := range q.Iter() {
		results[i] = id
		dists[i] = dist
		i++
	}
	return results, dists, nil
}

func (s *SPFresh) SearchByVectorDistance(
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
		ids, dist, err := s.SearchByVector(ctx, vector, totalLimit, allow)
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
			s.Logger.
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

type KSmallestItem struct {
	ID   uint64
	Dist float32
}

// KSmallest maintains the k smallest elements by distance in a sorted array
type KSmallest struct {
	data []KSmallestItem
	k    int
}

func NewKSmallest(k int) *KSmallest {
	return &KSmallest{
		data: make([]KSmallestItem, 0, k),
		k:    k,
	}
}

// Insert adds a new element, maintaining only k smallest elements by distance
func (ks *KSmallest) Insert(id uint64, dist float32) {
	item := KSmallestItem{ID: id, Dist: dist}

	// If array isn't full yet, just insert in sorted position
	if len(ks.data) < ks.k {
		pos := ks.searchByDistance(dist)
		ks.data = append(ks.data, KSmallestItem{})
		copy(ks.data[pos+1:], ks.data[pos:])
		ks.data[pos] = item
		return
	}

	// If array is full, only insert if distance is smaller than max (last element)
	if dist < ks.data[ks.k-1].Dist {
		pos := ks.searchByDistance(dist)
		// Shift elements to the right and insert
		copy(ks.data[pos+1:], ks.data[pos:ks.k-1])
		ks.data[pos] = item
	}
}

// searchByDistance finds the insertion position for a given distance
func (ks *KSmallest) searchByDistance(dist float32) int {
	left, right := 0, len(ks.data)
	for left < right {
		mid := (left + right) / 2
		if ks.data[mid].Dist < dist {
			left = mid + 1
		} else {
			right = mid
		}
	}
	return left
}

// Max returns the maximum distance element among the k smallest (last element)
func (ks *KSmallest) Max() float32 {
	if len(ks.data) == 0 {
		return math.MaxFloat32
	}
	item := ks.data[len(ks.data)-1]
	return item.Dist
}

// Size returns current number of elements
func (ks *KSmallest) Size() int {
	return len(ks.data)
}

// IsFull returns true if we have k elements
func (ks *KSmallest) IsFull() bool {
	return len(ks.data) == ks.k
}

// GetAll returns a copy of all elements (sorted by distance)
func (ks *KSmallest) GetAll() []KSmallestItem {
	result := make([]KSmallestItem, len(ks.data))
	copy(result, ks.data)
	return result
}

// GetIDs returns all IDs in distance order
func (ks *KSmallest) GetIDs() []uint64 {
	ids := make([]uint64, len(ks.data))
	for i, item := range ks.data {
		ids[i] = item.ID
	}
	return ids
}

// GetDistances returns all distances in order
func (ks *KSmallest) GetDistances() []float32 {
	dists := make([]float32, len(ks.data))
	for i, item := range ks.data {
		dists[i] = item.Dist
	}
	return dists
}

// Contains checks if an ID exists in the k smallest elements
func (ks *KSmallest) Contains(id uint64) bool {
	for _, item := range ks.data {
		if item.ID == id {
			return true
		}
	}
	return false
}

func (ks *KSmallest) Iter() iter.Seq2[uint64, float32] {
	return func(yield func(uint64, float32) bool) {
		for _, item := range ks.data {
			if !yield(item.ID, item.Dist) {
				break
			}
		}
	}
}

// RemoveMin removes and returns the minimum distance element
func (ks *KSmallest) RemoveMin() (uint64, float32, bool) {
	if len(ks.data) == 0 {
		return 0, 0, false
	}
	min := ks.data[0]
	ks.data = ks.data[1:]
	return min.ID, min.Dist, true
}
