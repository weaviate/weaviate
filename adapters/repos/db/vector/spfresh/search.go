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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

func (s *SPFresh) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	fmt.Println("NATEE SPFResh.SearchByVector", vector, k, allowList)
	vector = distancer.Normalize(vector)

	queryVector := s.Quantizer.Encode(vector)

	// If k is larger than the configured number of candidates, use k as the candidate number
	// to enlarge the search space.
	candidateNum := max(k, s.UserConfig.InternalPostingCandidates)

	centroids, err := s.SPTAG.Search(queryVector, candidateNum)
	if err != nil {
		return nil, nil, err
	}

	if len(centroids) == 0 {
		return nil, nil, nil
	}

	// compute the max distance to filter out candidates that are too far away
	maxDist := centroids[0].Distance * s.UserConfig.MaxDistanceRatio

	// filter out candidates that are too far away or have no posting size
	selected := make([]uint64, 0, s.UserConfig.InternalPostingCandidates)
	for i := 0; i < len(centroids) && len(selected) < s.UserConfig.InternalPostingCandidates; i++ {
		if (maxDist > 0.1 && centroids[i].Distance > maxDist) || s.PostingSizes.Get(centroids[i].ID) == 0 {
			continue
		}

		selected = append(selected, centroids[i].ID)
	}

	// read all the selected postings
	postings, err := s.Store.MultiGet(ctx, selected)
	if err != nil {
		return nil, nil, err
	}

	visited := s.visitedPool.Borrow()
	defer s.visitedPool.Return(visited)

	q := priorityqueue.NewMin[uint64](k)
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
			// if visited.Visited(id) {
			// 	continue
			// }

			// skip vectors that are not in the allow list.
			// if the allow list is nil, allow all vectors.
			if allowList != nil && !allowList.Contains(id) {
				continue
			}

			// visited.Visit(id)

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
	results := make([]uint64, 0, q.Len())
	dists := make([]float32, 0, q.Len())
	for q.Len() > 0 {
		item := q.Pop()
		fmt.Println("NATEE SPFResh.SearchByVector q.Pop", q.Len(), item.ID, item.Dist)
		results = append(results, item.ID)
		dists = append(dists, item.Dist)
	}
	fmt.Println("NATEE SPFResh.SearchByVector results", results, dists)
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
