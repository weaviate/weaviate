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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

const (
	// minimum max distance to use when pruning
	pruningMinMaxDistance = 0.1
)

func (s *SPFresh) SearchByVector(ctx context.Context, vector []float32, k int, allowList helpers.AllowList) ([]uint64, []float32, error) {
	queryVector := NewAnonymousCompressedVector(s.SPTAG.Quantizer().Encode(vector))

	var selected []uint64
	var postings []Posting

	// If k is larger than the configured number of candidates, use k as the candidate number
	// to enlarge the search space.
	candidateNum := max(k, s.config.SearchProbe)

	centroids, err := s.SPTAG.Search(vector, candidateNum)
	if err != nil {
		return nil, nil, err
	}

	q := NewResultSet(k)

	// compute the max distance to filter out candidates that are too far away
	maxDist := centroids.data[0].Distance * s.config.MaxDistanceRatio

	// filter out candidates that are too far away or have no vectors
	selected = make([]uint64, 0, candidateNum)
	for i := 0; i < len(centroids.data) && len(selected) < candidateNum; i++ {
		if (maxDist > pruningMinMaxDistance && centroids.data[i].Distance > maxDist) || s.PostingSizes.Get(centroids.data[i].ID) == 0 {
			continue
		}

		selected = append(selected, centroids.data[i].ID)
	}

	// read all the selected postings
	postings, err = s.Store.MultiGet(ctx, selected)
	if err != nil {
		return nil, nil, err
	}

	visited := s.visitedPool.Borrow()
	defer s.visitedPool.Return(visited)

	totalVectors := 0
	for i, p := range postings {
		if p == nil { // posting nil if not found
			continue
		}

		// keep track of the posting size
		postingSize := p.Len()
		totalVectors += postingSize

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

			// skip vectors that are not in the allow list
			if allowList != nil && !allowList.Contains(id) {
				continue
			}

			dist, err := v.Distance(s.distancer, queryVector)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to compute distance for vector %d", id)
			}

			q.Insert(id, dist)
		}

		// if the posting size is lower than the configured minimum,
		// enqueue a merge operation
		if postingSize < int(s.config.MinPostingSize) {
			err = s.enqueueMerge(ctx, selected[i])
			if err != nil {
				return nil, nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", selected[i])
			}
		}
	}

	ids := make([]uint64, q.Len())
	dists := make([]float32, q.Len())
	i := 0
	for id, dist := range q.Iter() {
		ids[i] = id
		dists[i] = dist
		i++
	}

	return ids, dists, nil
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
			s.logger.
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
