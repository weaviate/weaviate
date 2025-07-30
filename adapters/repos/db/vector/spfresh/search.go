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
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
)

func (s *SPFresh) Search(ctx context.Context, queryVector []byte, k int) ([]uint64, error) {
	// If k is larger than the configured number of candidates, use k as the candidate number
	// to enlarge the search space.
	candidateNum := max(k, s.UserConfig.InternalPostingCandidates)

	centroids, err := s.SPTAG.Search(queryVector, candidateNum)
	if err != nil {
		return nil, err
	}

	if len(centroids) == 0 {
		return nil, nil
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
		return nil, err
	}

	dedup := newDeduplicator() // TODO: use a pool?

	q := priorityqueue.NewMin[uint64](k)
	for i, p := range postings {
		if p == nil { // posting nil if not found
			continue
		}

		// keep track of the posting size
		postingSize := len(p)

		for _, v := range p {
			// skip deleted vectors
			if s.VersionMap.IsDeleted(v.ID) {
				postingSize--
				continue
			}

			// skip duplicates
			if !dedup.tryAdd(v.ID) {
				continue
			}

			dist, err := s.SPTAG.ComputeDistance(v.Data, queryVector)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to compute distance for vector %d", v.ID)
			}

			q.Insert(v.ID, dist)
		}

		// if the posting size is lower than the configured minimum,
		// enqueue a merge operation
		if postingSize < int(s.UserConfig.MinPostingSize) {
			err = s.enqueueMerge(ctx, selected[i])
			if err != nil {
				return nil, errors.Wrapf(err, "failed to enqueue merge for posting %d", selected[i])
			}
		}
	}

	results := make([]uint64, 0, q.Len())
	for q.Len() > 0 {
		item := q.Pop()
		results = append(results, item.ID)
	}

	return results, nil
}
