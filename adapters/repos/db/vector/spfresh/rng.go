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

import "github.com/pkg/errors"

// RNGSelect performs a Relative Neighborhood Graph selection to determine
// the postings where the vector should be assigned to.
// It performs a search to find candidate postings, then iteratively selects
// postings that are not too close to already selected ones based on the RNGFactor.
// If `reassignedFromID` is non-zero, the function will abort and return false
// if one of the selected postings is equal to `reassignedFromID`.
func (s *SPFresh) RNGSelect(query Vector, reassignedFromID uint64) ([]SearchResult, bool, error) {
	replicas := make([]SearchResult, 0, s.config.Replicas)
	candidates, err := s.SPTAG.Search(query, s.config.InternalPostingCandidates)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to search for nearest neighbors")
	}

	for _, c := range candidates {
		cCenter := s.SPTAG.Get(c.ID)

		tooClose := false
		for _, r := range replicas {
			rCenter := s.SPTAG.Get(r.ID)
			centerDist, err := cCenter.Vector.Distance(s.distancer, rCenter.Vector)
			if err != nil {
				return nil, false, errors.Wrapf(err, "failed to compute distance for edge %d -> %d", c.ID, r.ID)
			}

			if centerDist <= (1.0/s.config.RNGFactor)*c.Distance {
				tooClose = true
				break
			}
		}
		if tooClose {
			continue
		}

		// abort if candidate already assigned to `reassignedFrom`
		if reassignedFromID != 0 && c.ID == reassignedFromID {
			return nil, false, nil
		}

		replicas = append(replicas, c)
	}

	return replicas, true, nil
}
