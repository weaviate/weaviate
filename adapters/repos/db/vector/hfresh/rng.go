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
	"github.com/pkg/errors"
)

// RNGSelect performs a Relative Neighborhood Graph selection to determine
// the postings where the vector should be assigned to.
// It performs a search to find candidate postings, then iteratively selects
// postings that are not too close to already selected ones based on the RNGFactor.
// If `reassignedFromID` is non-zero, the function will abort and return false
// if one of the selected postings is equal to `reassignedFromID`.
func (h *HFresh) RNGSelect(query []float32, reassignedFromID uint64) (*ResultSet, bool, error) {
	replicas := NewResultSet(int(h.replicas))
	candidates, err := h.Centroids.Search(query, h.config.InternalPostingCandidates)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to search for nearest neighbors")
	}

	for cID, cDistance := range candidates.Iter() {
		cCenter := h.Centroids.Get(cID)

		tooClose := false
		for _, r := range replicas.data {
			rCenter := h.Centroids.Get(r.ID)
			centerDist, err := h.distancer.DistanceBetweenVectors(cCenter.Uncompressed, rCenter.Uncompressed)
			if err != nil {
				return nil, false, errors.Wrapf(err, "failed to compute distance for edge %d -> %d", cID, r.ID)
			}

			if centerDist <= (1.0/h.rngFactor)*cDistance {
				tooClose = true
				break
			}
		}
		if tooClose {
			continue
		}

		// abort if candidate already assigned to `reassignedFrom`
		if reassignedFromID != 0 && cID == reassignedFromID {
			return nil, false, nil
		}

		replicas.data = append(replicas.data, Result{ID: cID, Distance: cDistance})
		if replicas.Len() >= int(h.replicas) {
			break
		}
	}

	return replicas, true, nil
}
