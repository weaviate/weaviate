package spfresh

import "github.com/pkg/errors"

// RNGSelect performs a Relative Neighborhood Graph selection to determine
// the postings where the vector should be assigned to.
// It performs a search to find candidate postings, then iteratively selects
// postings that are not too close to already selected ones based on the RNGFactor.
// If `unless` is non-zero, the function will abort and return false
// if one of the selected postings is equal to `unless`.
func (s *SPFresh) RNGSelect(query Vector, unless uint64) ([]SearchResult, bool, error) {
	results, err := s.SPTAG.Search(query, s.Config.InternalPostingCandidates)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to search for nearest neighbors")
	}

	replicas := make([]SearchResult, 0, s.Config.Replicas)

	for i := 0; i < len(results) && len(replicas) < s.Config.Replicas; i++ {
		candidate := results[i]

		qDist := candidate.Distance
		candidateCentroid := s.SPTAG.Get(candidate.ID)

		tooClose := false
		for j := range replicas {
			other := s.SPTAG.Get(replicas[j].ID)
			pairDist, err := candidateCentroid.Vector.Distance(s.distancer, other.Vector)
			if err != nil {
				return nil, false, errors.Wrapf(err, "failed to compute distance for edge %d -> %d", candidate.ID, replicas[j].ID)
			}

			if s.Config.RNGFactor*pairDist <= qDist {
				tooClose = true
				break
			}
		}
		if tooClose {
			continue
		}

		// abort if candidate already assigned to `unless`
		if unless != 0 && candidate.ID == unless {
			return nil, false, nil
		}

		replicas = append(replicas, candidate)
	}

	return replicas, true, nil
}
