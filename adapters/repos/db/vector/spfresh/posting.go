package spfresh

type Vector struct {
	ID      uint64
	Version VectorVersion
	Data    []float32
}

// A Posting is a collection of vectors associated with the same centroid.
type Posting []Vector

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return a new Posting.
func (p Posting) GarbageCollect(versionMap *VersionMap) Posting {
	var filtered Posting

	for _, v := range p {
		version := versionMap.Get(v.ID)
		if !version.Deleted() {
			filtered = append(filtered, Vector{
				ID:      v.ID,
				Version: version,
				Data:    v.Data,
			})
		}
	}

	return filtered
}

// Dimensions returns the number of dimensions of the vectors in the posting.
// It returns 0 if the posting is empty.
func (p Posting) Dimensions() int {
	if len(p) == 0 {
		return 0
	}

	return len(p[0].Data)
}

// A PostingSplitter splits a posting into two evenly distributed groups.
type PostingSplitter interface {
	// Split takes a posting and returns two centroids and a list of groups.
	// Each group contains the indices of vectors that belong to the corresponding centroid.
	Split(vectors Posting) (*SplitResult, error)
}

type SplitResult struct {
	LeftCentroid  []float32
	LeftPosting   Posting
	RightCentroid []float32
	RightPosting  Posting
}
