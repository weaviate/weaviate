package hfresh

// PostingMetadata holds various information about a posting.
type PostingMetadata struct {
	// Size keeps track of the number of vectors in the posting.
	Size uint32
	// Vectors holds the IDs of the vectors in the posting.
	Vectors []uint64
	// Version keeps track of the version of the posting list.
	// Versions are incremented on each Put operation to the posting list,
	// and allow for simpler cleanup of stale data during LSMKV compactions.
	// It uses a combination of an LSMKV store for persistence and an in-memory
	// cache for fast access.
	Version uint32
}
