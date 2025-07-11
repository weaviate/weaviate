package spfresh

import (
	"context"
)

// BlockController is the interface for managing I/O of postings on disk.
type BlockController interface {
	// Get reads the entire data of the given posting.
	// The returned data must not be modified by the caller.
	Get(ctx context.Context, postingID uint64) (Posting, error)

	// ParallelGet reads multiple postings concurrently.
	// The returned data slices are in the same order as the ids.
	// If any id is not found, the function returns an error.
	ParallelGet(ctx context.Context, postingIDs []uint64) ([]Posting, error)

	// Put writes the full content of a posting, overwriting any existing data.
	Put(ctx context.Context, postingID uint64, posting Posting) error

	// Append appends new data to an existing posting.
	Append(ctx context.Context, postingID uint64, vector *Vector) error

	// VectorCount returns the number of vectors in a posting.
	VectorCount(postingID uint64) (int, error)
}
