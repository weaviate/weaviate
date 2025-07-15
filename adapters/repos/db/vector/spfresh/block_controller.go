package spfresh

import (
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

var (
	// ErrPostingNotFound is returned when a posting with the given ID does not exist.
	ErrPostingNotFound = errors.New("posting not found")
)

// BlockController manages I/O of postings on disk.
type BlockController struct {
	mapping *common.FlatBuffer[BlockMapping]
	store   Store
	encoder BlockEncoder
}

// Get reads the entire data of the given posting.
func (b *BlockController) Get(ctx context.Context, postingID uint64) (Posting, error) {
	mapping := b.mapping.Get(postingID)
	if mapping.PostingID == 0 {
		return nil, ErrPostingNotFound
	}

	var posting Posting
	for _, blockID := range mapping.Blocks {
		block, err := b.store.Get(ctx, blockID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get block %d for posting %d", blockID, postingID)
		}

		vectors, err := b.encoder.Decode(block)
		if err != nil {
			return nil, err
		}

		posting = append(posting, vectors...)
	}

	return posting, nil
}

// Put writes the full content of a posting, overwriting any existing data.
func (b *BlockController) Put(ctx context.Context, postingID uint64, posting Posting) error {
	return nil
}

// Append appends new data to an existing posting.
func (b *BlockController) Append(ctx context.Context, postingID uint64, vector *Vector) error {
	return nil
}

// VectorCount returns the number of vectors in a posting.
func (b *BlockController) VectorCount(postingID uint64) (int, error) {
	return 0, nil
}

// BlockMapping represents the mapping of a posting ID to its associated blocks.
type BlockMapping struct {
	PostingID uint64
	Blocks    []uint64
}

// Store defines the interface for a storage backend that can retrieve blocks by their ID.
// It is used by the BlockController to read postings from disk.
type Store interface {
	Get(ctx context.Context, key uint64) ([]byte, error)
}

// BlockEncoder encodes and decodes vectors to and from blocks.
type BlockEncoder interface {
	Encode(vectors []Vector) ([]byte, error)
	Decode(bloc []byte) ([]Vector, error)
}
