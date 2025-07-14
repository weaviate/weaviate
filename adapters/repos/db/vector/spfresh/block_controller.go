package spfresh

import (
	"context"
	"encoding/binary"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	// TODO: move this to the helpers package
	blockControllerBucketName = "block_controller"
)

// BlockController manages I/O of postings on disk.
type BlockController struct {
	mapping *common.FlatBuffer[BlockMapping]
	store   Store
	// TODO: add logical locks
}

// Get reads the entire data of the given posting.
func (b *BlockController) Get(ctx context.Context, postingID uint64) (Posting, error) {
	mapping := b.mapping.Get(postingID)
	if mapping.PostingID == 0 {
		return nil, nil
	}

	for _, blockID := range mapping.Blocks {
		_, err := b.store.Get(ctx, blockID)
		if err != nil {
			return nil, err
		}

		// TODO: decode and append to the posting
	}

	return Posting{}, nil
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

type BlockMapping struct {
	PostingID uint64
	Blocks    []uint64
}

type Store interface {
	Get(ctx context.Context, key uint64) ([]byte, error)
}

type LSMStore struct {
	store *lsmkv.Store
}

func (l *LSMStore) Get(ctx context.Context, key uint64) ([]byte, error) {
	var encKey [8]byte
	binary.BigEndian.PutUint64(encKey[:], key)

	return l.store.Bucket(blockControllerBucketName).Get(encKey[:])
}
