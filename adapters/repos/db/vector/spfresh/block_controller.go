package spfresh

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	// hardcoded block size for postings.
	// Once we switch to SSD blocks, this can be changed to a more dynamic value.
	blockSize = 4096
)

var (
	// ErrPostingNotFound is returned when a posting with the given ID does not exist.
	ErrPostingNotFound = errors.New("posting not found")
)

// BlockController manages I/O of postings on disk.
type BlockController struct {
	store   Store
	encoder BlockEncoder
	// mapping of posting IDs to their blocks offsets.
	// FlatBuffer is used here because the initial store implementation
	// will use monotonic counters for block offsets.
	// Once we switch to SSD blocks (e.g. SPDK), we can switch to a different
	// implementation that supports sparse mappings.
	mapping   *common.FlatBuffer[BlockMapping]
	blockPool *BlockProvider
	bufPool   sync.Pool
}

// Get reads the entire data of the given posting.
func (b *BlockController) Get(ctx context.Context, postingID uint64) (Posting, error) {
	mapping := b.mapping.Get(postingID)
	if mapping.PostingID == 0 {
		return nil, ErrPostingNotFound
	}

	buf := b.getBuffer()
	defer b.putBuffer(buf)

	for _, blockID := range mapping.BlockOffsets {
		block, err := b.store.Get(ctx, blockID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get block %d for posting %d", blockID, postingID)
		}

		buf = append(buf, block...)
	}

	return b.encoder.Decode(buf)
}

func (b *BlockController) getBuffer() []byte {
	return (*b.bufPool.Get().(*[]byte))
}

func (b *BlockController) putBuffer(buf []byte) {
	buf = buf[:0]
	b.bufPool.Put(&buf)
}

// Put writes the full content of a posting, overwriting any existing data.
func (b *BlockController) Put(ctx context.Context, postingID uint64, posting Posting) error {
	if len(posting) == 0 {
		return errors.New("posting cannot be empty")
	}

	// encode the posting into binary data
	data, err := b.encoder.Encode(posting)
	if err != nil {
		return errors.Wrapf(err, "failed to encode posting %d", postingID)
	}

	// store the posting in individual blocks
	var offsets []uint64
	for len(data) > 0 {
		offset := b.blockPool.getFreeBlockOffset()
		err = b.store.Put(ctx, offset, data[:blockSize])
		if err != nil {
			return errors.Wrapf(err, "failed to put block %d for posting %d", offset, postingID)
		}

		data = data[blockSize:]
		offsets = append(offsets, offset)
	}

	// copy the previous mapping if it exists
	old := b.mapping.Get(postingID)

	// update the mapping with the new offsets
	b.mapping.Set(postingID, BlockMapping{
		PostingID:    postingID,
		BlockOffsets: offsets,
	})

	// add the old offsets back to the free block pool
	for _, offset := range old.BlockOffsets {
		b.blockPool.freeBlockPool.Put(offset)
	}

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
	PostingID    uint64
	BlockOffsets []uint64
}

// Store defines the interface for a storage backend that can retrieve blocks by their ID.
// It is used by the BlockController to read blocks from disk.
type Store interface {
	Get(ctx context.Context, offset uint64) ([]byte, error)
	Put(ctx context.Context, offset uint64, block []byte) error
}

// BlockEncoder encodes and decodes vectors to and from blocks.
type BlockEncoder interface {
	Encode(vectors []Vector) ([]byte, error)
	Decode(bloc []byte) ([]Vector, error)
}

type BlockProvider struct {
	// freeBlockPool holds free block offsets/IDs for reuse.
	freeBlockPool *common.Pool[uint64]
	// A simple monotonic counter to generate unique block offsets,
	// as we are storing blocks in an LSM store.
	offsetGenerator *common.MonotonicCounter[uint64]
}

func (b *BlockProvider) getFreeBlockOffset() uint64 {
	// get a free block offset from the pool
	offset, ok := b.freeBlockPool.Get()
	if ok {
		return offset
	}

	// if the pool is empty, generate a new offset
	return b.offsetGenerator.Next()
}
