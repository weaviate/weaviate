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
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	// hardcoded block size for postings.
	// Once we switch to SPDK, this can be changed to a more dynamic value.
	blockSize = 4096

	// bucketName is the name of the bucket where postings are stored in the LSM store.
	bucketName = "spfresh_blocks"
)

// ErrPostingNotFound is returned when a posting with the given ID does not exist.
var ErrPostingNotFound = errors.New("posting not found")

// BlockController manages I/O of postings on disk.
type BlockController struct {
	store       Store
	encoder     *PostingEncoder
	metadata    *common.FlatBuffer[PostingMetadata]
	blockPool   *BlockProvider
	bufPool     sync.Pool // holds byte slices for reading blocks
	mappingPool sync.Pool // holds slices of uint64 for offsets
}

type BlockControllerConfig struct {
	VectorDimensions int    // Number of dimensions of the vectors to encode/decode
	StartOffset      uint64 // Starting offset for the first block
}

func NewBlockController(store Store, freeBlockPool *common.Pool[uint64], config *BlockControllerConfig) *BlockController {
	return &BlockController{
		store:     store,
		encoder:   NewBlockEncoder(config.VectorDimensions),
		metadata:  common.NewFlatBuffer[PostingMetadata](1024),
		blockPool: NewBlockProvider(config.StartOffset, freeBlockPool),
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, 3*blockSize)
				return &b
			},
		},
		mappingPool: sync.Pool{
			New: func() any {
				b := make([]uint64, 0, 3)
				return &b
			},
		},
	}
}

// Get reads the entire data of the given posting.
func (b *BlockController) Get(ctx context.Context, postingID uint64) (Posting, error) {
	mapping := b.metadata.Get(postingID)
	if mapping.Offsets == nil {
		return nil, ErrPostingNotFound
	}

	buf := b.getBuffer()
	defer b.putBuffer(buf)

	offsets := *mapping.Offsets.Load()
	for _, blockID := range offsets {
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
	data, release, err := b.encoder.Encode(posting)
	if err != nil {
		return errors.Wrapf(err, "failed to encode posting %d", postingID)
	}
	defer release()

	// store the posting in individual blocks
	offsets := make([]uint64, 0, len(data)/blockSize+1)
	for len(data) > 0 {
		offset := b.blockPool.getFreeBlockOffset()
		if len(data) < blockSize {
			err = b.store.Put(ctx, offset, data)
			if err != nil {
				return errors.Wrapf(err, "failed to put block %d for posting %d", offset, postingID)
			}
			data = nil // all data has been written
		} else {
			err = b.store.Put(ctx, offset, data[:blockSize])
			if err != nil {
				return errors.Wrapf(err, "failed to put block %d for posting %d", offset, postingID)
			}
			data = data[blockSize:]
		}

		offsets = append(offsets, offset)
	}

	// copy the previous metadata if it exists
	old := b.metadata.Get(postingID)

	// update the metadata with the new offsets
	b.metadata.Set(postingID, NewPostingMetadata(offsets, uint64(len(posting))))

	// add the old offsets back to the free block pool
	if old.Offsets != nil {
		oldOffsets := *old.Offsets.Load()
		for _, offset := range oldOffsets {
			b.blockPool.freeBlockPool.Put(offset)
		}
	}

	return nil
}

// Append appends new data to an existing posting.
func (b *BlockController) Append(ctx context.Context, postingID uint64, vector *Vector) (err error) {
	if vector == nil || len(vector.Data) == 0 {
		return errors.New("vector cannot be nil or empty")
	}

	// get posting metadata
	metadata := b.metadata.Get(postingID)
	if metadata.Offsets == nil {
		return ErrPostingNotFound
	}

	// encode the vector
	data, release, err := b.encoder.EncodeVector(vector)
	if err != nil {
		return errors.Wrapf(err, "failed to encode vector for posting %d", postingID)
	}
	defer release()

	// allocate a new block
	newBlockOffset := b.blockPool.getFreeBlockOffset()

	defer func() {
		// add the new block back to the free block pool if the operation fails
		if err != nil {
			b.blockPool.freeBlockPool.Put(newBlockOffset)
		}
	}()

	for {
		var newOffsets []uint64

		// load the current offsets and keep the pointer
		// for the CAS operation
		oldOffsets := metadata.Offsets.Load()
		o := *oldOffsets
		lastOffset := o[len(o)-1]

		// get the last block
		lastBlock, err := b.store.Get(ctx, lastOffset)
		if err != nil {
			return errors.Wrapf(err, "failed to get last block %d for posting %d", lastOffset, postingID)
		}

		// check if the last block has enough space to append the new data
		if len(lastBlock)+len(data) < blockSize {
			// append to the last block
			lastBlock = append(lastBlock, data...)
			// allocate a new offsets slice with the same length
			newOffsets = b.getOffsetBuffer(len(o))
		} else {
			lastBlock = data
			// allocate a new offsets slice with one more element
			newOffsets = b.getOffsetBuffer(len(o) + 1)
		}

		// copy the old offsets
		copy(newOffsets, o)
		// set the new block offset at the end
		newOffsets[len(newOffsets)-1] = newBlockOffset

		// store the new block
		err = b.store.Put(ctx, newBlockOffset, lastBlock)
		if err != nil {
			return errors.Wrapf(err, "failed to put new block %d for posting %d", newBlockOffset, postingID)
		}

		if metadata.Offsets.CompareAndSwap(oldOffsets, &newOffsets) {
			// successfully updated the offset mapping,
			// update the vector count
			// note: we introduce some inconsistency here, but it is acceptable
			// because the vector count is only used in the background by the local rebuilder
			metadata.VectorCount.Add(1)

			// add the old offset back to the free block pool
			b.blockPool.freeBlockPool.Put(lastOffset)

			// add the old offset slice back to the mapping pool
			b.mappingPool.Put(oldOffsets)
			return nil
		}
	}
}

func (b *BlockController) getOffsetBuffer(size int) []uint64 {
	buf := b.mappingPool.Get().(*[]uint64)
	if cap(*buf) < size {
		*buf = make([]uint64, size)
	} else {
		*buf = (*buf)[:size]
	}
	return *buf
}

// VectorCount returns the number of vectors in a posting.
func (b *BlockController) VectorCount(postingID uint64) (int, error) {
	metadata := b.metadata.Get(postingID)
	if metadata.VectorCount == nil {
		return 0, ErrPostingNotFound
	}

	return int(metadata.VectorCount.Load()), nil
}

// PostingMetadata contains the mapping of a posting ID to its associated blocks.
type PostingMetadata struct {
	Offsets     *atomic.Pointer[[]uint64]
	VectorCount *atomic.Uint64
}

func NewPostingMetadata(offsets []uint64, vectorCount uint64) PostingMetadata {
	var ptr atomic.Pointer[[]uint64]
	ptr.Store(&offsets)
	var count atomic.Uint64
	count.Store(vectorCount)

	return PostingMetadata{
		Offsets:     &ptr,
		VectorCount: &count,
	}
}

type BlockProvider struct {
	// freeBlockPool holds free block offsets/IDs for reuse.
	freeBlockPool *common.Pool[uint64]
	// A simple monotonic counter to generate unique block offsets,
	// as we are storing blocks in an LSM store.
	offsetGenerator *common.MonotonicCounter[uint64]
}

func NewBlockProvider(startOffset uint64, pool *common.Pool[uint64]) *BlockProvider {
	return &BlockProvider{
		freeBlockPool:   pool,
		offsetGenerator: common.NewUint64Counter(startOffset),
	}
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
