package hfresh

import (
	"context"
	"encoding/binary"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

type PostingSizes struct {
	data  *common.GroupedPagedArray[uint32]
	store *PostingSizesStore
}

func NewPostingSizes(bucket *lsmkv.Bucket) *PostingSizes {
	return &PostingSizes{
		data:  common.NewGroupedPagedArray[uint32](16*1024, 64*1024), // 1 billion entries with 64k per page
		store: NewPostingSizesStore(bucket),
	}
}

// Increment increases the size of the posting by 1 and returns the new size.
// It also persists the new size in the underlying store.
func (p *PostingSizes) Increment(ctx context.Context, postingID uint64) (uint32, error) {
	page, slot := p.data.EnsurePageFor(postingID)
	newSize := atomic.AddUint32(&page[slot], 1)

	err := p.store.Set(ctx, postingID, newSize)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to set size for posting %d", postingID)
	}

	return newSize, nil
}

// Get returns the size of the posting with the given ID. If the posting is not found, it returns ErrPostingNotFound.
func (p *PostingSizes) Get(ctx context.Context, postingID uint64) (uint32, error) {
	page, slot := p.data.GetPageFor(postingID)
	if page == nil {
		return 0, ErrPostingNotFound
	}

	size := atomic.LoadUint32(&page[slot])
	if size == 0 {
		return 0, ErrPostingNotFound
	}

	return size, nil
}

// Set sets the size of the posting with the given ID. It also persists the new size in the underlying store.
func (p *PostingSizes) Set(ctx context.Context, postingID uint64, size uint32) error {
	page, slot := p.data.EnsurePageFor(postingID)
	atomic.StoreUint32(&page[slot], size)

	return p.store.Set(ctx, postingID, size)
}

// PostingSizesStore is a persistent store for posting sizes.
// It stores the sizes in a shared LSMKV bucket.
type PostingSizesStore struct {
	bucket *lsmkv.Bucket
}

func NewPostingSizesStore(bucket *lsmkv.Bucket) *PostingSizesStore {
	return &PostingSizesStore{
		bucket: bucket,
	}
}

func (v *PostingSizesStore) key(postingID uint64) []byte {
	buf := make([]byte, len(postingSizesBucketPrefix)+8)
	copy(buf, postingSizesBucketPrefix)
	binary.LittleEndian.PutUint64(buf[len(postingSizesBucketPrefix):], postingID)
	return buf
}

func (v *PostingSizesStore) Get(ctx context.Context, postingID uint64) (uint32, error) {
	key := v.key(postingID)
	size, err := v.bucket.Get(key[:])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get size for %d", postingID)
	}

	if len(size) == 0 {
		return 0, ErrPostingNotFound
	}

	return binary.LittleEndian.Uint32(size), nil
}

func (v *PostingSizesStore) Set(ctx context.Context, postingID uint64, size uint32) error {
	key := v.key(postingID)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, size)
	return v.bucket.Put(key[:], buf)
}
