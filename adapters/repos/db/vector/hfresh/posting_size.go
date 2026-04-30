package hfresh

import (
	"bytes"
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
	count atomic.Uint64
}

func NewPostingSizes(bucket *lsmkv.Bucket) *PostingSizes {
	return &PostingSizes{
		data:  common.NewGroupedPagedArray[uint32](16*1024, 64*1024), // 1 billion entries with 64k per page
		store: NewPostingSizesStore(bucket, postingSizesBucketPrefix),
	}
}

// Increment increases the size of the posting by 1 and returns the new size.
// It also persists the new size in the underlying store.
func (p *PostingSizes) Increment(ctx context.Context, postingID uint64) (uint32, error) {
	page, slot := p.data.EnsurePageFor(postingID)
	newSize := atomic.AddUint32(&page[slot], 1)

	if newSize == 1 {
		// if the new size is 1, it means we just created a new entry.
		p.count.Add(1)
	}

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

	oldSize := atomic.LoadUint32(&page[slot])
	if oldSize == 0 && size > 0 {
		p.count.Add(1)
	} else if oldSize > 0 && size == 0 {
		p.count.Add(^uint64(0)) // decrement by 1
	}

	atomic.StoreUint32(&page[slot], size)

	return p.store.Set(ctx, postingID, size)
}

// Restore loads all posting sizes from the underlying store into memory. This should be called during startup to populate the in-memory cache.
func (p *PostingSizes) Restore(ctx context.Context) error {
	return p.store.Iter(ctx, func(postingID uint64, size uint32) error {
		page, slot := p.data.EnsurePageFor(postingID)
		atomic.StoreUint32(&page[slot], size)
		if size > 0 {
			p.count.Add(1)
		}
		return nil
	})
}

// Count returns the total number of postings.
func (p *PostingSizes) Count() uint64 {
	return p.count.Load()
}

// PostingSizesStore is a persistent store for posting sizes.
// It stores the sizes in a shared LSMKV bucket.
type PostingSizesStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix []byte
}

func NewPostingSizesStore(bucket *lsmkv.Bucket, keyPrefix []byte) *PostingSizesStore {
	return &PostingSizesStore{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingSizesStore) key(postingID uint64) []byte {
	buf := make([]byte, len(p.keyPrefix)+8)
	copy(buf, p.keyPrefix)
	binary.LittleEndian.PutUint64(buf[len(p.keyPrefix):], postingID)
	return buf
}

func (p *PostingSizesStore) Get(ctx context.Context, postingID uint64) (uint32, error) {
	key := p.key(postingID)
	size, err := p.bucket.Get(key[:])
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get size for %d", postingID)
	}

	if len(size) == 0 {
		return 0, ErrPostingNotFound
	}

	return binary.LittleEndian.Uint32(size), nil
}

func (p *PostingSizesStore) Set(ctx context.Context, postingID uint64, size uint32) error {
	key := p.key(postingID)
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, size)
	return p.bucket.Put(key[:], buf)
}

func (p *PostingSizesStore) Iter(ctx context.Context, fn func(uint64, uint32) error) error {
	c := p.bucket.Cursor()
	defer c.Close()

	var i int
	for k, v := c.Seek(p.keyPrefix); len(k) > 0 && bytes.HasPrefix(k, p.keyPrefix); k, v = c.Next() {
		i++
		if len(v) == 0 {
			continue
		}

		if i%1000 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}

		postingID := binary.LittleEndian.Uint64(k[len(p.keyPrefix):])
		size := binary.LittleEndian.Uint32(v)
		err := fn(postingID, size)
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}
