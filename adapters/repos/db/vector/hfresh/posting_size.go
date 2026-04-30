package hfresh

import (
	"context"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingSizesStore is a persistent store for vector versions.
// It stores the versions in an LSMKV bucket.
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
