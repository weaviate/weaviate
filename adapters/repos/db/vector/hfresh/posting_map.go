//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import (
	"context"
	"encoding/binary"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingMap manages various information about postings.
type PostingMap struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, []uint64]
	bucket  *PostingMapStore
}

func NewPostingMap(bucket *lsmkv.Bucket, metrics *Metrics) *PostingMap {
	b := NewPostingMapStore(bucket, postingMapBucketPrefix)

	cache, _ := otter.New[uint64, []uint64](nil)

	return &PostingMap{
		cache:   cache,
		metrics: metrics,
		bucket:  b,
	}
}

// Get returns the vector IDs associated with this posting.
// The returned function must be called to release the posting back to the pool.
func (v *PostingMap) Get(ctx context.Context, postingID uint64) ([]uint64, error) {
	m, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, []uint64](func(ctx context.Context, key uint64) ([]uint64, error) {
		m, err := v.bucket.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return nil, otter.ErrNotFound
			}

			return nil, err
		}

		return m, nil
	}))
	if errors.Is(err, otter.ErrNotFound) {
		return nil, ErrPostingNotFound
	}
	if err != nil {
		return nil, err
	}

	return m, err
}

// CountVectorIDs returns the number of vector IDs in the posting with the given ID.
// If the posting does not exist, it returns 0.
func (v *PostingMap) CountVectorIDs(ctx context.Context, postingID uint64) (uint32, error) {
	m, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}

	if m == nil {
		return 0, nil
	}

	return uint32(len(m)), nil
}

// SetVectorIDs sets the vector IDs for the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) SetVectorIDs(ctx context.Context, postingID uint64, posting Posting) error {
	vectorIDs := make([]uint64, len(posting))
	for i, vector := range posting {
		vectorIDs[i] = vector.ID()
	}

	err := v.bucket.Set(ctx, postingID, vectorIDs)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, vectorIDs)

	v.metrics.ObservePostingSize(float64(len(vectorIDs)))

	return nil
}

// AddVectorID adds a vector ID to the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) AddVectorID(ctx context.Context, postingID uint64, vectorID uint64) (uint32, error) {
	old, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}

	var vectors []uint64

	if old != nil {
		vectors = make([]uint64, len(old))
		copy(vectors, old)
	}
	vectors = append(vectors, vectorID)

	err = v.bucket.Set(ctx, postingID, vectors)
	if err != nil {
		return 0, err
	}

	v.cache.Set(postingID, vectors)

	v.metrics.ObservePostingSize(float64(len(vectors)))
	return uint32(len(vectors)), nil
}

// PostingMapStore is a persistent store for vector IDs.
type PostingMapStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix byte
}

func NewPostingMapStore(bucket *lsmkv.Bucket, keyPrefix byte) *PostingMapStore {
	return &PostingMapStore{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingMapStore) key(postingID uint64) [9]byte {
	var buf [9]byte
	buf[0] = p.keyPrefix
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	return buf
}

// Get retrieves the vector IDs for the given posting ID.
func (p *PostingMapStore) Get(ctx context.Context, postingID uint64) ([]uint64, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	numVectors := binary.LittleEndian.Uint32(v[:4])
	vectorIDs := make([]uint64, numVectors)
	for i := uint32(0); i < numVectors; i++ {
		start := 4 + i*8
		end := start + 8
		vectorIDs[i] = binary.LittleEndian.Uint64(v[start:end])
	}

	return vectorIDs, nil
}

// Set adds or replaces the vector IDs for the given posting ID.
func (p *PostingMapStore) Set(ctx context.Context, postingID uint64, vectorIDs []uint64) error {
	key := p.key(postingID)

	buf := make([]byte, 0, 4+8*len(vectorIDs))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(vectorIDs)))
	for _, vectorID := range vectorIDs {
		buf = binary.LittleEndian.AppendUint64(buf, vectorID)
	}

	return p.bucket.Put(key[:], buf)
}
