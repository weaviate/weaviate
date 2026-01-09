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
	"sync"

	"github.com/maypok86/otter/v2"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingMap manages various information about postings.
type PostingMap struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, *PostingMap]
	bucket  *PostingMetadataBucket
	pool    sync.Pool
}

func NewPostingMap(bucket *lsmkv.Bucket, metrics *Metrics) *PostingMap {
	b := NewPostingMetadataBucket(bucket, postingMetadataBucketPrefix)

	cache, _ := otter.New[uint64, *PostingMap](nil)

	return &PostingMap{
		cache:   cache,
		metrics: metrics,
		bucket:  b,
		pool: sync.Pool{
			New: func() any {
				s := make([]uint64, 256)
				return &s
			},
		},
	}
}

func (v *PostingMap) getSliceFromPool(size int, capacity int) *[]uint64 {
	s := v.pool.Get().(*[]uint64)
	if cap(*s) < size {
		if capacity <= 0 {
			*s = make([]uint64, size)
		} else {
			*s = make([]uint64, size, capacity)
		}
	}
	*s = (*s)[:size]
	return s
}

func (v *PostingMap) invalidate(m *PostingMap) {
	if m == nil {
		return
	}

	m.m.Lock()
	if !m.cacheInvalidated {
		m.cacheInvalidated = true
	}
	m.m.Unlock()
}

// Get returns the size of the posting with the given ID.
// The returned function must be called to release the posting back to the pool.
func (v *PostingMap) Get(ctx context.Context, postingID uint64) (*PostingMap, func(), error) {
	m, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, *PostingMap](func(ctx context.Context, key uint64) (*PostingMap, error) {
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
		return nil, func() {}, ErrPostingNotFound
	}
	if err != nil {
		return nil, func() {}, err
	}

	return m, func() {
		if m == nil {
			return
		}
		m.m.Lock()
		if m.cacheInvalidated {
			v.pool.Put(&m.Vectors)
		}
		m.m.Unlock()
	}, err
}

// CountVectorIDs returns the number of vector IDs in the posting with the given ID.
// If the posting does not exist, it returns 0.
func (v *PostingMap) CountVectorIDs(ctx context.Context, postingID uint64) (uint32, error) {
	m, release, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}
	defer release()

	if m == nil {
		return 0, nil
	}

	return uint32(len(m.Vectors)), nil
}

// GetVersion returns the version of the posting with the given ID.
func (v *PostingMap) GetVersion(ctx context.Context, postingID uint64) (uint32, error) {
	m, release, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}
	defer release()

	if m == nil {
		return 0, nil
	}

	return m.Version, nil
}

// SetVectorIDs sets the vector IDs for the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) SetVectorIDs(ctx context.Context, postingID uint64, posting Posting) error {
	old, release, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return err
	}
	defer release()

	metadata := PostingMap{
		Vectors: *v.getSliceFromPool(len(posting), 0),
	}
	for i, vector := range posting {
		metadata.Vectors[i] = vector.ID()
	}
	if old != nil {
		metadata.Version = old.Version
	}

	err = v.bucket.Set(ctx, postingID, &metadata)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, &metadata)

	v.metrics.ObservePostingSize(float64(len(metadata.Vectors)))

	v.invalidate(old)

	return nil
}

// AddVectorID adds a vector ID to the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) AddVectorID(ctx context.Context, postingID uint64, vectorID uint64) (uint32, error) {
	old, release, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}
	defer release()

	var metadata PostingMap

	if old != nil {
		metadata.Vectors = *v.getSliceFromPool(len(old.Vectors), len(old.Vectors)+1)
		copy(metadata.Vectors, old.Vectors)
		metadata.Version = old.Version
	}
	metadata.Vectors = append(metadata.Vectors, vectorID)

	err = v.bucket.Set(ctx, postingID, &metadata)
	if err != nil {
		return 0, err
	}

	v.cache.Set(postingID, &metadata)

	v.metrics.ObservePostingSize(float64(len(metadata.Vectors)))

	v.invalidate(old)

	return uint32(len(metadata.Vectors)), nil
}

// SetVersion sets the version for the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) SetVersion(ctx context.Context, postingID uint64, newVersion uint32) error {
	old, release, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return err
	}
	defer release()

	var metadata PostingMap
	if old != nil {
		metadata.Vectors = old.Vectors // no need to copy, we won't modify
	}
	metadata.Version = newVersion

	err = v.bucket.Set(ctx, postingID, &metadata)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, &metadata)

	return nil
}

// PostingMetadataBucket is a persistent store for posting metadata.
type PostingMetadataBucket struct {
	bucket    *lsmkv.Bucket
	keyPrefix byte
}

func NewPostingMetadataBucket(bucket *lsmkv.Bucket, keyPrefix byte) *PostingMetadataBucket {
	return &PostingMetadataBucket{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingMetadataBucket) key(postingID uint64) [9]byte {
	var buf [9]byte
	buf[0] = p.keyPrefix
	binary.LittleEndian.PutUint64(buf[1:], postingID)
	return buf
}

// Get retrieves the posting metadata for the given posting ID.
func (p *PostingMetadataBucket) Get(ctx context.Context, postingID uint64) (*PostingMap, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	var metadata PostingMap
	err = msgpack.Unmarshal(v, &metadata)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal posting metadata for %d", postingID)
	}

	return &metadata, nil
}

// Set adds or replaces the posting metadata for the given posting ID.
func (p *PostingMetadataBucket) Set(ctx context.Context, postingID uint64, metadata *PostingMap) error {
	key := p.key(postingID)

	data, err := msgpack.Marshal(metadata)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal posting metadata for %d", postingID)
	}

	return p.bucket.Put(key[:], data)
}
