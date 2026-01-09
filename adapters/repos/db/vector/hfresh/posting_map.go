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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingMetadata holds the list of vector IDs associated with a posting.
// This value is cached in memory for fast access.
// Any read or modification to the vectors slice must be protected by the mutex.
type PostingMetadata struct {
	sync.RWMutex
	vectors []VectorMetadata
}

// VectorMetadata holds the ID and version of a vector.
type VectorMetadata struct {
	ID      uint64
	Version VectorVersion
}

// IsValid checks if the vector is outdated or deleted.
func (v *VectorMetadata) IsValid(ctx context.Context, vmap *VersionMap) (bool, error) {
	currentVersion, err := vmap.Get(ctx, v.ID)
	if err != nil {
		return false, err
	}

	return v.Version == currentVersion && !v.Version.Deleted(), nil
}

// PostingMap manages various information about postings.
type PostingMap struct {
	metrics *Metrics
	cache   *otter.Cache[uint64, *PostingMetadata]
	bucket  *PostingMapStore
}

func NewPostingMap(bucket *lsmkv.Bucket, metrics *Metrics) *PostingMap {
	b := NewPostingMapStore(bucket, postingMapBucketPrefix)

	cache, _ := otter.New[uint64, *PostingMetadata](nil)

	return &PostingMap{
		cache:   cache,
		metrics: metrics,
		bucket:  b,
	}
}

// Get returns the vector IDs associated with this posting.
func (v *PostingMap) Get(ctx context.Context, postingID uint64) (*PostingMetadata, error) {
	m, err := v.cache.Get(ctx, postingID, otter.LoaderFunc[uint64, *PostingMetadata](func(ctx context.Context, key uint64) (*PostingMetadata, error) {
		vids, err := v.bucket.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return nil, otter.ErrNotFound
			}

			return nil, err
		}

		return &PostingMetadata{vectors: vids}, nil
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

	m.RLock()
	size := uint32(len(m.vectors))
	m.RUnlock()

	return size, nil
}

// SetVectorIDs sets the vector IDs for the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) SetVectorIDs(ctx context.Context, postingID uint64, posting Posting) error {
	if len(posting) == 0 {
		err := v.bucket.Delete(ctx, postingID)
		if err != nil {
			return err
		}
		v.cache.Invalidate(postingID)
		return nil
	}

	vectorIDs := make([]VectorMetadata, len(posting))
	for i, vector := range posting {
		vectorIDs[i] = VectorMetadata{
			ID:      vector.ID(),
			Version: vector.Version(),
		}
	}

	err := v.bucket.Set(ctx, postingID, vectorIDs)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, &PostingMetadata{vectors: vectorIDs})

	v.metrics.ObservePostingSize(float64(len(vectorIDs)))

	return nil
}

// AddVectorID adds a vector ID to the posting with the given ID.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) AddVectorID(ctx context.Context, postingID uint64, vectorID uint64, version VectorVersion) (uint32, error) {
	m, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}

	if m != nil {
		m.Lock()
		m.vectors = append(m.vectors, VectorMetadata{ID: vectorID, Version: version})
		m.Unlock()
	} else {
		m = &PostingMetadata{
			vectors: []VectorMetadata{{ID: vectorID, Version: version}},
		}
	}

	err = v.bucket.Set(ctx, postingID, m.vectors)
	if err != nil {
		return 0, err
	}

	v.cache.Set(postingID, m)

	v.metrics.ObservePostingSize(float64(len(m.vectors)))
	return uint32(len(m.vectors)), nil
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
func (p *PostingMapStore) Get(ctx context.Context, postingID uint64) ([]VectorMetadata, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	numVectors := binary.LittleEndian.Uint32(v[:4])
	vectorIDs := make([]VectorMetadata, numVectors)
	for i := uint32(0); i < numVectors; i++ {
		start := 4 + i*16
		end := start + 16
		vectorIDs[i] = VectorMetadata{
			ID:      binary.LittleEndian.Uint64(v[start : start+8]),
			Version: VectorVersion(binary.LittleEndian.Uint64(v[start+8 : end])),
		}
	}

	return vectorIDs, nil
}

// Set adds or replaces the vector IDs for the given posting ID.
func (p *PostingMapStore) Set(ctx context.Context, postingID uint64, vectorIDs []VectorMetadata) error {
	key := p.key(postingID)

	buf := make([]byte, 0, 4+16*len(vectorIDs))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(vectorIDs)))
	for _, vectorID := range vectorIDs {
		buf = binary.LittleEndian.AppendUint64(buf, vectorID.ID)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(vectorID.Version))
	}

	return p.bucket.Put(key[:], buf)
}

func (p *PostingMapStore) Delete(ctx context.Context, postingID uint64) error {
	key := p.key(postingID)
	return p.bucket.Delete(key[:])
}
