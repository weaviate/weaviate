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
	"iter"
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
	vectors []uint64
	version []VectorVersion
	// whether this cached entry has been loaded from disk
	// or has been refreshed by a background operation.
	fromDisk bool
}

// Iter returns an iterator over the vector metadata in the posting.
func (m *PostingMetadata) Iter() iter.Seq2[int, *VectorMetadata] {
	var v VectorMetadata

	return func(yield func(int, *VectorMetadata) bool) {
		for i := range m.vectors {
			v.ID = m.vectors[i]
			v.Version = m.version[i]
			if !yield(i, &v) {
				return
			}
		}
	}
}

// GetValidVectors returns the list of vector IDs that are still valid
// according to the provided VersionMap.
func (m *PostingMetadata) GetValidVectors(ctx context.Context, vmap *VersionMap) ([]uint64, error) {
	m.RLock()
	defer m.RUnlock()

	validVectors := make([]uint64, 0, len(m.vectors))

	for i, vectorID := range m.vectors {
		currentVersion, err := vmap.Get(ctx, vectorID)
		if err != nil {
			return nil, err
		}

		if m.version[i] == currentVersion && !m.version[i].Deleted() {
			validVectors = append(validVectors, vectorID)
		}
	}

	return validVectors, nil
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
		vids, vvers, err := v.bucket.Get(ctx, postingID)
		if err != nil {
			if errors.Is(err, ErrPostingNotFound) {
				return nil, otter.ErrNotFound
			}

			return nil, err
		}

		return &PostingMetadata{vectors: vids, version: vvers, fromDisk: true}, nil
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

// SetVectorIDs sets the vector IDs for the posting with the given ID in-memory and persists them to disk.
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

	vectorIDs := make([]uint64, len(posting))
	vectorVersions := make([]VectorVersion, len(posting))
	for i, vector := range posting {
		vectorIDs[i] = vector.ID()
		vectorVersions[i] = vector.Version()
	}

	err := v.bucket.Set(ctx, postingID, vectorIDs, vectorVersions)
	if err != nil {
		return err
	}
	v.cache.Set(postingID, &PostingMetadata{vectors: vectorIDs, version: vectorVersions})
	v.metrics.ObservePostingSize(float64(len(vectorIDs)))

	return nil
}

// FastAddVectorID adds a vector ID to the posting with the given ID
// but only updates the in-memory cache.
// The store is updated asynchronously by an analyze, split, or merge operation.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) FastAddVectorID(ctx context.Context, postingID uint64, vectorID uint64, version VectorVersion) (uint32, error) {
	m, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}

	if m != nil {
		m.Lock()
		m.vectors = append(m.vectors, vectorID)
		m.version = append(m.version, version)
		m.Unlock()
	} else {
		m = &PostingMetadata{
			vectors: []uint64{vectorID},
			version: []VectorVersion{version},
		}
		v.cache.Set(postingID, m)
	}

	v.metrics.ObservePostingSize(float64(len(m.vectors)))
	return uint32(len(m.vectors)), nil
}

// Persist the vector IDs for the posting with the given ID to disk.
func (v *PostingMap) Persist(ctx context.Context, postingID uint64) error {
	m, err := v.Get(ctx, postingID)
	if err != nil {
		return err
	}

	m.RLock()
	defer m.RUnlock()

	return v.bucket.Set(ctx, postingID, m.vectors, m.version)
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
func (p *PostingMapStore) Get(ctx context.Context, postingID uint64) ([]uint64, []VectorVersion, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, nil, ErrPostingNotFound
	}

	numVectors := binary.LittleEndian.Uint32(v[:4])
	vectorIDs := make([]uint64, numVectors)
	vectorVersions := make([]VectorVersion, numVectors)
	for i := uint32(0); i < numVectors; i++ {
		start := 4 + i*16
		end := start + 16
		vectorIDs[i] = binary.LittleEndian.Uint64(v[start : start+8])
		vectorVersions[i] = VectorVersion(binary.LittleEndian.Uint64(v[start+8 : end]))
	}

	return vectorIDs, vectorVersions, nil
}

// Set adds or replaces the vector IDs for the given posting ID.
func (p *PostingMapStore) Set(ctx context.Context, postingID uint64, vectorIDs []uint64, vectorVersions []VectorVersion) error {
	key := p.key(postingID)

	buf := make([]byte, 0, 4+16*len(vectorIDs))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(vectorIDs)))
	for i, vectorID := range vectorIDs {
		buf = binary.LittleEndian.AppendUint64(buf, vectorID)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(vectorVersions[i]))
	}

	return p.bucket.Put(key[:], buf)
}

func (p *PostingMapStore) Delete(ctx context.Context, postingID uint64) error {
	key := p.key(postingID)
	return p.bucket.Delete(key[:])
}
