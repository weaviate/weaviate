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

// Encoding schemes for vector IDs - same as packedconn
const (
	schemeID2Byte Scheme = iota // 2 bytes per value (0-65535)
	schemeID3Byte               // 3 bytes per value (0-16777215)
	schemeID4Byte               // 4 bytes per value (0-4294967295)
	schemeID5Byte               // 5 bytes per value (0-1099511627775)
	schemeID8Byte               // 8 bytes per value (full uint64)
)

type Scheme uint8

// BytesPerValue returns the number of bytes used per value for the given scheme.
func (s Scheme) BytesPerValue() int {
	switch s {
	case schemeID2Byte:
		return 2
	case schemeID3Byte:
		return 3
	case schemeID4Byte:
		return 4
	case schemeID5Byte:
		return 5
	default:
		return 8
	}
}

// determineScheme analyzes values to pick the most efficient encoding.
func determineScheme(values []uint64) Scheme {
	if len(values) == 0 {
		return schemeID2Byte
	}

	maxVal := slices.Max(values)

	return schemeFor(maxVal)
}

func schemeFor(value uint64) Scheme {
	if value <= 65535 {
		return schemeID2Byte
	} else if value <= 16777215 {
		return schemeID3Byte
	} else if value <= 4294967295 {
		return schemeID4Byte
	} else if value <= 1099511627775 {
		return schemeID5Byte
	}
	return schemeID8Byte
}

// encodeVectorIDs encodes vector IDs using the specified scheme.
func encodeVectorIDs(values []uint64, scheme uint8) []byte {
	switch scheme {
	case schemeID2Byte:
		data := make([]byte, len(values)*2)
		for i, val := range values {
			data[i*2] = byte(val)
			data[i*2+1] = byte(val >> 8)
		}
		return data

	case schemeID3Byte:
		data := make([]byte, len(values)*3)
		for i, val := range values {
			data[i*3] = byte(val)
			data[i*3+1] = byte(val >> 8)
			data[i*3+2] = byte(val >> 16)
		}
		return data

	case schemeID4Byte:
		data := make([]byte, len(values)*4)
		for i, val := range values {
			data[i*4] = byte(val)
			data[i*4+1] = byte(val >> 8)
			data[i*4+2] = byte(val >> 16)
			data[i*4+3] = byte(val >> 24)
		}
		return data

	case schemeID5Byte:
		data := make([]byte, len(values)*5)
		for i, val := range values {
			data[i*5] = byte(val)
			data[i*5+1] = byte(val >> 8)
			data[i*5+2] = byte(val >> 16)
			data[i*5+3] = byte(val >> 24)
			data[i*5+4] = byte(val >> 32)
		}
		return data

	default: // schemeID8Byte
		data := make([]byte, len(values)*8)
		for i, val := range values {
			for j := 0; j < 8; j++ {
				data[i*8+j] = byte(val >> (j * 8))
			}
		}
		return data
	}
}

// decodeVectorIDs decodes vector IDs using the specified scheme.
func decodeVectorIDs(data []byte, scheme uint8, count uint32) []uint64 {
	result := make([]uint64, count)

	switch scheme {
	case schemeID2Byte:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*2]) | uint64(data[i*2+1])<<8
		}

	case schemeID3Byte:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*3]) | uint64(data[i*3+1])<<8 | uint64(data[i*3+2])<<16
		}

	case schemeID4Byte:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*4]) |
				uint64(data[i*4+1])<<8 |
				uint64(data[i*4+2])<<16 |
				uint64(data[i*4+3])<<24
		}

	case schemeID5Byte:
		for i := uint32(0); i < count; i++ {
			result[i] = uint64(data[i*5]) |
				uint64(data[i*5+1])<<8 |
				uint64(data[i*5+2])<<16 |
				uint64(data[i*5+3])<<24 |
				uint64(data[i*5+4])<<32
		}

	default: // schemeID8Byte
		for i := uint32(0); i < count; i++ {
			val := uint64(0)
			for j := uint32(0); j < 8; j++ {
				val |= uint64(data[i*8+j]) << (j * 8)
			}
			result[i] = val
		}
	}

	return result
}

type packedPostingMetadata []byte

func (p packedPostingMetadata) Iter() iter.Seq2[uint64, VectorVersion] {
	scheme := p[0]
	count := binary.LittleEndian.Uint32(p[1:5])

	bytesPerID := bytesPerScheme(scheme)
	idsStart := 5
	idsEnd := idsStart + int(count)*bytesPerID
	versionsStart := idsEnd
	vectorIDs := p[idsStart:idsEnd]
	vectorVersions := p[versionsStart:]

	switch scheme {
	case schemeID2Byte:
		return func(yield func(uint64, VectorVersion) bool) {
			for i := uint32(0); i < count; i++ {
				vID := uint64(vectorIDs[i*2]) | uint64(vectorIDs[i*2+1])<<8
				vVer := VectorVersion(vectorVersions[i])
				if !yield(vID, vVer) {
					return
				}
			}
		}
	case schemeID3Byte:
		return func(yield func(uint64, VectorVersion) bool) {
			for i := uint32(0); i < count; i++ {
				vID := uint64(vectorIDs[i*3]) | uint64(vectorIDs[i*3+1])<<8 | uint64(vectorIDs[i*3+2])<<16
				vVer := VectorVersion(vectorVersions[i])
				if !yield(vID, vVer) {
					return
				}
			}
		}
	case schemeID4Byte:
		return func(yield func(uint64, VectorVersion) bool) {
			for i := uint32(0); i < count; i++ {
				vID := uint64(vectorIDs[i*4]) |
					uint64(vectorIDs[i*4+1])<<8 |
					uint64(vectorIDs[i*4+2])<<16 |
					uint64(vectorIDs[i*4+3])<<24
				vVer := VectorVersion(vectorVersions[i])
				if !yield(vID, vVer) {
					return
				}
			}
		}
	case schemeID5Byte:
		return func(yield func(uint64, VectorVersion) bool) {
			for i := uint32(0); i < count; i++ {
				vID := uint64(vectorIDs[i*5]) |
					uint64(vectorIDs[i*5+1])<<8 |
					uint64(vectorIDs[i*5+2])<<16 |
					uint64(vectorIDs[i*5+3])<<24 |
					uint64(vectorIDs[i*5+4])<<32
				vVer := VectorVersion(vectorVersions[i])
				if !yield(vID, vVer) {
					return
				}
			}
		}
	default: // schemeID8Byte
		return func(yield func(uint64, VectorVersion) bool) {
			for i := uint32(0); i < count; i++ {
				vID := uint64(0)
				for j := uint32(0); j < 8; j++ {
					vID |= uint64(vectorIDs[i*8+j]) << (j * 8)
				}
				vVer := VectorVersion(vectorVersions[i])
				if !yield(vID, vVer) {
					return
				}
			}
		}
	}
}

// Get retrieves the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * bytesPerScheme: vector IDs
//   - count * 1 byte: vector versions
func (p *PostingMapStore) Get(ctx context.Context, postingID uint64) ([]uint64, []VectorVersion, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, nil, ErrPostingNotFound
	}

	scheme := v[0]
	count := binary.LittleEndian.Uint32(v[1:5])

	bytesPerID := bytesPerScheme(scheme)
	idsStart := 5
	idsEnd := idsStart + int(count)*bytesPerID
	versionsStart := idsEnd

	vectorIDs := decodeVectorIDs(v[idsStart:idsEnd], scheme, count)
	vectorVersions := make([]VectorVersion, count)
	for i := uint32(0); i < count; i++ {
		vectorVersions[i] = VectorVersion(v[versionsStart+int(i)])
	}

	return vectorIDs, vectorVersions, nil
}

// Set adds or replaces the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * bytesPerScheme: vector IDs
//   - count * 1 byte: vector versions
func (p *PostingMapStore) Set(ctx context.Context, postingID uint64, vectorIDs []uint64, vectorVersions []VectorVersion) error {
	key := p.key(postingID)

	scheme := determineScheme(vectorIDs)
	bytesPerID := bytesPerScheme(scheme)
	count := len(vectorIDs)

	// Header: 1 byte scheme + 4 bytes count
	// Data: count * bytesPerID for IDs + count * 1 for versions
	bufSize := 5 + count*bytesPerID + count
	buf := make([]byte, bufSize)

	// Write header
	buf[0] = scheme
	binary.LittleEndian.PutUint32(buf[1:5], uint32(count))

	// Write vector IDs
	idsData := encodeVectorIDs(vectorIDs, scheme)
	copy(buf[5:], idsData)

	// Write versions (1 byte each)
	versionsStart := 5 + len(idsData)
	for i, ver := range vectorVersions {
		buf[versionsStart+i] = byte(ver)
	}

	return p.bucket.Put(key[:], buf)
}

func (p *PostingMapStore) Delete(ctx context.Context, postingID uint64) error {
	key := p.key(postingID)
	return p.bucket.Delete(key[:])
}
