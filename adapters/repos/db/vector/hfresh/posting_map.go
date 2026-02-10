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
	"slices"
	"sync"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// PostingMetadata holds the list of vector IDs associated with a posting.
// This value is cached in memory for fast access.
// Any read or modification to the vectors slice must be protected by the mutex.
type PostingMetadata struct {
	sync.RWMutex
	PackedPostingMetadata
	// whether this cached entry has been loaded from disk
	// or has been refreshed by a background operation.
	fromDisk bool
}

// PostingMap manages various information about postings.
type PostingMap struct {
	metrics *Metrics
	data    *xsync.Map[uint64, *PostingMetadata]
	bucket  *PostingMapStore
}

func NewPostingMap(bucket *lsmkv.Bucket, metrics *Metrics) *PostingMap {
	b := NewPostingMapStore(bucket, postingMapBucketPrefix)

	return &PostingMap{
		data:    xsync.NewMap[uint64, *PostingMetadata](),
		metrics: metrics,
		bucket:  b,
	}
}

// Iter returns an iterator over all postings in the map.
func (v *PostingMap) Iter() iter.Seq2[uint64, *PostingMetadata] {
	return v.data.AllRelaxed()
}

// Get returns the vector IDs associated with this posting.
func (v *PostingMap) Get(ctx context.Context, postingID uint64) (*PostingMetadata, error) {
	m, ok := v.data.Load(postingID)
	if !ok {
		return nil, ErrPostingNotFound
	}

	return m, nil
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
	size := uint32(m.Count())
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
		v.data.Delete(postingID)
		return nil
	}

	var pm PackedPostingMetadata
	for _, vector := range posting {
		pm = pm.AddVector(vector.ID(), vector.Version())
	}

	err := v.bucket.Set(ctx, postingID, pm)
	if err != nil {
		return err
	}
	v.data.Store(postingID, &PostingMetadata{PackedPostingMetadata: pm})
	v.metrics.ObservePostingSize(float64(pm.Count()))

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
		m.PackedPostingMetadata = m.AddVector(vectorID, version)
		m.Unlock()
	} else {
		m = &PostingMetadata{
			PackedPostingMetadata: NewPackedPostingMetadata([]uint64{vectorID}, []VectorVersion{version}),
		}
		v.data.Store(postingID, m)
	}

	v.metrics.ObservePostingSize(float64(m.Count()))
	return uint32(m.Count()), nil
}

// Persist the vector IDs for the posting with the given ID to disk.
func (v *PostingMap) Persist(ctx context.Context, postingID uint64) error {
	m, err := v.Get(ctx, postingID)
	if err != nil {
		return err
	}

	m.RLock()
	defer m.RUnlock()

	return v.bucket.Set(ctx, postingID, m.PackedPostingMetadata)
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

type PackedPostingMetadata []byte

func NewPackedPostingMetadata(vectorIDs []uint64, versions []VectorVersion) PackedPostingMetadata {
	var data PackedPostingMetadata
	for i, id := range vectorIDs {
		data = data.AddVector(id, versions[i])
	}
	return data
}

func (p PackedPostingMetadata) Iter() iter.Seq2[uint64, VectorVersion] {
	if len(p) < 5 {
		return func(yield func(uint64, VectorVersion) bool) {}
	}

	scheme := Scheme(p[0])
	count := binary.LittleEndian.Uint32(p[1:5])
	bytesPerID := scheme.BytesPerValue()
	bytesPerValue := bytesPerID + 1 // ID + version byte
	start := 5
	data := p[start:]

	return func(yield func(uint64, VectorVersion) bool) {
		for i := range count {
			offset := i * uint32(bytesPerValue)

			// Decode ID
			vID := uint64(0)
			for j := range bytesPerID {
				vID |= uint64(data[offset+uint32(j)]) << (j * 8)
			}

			// Decode version
			vVer := VectorVersion(data[offset+uint32(bytesPerID)])

			if !yield(vID, vVer) {
				return
			}
		}
	}
}

func (p PackedPostingMetadata) GetAt(index int) (uint64, VectorVersion) {
	scheme := Scheme(p[0])
	count := binary.LittleEndian.Uint32(p[1:5])
	if index >= int(count) {
		return 0, 0
	}
	bytesPerID := scheme.BytesPerValue()
	bytesPerValue := bytesPerID + 1
	start := 5
	offset := index * bytesPerValue

	// Decode ID
	vID := uint64(0)
	for j := 0; j < bytesPerID; j++ {
		vID |= uint64(p[start+offset+j]) << (j * 8)
	}

	// Decode version
	vVer := VectorVersion(p[start+offset+bytesPerID])

	return vID, vVer
}

// Count returns the number of vector IDs in the posting metadata.
func (p PackedPostingMetadata) Count() uint32 {
	if len(p) < 5 {
		return 0
	}
	return binary.LittleEndian.Uint32(p[1:5])
}

// AddVector adds a new vector ID to the packed metadata, returning a new PackedPostingMetadata.
// It handles upgrading the encoding scheme if the new vector ID exceeds the current scheme's limits.
func (p PackedPostingMetadata) AddVector(vectorID uint64, version VectorVersion) PackedPostingMetadata {
	const headerSize = 5
	var currentScheme, newScheme Scheme
	var currentCount uint32

	if len(p) == 0 {
		currentScheme = schemeFor(vectorID)
		newScheme = currentScheme
		p = make([]byte, headerSize, headerSize+currentScheme.BytesPerValue()+1)
		p[0] = byte(currentScheme)
	} else {
		currentScheme = Scheme(p[0])
		newScheme = schemeFor(vectorID)
		currentCount = binary.LittleEndian.Uint32(p[1:5])
	}

	newCount := currentCount + 1

	// same scheme or lower, just append the new ID and version
	// using the current scheme's byte width (not the new one)
	if currentScheme >= newScheme {
		currentBytesPerValue := currentScheme.BytesPerValue()
		for i := range currentBytesPerValue {
			p = append(p, byte(vectorID>>(i*8)))
		}
		p = append(p, byte(version))

		// update count in header
		binary.LittleEndian.PutUint32(p[1:5], newCount)
		return p
	}

	// new scheme needed, re-encode all existing IDs with the new scheme and append the new ID
	bytesPerValue := newScheme.BytesPerValue()
	idsSize := int(newCount) * bytesPerValue
	versionsSize := int(newCount) // 1 byte per version

	newData := make([]byte, headerSize+idsSize+versionsSize)
	// write new header
	newData[0] = byte(newScheme)
	// write count
	binary.LittleEndian.PutUint32(newData[1:5], newCount)

	// write IDs and versions
	offset := headerSize
	for id, ver := range p.Iter() {
		// write the id
		for j := range bytesPerValue {
			newData[offset] = byte(id >> (j * 8))
			offset++
		}
		// write the version
		newData[offset] = byte(ver)
		offset++
	}

	// write the new ID
	for j := range bytesPerValue {
		newData[offset] = byte(vectorID >> (j * 8))
		offset++
	}
	// write the new version
	newData[offset] = byte(version)

	return newData
}

// Get retrieves the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * (bytesPerScheme + 1): vector IDs and version
func (p *PostingMapStore) Get(ctx context.Context, postingID uint64) (PackedPostingMetadata, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	return PackedPostingMetadata(v), nil
}

// Set adds or replaces the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * (bytesPerScheme + 1): vector IDs and version
func (p *PostingMapStore) Set(ctx context.Context, postingID uint64, metadata PackedPostingMetadata) error {
	key := p.key(postingID)
	return p.bucket.Put(key[:], metadata)
}

func (p *PostingMapStore) Delete(ctx context.Context, postingID uint64) error {
	key := p.key(postingID)
	return p.bucket.Delete(key[:])
}
