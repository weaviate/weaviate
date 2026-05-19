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
	"bytes"
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
// Any read or modification to the vectors slice must be protected by the mutex.
type PostingMetadata struct {
	sync.RWMutex
	PackedPostingMetadata
}

// PostingMap manages various information about postings.
type PostingMap struct {
	data   *xsync.Map[uint64, *PostingMetadata]
	bucket *PostingMapStore
}

func NewPostingMap(bucket *lsmkv.Bucket) *PostingMap {
	b := NewPostingMapStore(bucket, postingMapBucketPrefix)

	return &PostingMap{
		data:   xsync.NewMap[uint64, *PostingMetadata](),
		bucket: b,
	}
}

// Size returns the total number of postings in the map.
func (v *PostingMap) Size() int {
	return v.data.Size()
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

// CountVectors returns the number of vector IDs in the posting with the given ID.
// If the posting does not exist, it returns 0.
func (v *PostingMap) CountVectors(ctx context.Context, postingID uint64) (uint32, error) {
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

// CountAllVectors returns the total number of vector IDs across all postings,
// including deleted vectors that have not yet been cleaned up.
// This is used for metrics and does not need to be exact, so it iterates over the in-memory cache without locking.
func (v *PostingMap) CountAllVectors(ctx context.Context) (uint64, error) {
	var total uint64
	for _, m := range v.data.AllRelaxed() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		m.RLock()
		count := m.Count()
		m.RUnlock()

		total += uint64(count)
	}

	return total, nil
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
		pm = pm.AddVector(vector.ID())
	}
	pm = pm.Compact()

	existing, err := v.bucket.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return errors.Wrapf(err, "failed to get existing posting metadata for posting %d", postingID)
	}
	if err == nil && bytes.Equal(pm, existing) {
		// no change, skip the update
		return nil
	}

	// store the updated posting metadata on disk and update the in-memory cache
	err = v.bucket.Set(ctx, postingID, pm)
	if err != nil {
		return err
	}

	// update the in-memory cache, if the posting metadata already exists,
	// update it in-place to avoid unnecessary allocations
	v.data.Compute(postingID, func(oldValue *PostingMetadata, loaded bool) (newValue *PostingMetadata, op xsync.ComputeOp) {
		if !loaded {
			return &PostingMetadata{PackedPostingMetadata: pm}, xsync.UpdateOp
		}

		oldValue.Lock()
		if !bytes.Equal(oldValue.PackedPostingMetadata, pm) {
			oldValue.PackedPostingMetadata = pm
		}
		oldValue.Unlock()

		// we modified the internal pointer of the existing value, so we don't need to update the map
		return oldValue, xsync.CancelOp
	})

	return nil
}

// FastAddVectorID adds a vector ID to the posting with the given ID
// but only updates the in-memory cache.
// The store is updated asynchronously by an analyze, split, or merge operation.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) FastAddVectorID(ctx context.Context, postingID uint64, vectorID uint64) (uint32, error) {
	m, err := v.Get(ctx, postingID)
	if err != nil && !errors.Is(err, ErrPostingNotFound) {
		return 0, err
	}

	if m != nil {
		m.Lock()
		m.PackedPostingMetadata = m.AddVector(vectorID)
		count := m.Count()
		m.Unlock()
		return uint32(count), nil
	} else {
		m = &PostingMetadata{
			PackedPostingMetadata: NewPackedPostingMetadata([]uint64{vectorID}),
		}
		count := m.Count()
		v.data.Store(postingID, m)
		return uint32(count), nil
	}
}

// Restore loads all postings from disk into memory. It should be called during startup to populate the in-memory cache.
func (v *PostingMap) Restore(ctx context.Context) error {
	return v.bucket.Iter(ctx, func(u uint64, ppm PackedPostingMetadata) error {
		v.data.Store(u, &PostingMetadata{PackedPostingMetadata: ppm})
		return nil
	})
}

// PostingMapStore is a persistent store for vector IDs.
type PostingMapStore struct {
	bucket    *lsmkv.Bucket
	keyPrefix []byte
}

func NewPostingMapStore(bucket *lsmkv.Bucket, keyPrefix []byte) *PostingMapStore {
	return &PostingMapStore{
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
}

func (p *PostingMapStore) key(postingID uint64) []byte {
	buf := make([]byte, len(p.keyPrefix)+8)
	copy(buf, p.keyPrefix)
	binary.LittleEndian.PutUint64(buf[len(p.keyPrefix):], postingID)
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

func NewPackedPostingMetadata(vectorIDs []uint64) PackedPostingMetadata {
	var data PackedPostingMetadata
	for _, id := range vectorIDs {
		data = data.AddVector(id)
	}
	return data
}

func (p PackedPostingMetadata) Iter() iter.Seq[uint64] {
	if len(p) < 5 {
		return func(yield func(uint64) bool) {}
	}

	scheme := Scheme(p[0])
	count := binary.LittleEndian.Uint32(p[1:5])
	bytesPerID := scheme.BytesPerValue()
	start := 5
	data := p[start:]

	return func(yield func(uint64) bool) {
		for i := range count {
			offset := i * uint32(bytesPerID)

			// Decode ID
			vID := uint64(0)
			for j := range bytesPerID {
				vID |= uint64(data[offset+uint32(j)]) << (j * 8)
			}

			if !yield(vID) {
				return
			}
		}
	}
}

func (p PackedPostingMetadata) GetAt(index int) uint64 {
	scheme := Scheme(p[0])
	count := binary.LittleEndian.Uint32(p[1:5])
	if index >= int(count) {
		return 0
	}
	bytesPerID := scheme.BytesPerValue()
	start := 5
	offset := index * bytesPerID

	// Decode ID
	vID := uint64(0)
	for j := 0; j < bytesPerID; j++ {
		vID |= uint64(p[start+offset+j]) << (j * 8)
	}

	return vID
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
func (p PackedPostingMetadata) AddVector(vectorID uint64) PackedPostingMetadata {
	const headerSize = 5
	var currentScheme, newScheme Scheme
	var currentCount uint32

	if len(p) == 0 {
		currentScheme = schemeFor(vectorID)
		newScheme = currentScheme
		p = bufferPool.Get(headerSize, headerSize+currentScheme.BytesPerValue())
		p[0] = byte(currentScheme)
	} else {
		currentScheme = Scheme(p[0])
		newScheme = schemeFor(vectorID)
		currentCount = binary.LittleEndian.Uint32(p[1:5])
	}

	newCount := currentCount + 1

	// same scheme or lower, just append the new ID
	// using the current scheme's byte width (not the new one)
	if currentScheme >= newScheme {
		currentBytesPerValue := currentScheme.BytesPerValue()
		newSize := headerSize + int(newCount)*currentBytesPerValue
		if cap(p) < newSize {
			newP := bufferPool.Get(len(p), growPackedPostingMetadataCapacity(cap(p), newSize))
			copy(newP, p)
			bufferPool.Put(p)
			p = newP
		}

		for i := range currentBytesPerValue {
			p = append(p, byte(vectorID>>(i*8)))
		}

		// update count in header
		binary.LittleEndian.PutUint32(p[1:5], newCount)
		return p
	}

	// new scheme needed, re-encode all existing IDs with the new scheme and append the new ID
	bytesPerValue := newScheme.BytesPerValue()
	newSize := headerSize + int(newCount)*bytesPerValue

	newData := bufferPool.Get(newSize, newSize)
	// write new header
	newData[0] = byte(newScheme)
	// write count
	binary.LittleEndian.PutUint32(newData[1:5], newCount)

	// write IDs
	offset := headerSize
	for id := range p.Iter() {
		// write the id
		for j := range bytesPerValue {
			newData[offset] = byte(id >> (j * 8))
			offset++
		}
	}

	// write the new ID
	for j := range bytesPerValue {
		newData[offset] = byte(vectorID >> (j * 8))
		offset++
	}

	bufferPool.Put(p)

	return newData
}

func (p PackedPostingMetadata) Compact() PackedPostingMetadata {
	if len(p) == cap(p) {
		return p
	}

	compact := make(PackedPostingMetadata, len(p))
	copy(compact, p)
	bufferPool.Put(p)

	return compact
}

// growPackedPostingMetadataCapacity calculates a new capacity for a PackedPostingMetadata
// buffer when adding a new vector ID would exceed the current capacity.
func growPackedPostingMetadataCapacity(current, needed int) int {
	if current <= 0 {
		return needed
	}

	growth := current / 4
	if growth < 64 {
		growth = 64
	}

	return max(needed, current+growth)
}

// Get retrieves the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * bytesPerScheme: vector IDs
func (p *PostingMapStore) Get(ctx context.Context, postingID uint64) (PackedPostingMetadata, error) {
	key := p.key(postingID)
	v, err := p.bucket.Get(key[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get posting size for %d", postingID)
	}
	if len(v) == 0 {
		return nil, ErrPostingNotFound
	}

	return normalizePackedPostingMetadata(PackedPostingMetadata(v), false), nil
}

// Set adds or replaces the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * bytesPerScheme: vector IDs
func (p *PostingMapStore) Set(ctx context.Context, postingID uint64, metadata PackedPostingMetadata) error {
	key := p.key(postingID)
	// copy metadata to a new array
	metadataCopy := bufferPool.Get(len(metadata), len(metadata))
	copy(metadataCopy, metadata)
	return p.bucket.Put(key[:], metadataCopy)
}

func (p *PostingMapStore) Delete(ctx context.Context, postingID uint64) error {
	key := p.key(postingID)
	return p.bucket.Delete(key[:])
}

func (p *PostingMapStore) Iter(ctx context.Context, fn func(uint64, PackedPostingMetadata) error) error {
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
		metadata := normalizePackedPostingMetadata(PackedPostingMetadata(v), true)
		err := fn(postingID, metadata)
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

// normalizePackedPostingMetadata checks if the given metadata is in a compact format and if not,
// it creates a new compact copy of it.
func normalizePackedPostingMetadata(metadata PackedPostingMetadata, copyCurrent bool) PackedPostingMetadata {
	if len(metadata) < 5 {
		return metadata
	}

	scheme := Scheme(metadata[0])
	count := int(binary.LittleEndian.Uint32(metadata[1:5]))
	bytesPerID := scheme.BytesPerValue()
	currentSize := 5 + count*bytesPerID
	if len(metadata) == currentSize {
		if !copyCurrent {
			return metadata
		}
		normalized := bufferPool.Get(len(metadata), len(metadata))
		copy(normalized, metadata)
		return normalized
	}

	legacySize := 5 + count*(bytesPerID+1)
	if len(metadata) != legacySize {
		if !copyCurrent {
			return metadata
		}
		normalized := bufferPool.Get(len(metadata), len(metadata))
		copy(normalized, metadata)
		return normalized
	}

	normalized := bufferPool.Get(currentSize, currentSize)
	copy(normalized[:5], metadata[:5])
	for i := range count {
		src := 5 + i*(bytesPerID+1)
		dst := 5 + i*bytesPerID
		copy(normalized[dst:dst+bytesPerID], metadata[src:src+bytesPerID])
	}

	return normalized
}
