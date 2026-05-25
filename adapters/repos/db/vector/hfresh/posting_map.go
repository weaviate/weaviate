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
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const postingMapMigrationBatchSize = 10_000

// PostingMetadata holds the list of vector IDs associated with a posting.
type PostingMetadata struct {
	PackedPostingMetadata
}

type postingMapSlot struct {
	metadata atomic.Pointer[PostingMetadata]
}

// PostingMap manages various information about postings.
type PostingMap struct {
	data   *common.GroupedPagedArray[postingMapSlot]
	bucket *PostingMapStore
	locks  *common.ShardedRWLocks
	count  atomic.Uint64
}

func NewPostingMap(bucket *lsmkv.Bucket) *PostingMap {
	b := NewPostingMapStore(bucket, postingMapBucketPrefixV2)

	return &PostingMap{
		data:   common.NewGroupedPagedArray[postingMapSlot](16*1024, 64*1024), // 1 billion posting IDs with 64k per page
		bucket: b,
		locks:  common.NewShardedRWLocks(512),
	}
}

// Size returns the total number of postings in the map.
func (v *PostingMap) Size() int {
	return int(v.count.Load())
}

func (v *PostingMap) RLock(postingID uint64) {
	v.locks.RLock(postingID)
}

func (v *PostingMap) RUnlock(postingID uint64) {
	v.locks.RUnlock(postingID)
}

// Iter returns an iterator over all postings in the map.
func (v *PostingMap) Iter() iter.Seq2[uint64, *PostingMetadata] {
	return func(yield func(uint64, *PostingMetadata) bool) {
		for postingID, slot := range v.data.IterAllocated() {
			metadata := slot.metadata.Load()
			if metadata == nil {
				continue
			}

			if !yield(postingID, metadata) {
				return
			}
		}
	}
}

// Get returns the vector IDs associated with this posting.
func (v *PostingMap) Get(ctx context.Context, postingID uint64) (*PostingMetadata, error) {
	slot, ok := v.getSlot(postingID)
	if !ok {
		return nil, ErrPostingNotFound
	}

	metadata := slot.metadata.Load()
	if metadata == nil {
		return nil, ErrPostingNotFound
	}

	return metadata, nil
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
		v.deleteSlot(postingID)
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
		v.setSlot(postingID, pm)
		return nil
	}

	// store the updated posting metadata on disk and update the in-memory cache
	err = v.bucket.Set(ctx, postingID, pm)
	if err != nil {
		return err
	}

	v.setSlot(postingID, pm)

	return nil
}

// FastAddVectorID adds a vector ID to the posting with the given ID
// but only updates the in-memory cache.
// The store is updated asynchronously by an analyze, split, or merge operation.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) FastAddVectorID(ctx context.Context, postingID uint64, vectorID uint64) (uint32, error) {
	v.locks.Lock(postingID)
	defer v.locks.Unlock(postingID)

	slot := v.ensureSlot(postingID)
	m := slot.metadata.Load()
	if m != nil {
		m.PackedPostingMetadata = m.AddVector(vectorID)
		count := m.Count()
		return uint32(count), nil
	}

	m = &PostingMetadata{
		PackedPostingMetadata: NewPackedPostingMetadata([]uint64{vectorID}),
	}
	count := m.Count()
	slot.metadata.Store(m)
	v.count.Add(1)
	return uint32(count), nil
}

// Restore loads all postings from disk into memory. It should be called during startup to populate the in-memory cache.
func (v *PostingMap) Restore(ctx context.Context) error {
	return v.bucket.Iter(ctx, func(u uint64, ppm PackedPostingMetadata) error {
		v.setSlot(u, ppm)
		return nil
	})
}

func (v *PostingMap) getSlot(postingID uint64) (*postingMapSlot, bool) {
	page, slot := v.data.GetPageFor(postingID)
	if page == nil {
		return nil, false
	}
	return &page[slot], true
}

func (v *PostingMap) ensureSlot(postingID uint64) *postingMapSlot {
	page, slot := v.data.EnsurePageFor(postingID)
	return &page[slot]
}

func (v *PostingMap) setSlot(postingID uint64, metadata PackedPostingMetadata) {
	v.locks.Lock(postingID)
	defer v.locks.Unlock(postingID)

	slot := v.ensureSlot(postingID)
	current := slot.metadata.Load()
	if current != nil && bytes.Equal(current.PackedPostingMetadata, metadata) {
		return
	}

	next := &PostingMetadata{
		PackedPostingMetadata: metadata,
	}
	if current == nil {
		v.count.Add(1)
	}
	slot.metadata.Store(next)
}

func (v *PostingMap) deleteSlot(postingID uint64) {
	v.locks.Lock(postingID)
	defer v.locks.Unlock(postingID)

	slot, ok := v.getSlot(postingID)
	if !ok {
		return
	}
	if slot.metadata.Swap(nil) != nil {
		v.count.Add(^uint64(0))
	}
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
	return postingMapKey(p.keyPrefix, postingID)
}

func postingMapKey(prefix []byte, postingID uint64) []byte {
	buf := make([]byte, len(prefix)+8)
	copy(buf, prefix)
	binary.LittleEndian.PutUint64(buf[len(prefix):], postingID)
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
		return nil, errors.Wrapf(err, "failed to get posting metadata for %d", postingID)
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

type postingMapMigrationEntry struct {
	key       []byte
	postingID uint64
	metadata  PackedPostingMetadata
}

func migratePostingMapV1ToV2(ctx context.Context, bucket *lsmkv.Bucket, logger logrus.FieldLogger) error {
	store := NewPostingMapStore(bucket, postingMapBucketPrefixV2)
	sizes := NewPostingSizesStore(bucket, postingSizesBucketPrefix)
	start := time.Now()
	var migrated int
	var loggedStart bool

	for {
		batch, err := legacyPostingMapBatch(ctx, bucket, postingMapMigrationBatchSize)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			if loggedStart {
				logger.WithFields(logrus.Fields{
					"action":   "hfresh_posting_map_migration",
					"migrated": migrated,
					"elapsed":  time.Since(start),
				}).Info("finished migrating HFresh posting map metadata")
			}
			return nil
		}

		if !loggedStart {
			loggedStart = true
			logger.WithField("action", "hfresh_posting_map_migration").
				Info("migrating HFresh posting map metadata")
		}

		for _, entry := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := store.Set(ctx, entry.postingID, entry.metadata); err != nil {
				return errors.Wrapf(err, "migrate posting map metadata for posting %d", entry.postingID)
			}
			if err := sizes.Set(ctx, entry.postingID, entry.metadata.Count()); err != nil {
				return errors.Wrapf(err, "migrate posting size for posting %d", entry.postingID)
			}
		}

		for _, entry := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := bucket.Delete(entry.key); err != nil {
				return errors.Wrapf(err, "delete legacy posting map metadata for posting %d", entry.postingID)
			}
		}

		migrated += len(batch)
		logger.WithFields(logrus.Fields{
			"action":   "hfresh_posting_map_migration",
			"batch":    len(batch),
			"migrated": migrated,
			"elapsed":  time.Since(start),
		}).Info("migrated HFresh posting map metadata batch")
	}
}

func legacyPostingMapBatch(ctx context.Context, bucket *lsmkv.Bucket, limit int) ([]postingMapMigrationEntry, error) {
	c := bucket.Cursor()
	defer c.Close()

	batch := make([]postingMapMigrationEntry, 0, limit)
	for k, v := c.Seek(postingMapBucketPrefixV1); len(k) > 0 && bytes.HasPrefix(k, postingMapBucketPrefixV1); k, v = c.Next() {
		if len(v) == 0 {
			continue
		}
		if len(batch)%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		postingID := binary.LittleEndian.Uint64(k[len(postingMapBucketPrefixV1):])
		key := make([]byte, len(k))
		copy(key, k)
		metadata := normalizePackedPostingMetadata(PackedPostingMetadata(v), true)
		batch = append(batch, postingMapMigrationEntry{
			key:       key,
			postingID: postingID,
			metadata:  metadata,
		})

		if len(batch) == limit {
			break
		}
	}

	return batch, ctx.Err()
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
