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
	"time"

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
	metrics    *Metrics
	data       *xsync.Map[uint64, *PostingMetadata]
	bucket     *PostingMapStore
	sizeMetric *oncePer
}

func NewPostingMap(bucket *lsmkv.Bucket, metrics *Metrics) *PostingMap {
	b := NewPostingMapStore(bucket, postingMapBucketPrefix)

	return &PostingMap{
		data:       xsync.NewMap[uint64, *PostingMetadata](),
		metrics:    metrics,
		bucket:     b,
		sizeMetric: OncePer(5 * time.Second),
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

// CountAllVectors returns the total number of vector IDs across all postings.
// It deduplicates vector IDs across postings, so the count is an approximation of the total number of unique vectors in the index.
// This is used for metrics and does not need to be exact, so it iterates over the in-memory cache without locking.
func (v *PostingMap) CountAllVectors(ctx context.Context) (uint64, error) {
	vectorIDSet := make(map[uint64]struct{})

	for _, m := range v.data.AllRelaxed() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		m.RLock()
		for id, version := range m.Iter() {
			if version.Deleted() {
				continue
			}
			vectorIDSet[id] = struct{}{}
		}
		m.RUnlock()
	}

	return uint64(len(vectorIDSet)), nil
}

// SetVectorIDs sets the vector IDs for the posting with the given ID in-memory and persists them to disk.
// It assumes the posting has been locked for writing by the caller.
// It is safe to read the cache concurrently.
func (v *PostingMap) SetVectorIDs(ctx context.Context, postingID uint64, posting Posting) error {
	defer v.setSizeMetricIfDue(ctx)

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

	count := pm.Count()

	v.data.Store(postingID, &PostingMetadata{PackedPostingMetadata: pm})
	v.metrics.ObservePostingSize(float64(count))

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

	var count uint32
	if m != nil {
		m.Lock()
		m.PackedPostingMetadata = m.AddVector(vectorID, version)
		count = m.Count()
		m.Unlock()
	} else {
		m = &PostingMetadata{
			PackedPostingMetadata: NewPackedPostingMetadata([]uint64{vectorID}, []VectorVersion{version}),
		}
		count = m.Count()
		v.data.Store(postingID, m)
	}

	v.metrics.ObservePostingSize(float64(count))
	return uint32(count), nil
}

// Restore loads all postings from disk into memory. It should be called during startup to populate the in-memory cache.
func (v *PostingMap) Restore(ctx context.Context) error {
	defer v.setSizeMetricIfDue(ctx)

	return v.bucket.Iter(ctx, func(u uint64, ppm PackedPostingMetadata) error {
		v.data.Store(u, &PostingMetadata{PackedPostingMetadata: ppm})
		return nil
	})
}

// setSizeMetricIfDue updates the size metric if the next update is due.
// It is called after any operation that modifies the postings to ensure the metric is reasonably up-to-date without causing too much overhead.
func (v *PostingMap) setSizeMetricIfDue(ctx context.Context) {
	var err error
	v.sizeMetric.do(func() {
		var count uint64
		count, err = v.CountAllVectors(ctx)
		if err != nil {
			return
		}

		v.metrics.SetSize(int(count))
	})
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
		p = bufferPool.Get(headerSize, headerSize+currentScheme.BytesPerValue()+1)
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
		newSize := headerSize + int(newCount)*(currentBytesPerValue+1)
		if cap(p) < newSize {
			newP := bufferPool.Get(len(p), newSize)
			copy(newP, p)
			bufferPool.Put(p)
			p = newP
		}

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
	newSize := headerSize + idsSize + versionsSize

	newData := bufferPool.Get(newSize, newSize)
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

	bufferPool.Put(p)

	return newData
}

// Set adds or replaces the vector IDs for the given posting ID.
// Disk format:
//   - 1 byte: scheme for vector IDs
//   - 4 bytes: count (uint32, little endian)
//   - count * (bytesPerScheme + 1): vector IDs and version
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
	prefix := []byte{p.keyPrefix}
	for k, v := c.Seek(prefix); len(k) > 0 && k[0] == p.keyPrefix; k, v = c.Next() {
		i++
		if len(v) == 0 {
			continue
		}

		if i%1000 == 0 && ctx.Err() != nil {
			return ctx.Err()
		}

		postingID := binary.LittleEndian.Uint64(k[1:])
		metadata := bufferPool.Get(len(v), len(v))
		copy(metadata, v)
		err := fn(postingID, metadata)
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

type oncePer struct {
	d    time.Duration
	t    *time.Ticker
	mu   sync.Mutex
	once sync.Once
}

func OncePer(d time.Duration) *oncePer {
	return &oncePer{
		d: d,
	}
}

func (o *oncePer) do(f func()) {
	o.once.Do(func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		f()
		o.t = time.NewTicker(o.d)
	})

	select {
	case <-o.t.C:
		if o.mu.TryLock() {
			defer o.mu.Unlock()
			f()
		}
	default:
	}
}
