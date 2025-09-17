//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"encoding/binary"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
)

type Vector interface {
	ID() uint64
	Version() VectorVersion
	Distance(distancer *Distancer, other Vector) (float32, error)
	DistanceWithRaw(distancer *Distancer, other []byte) (float32, error)
}

var _ Vector = CompressedVector(nil)

// A compressed vector is structured as follows:
// - 8 bytes for the vector ID (uint64, little endian)
// - 1 byte for the version (VectorVersion)
// - N bytes for the compressed vector data
type CompressedVector []byte

func NewCompressedVector(id uint64, version VectorVersion, data []byte) CompressedVector {
	v := make(CompressedVector, 8+1+len(data))
	binary.LittleEndian.PutUint64(v[:8], id)
	v[8] = byte(version)
	copy(v[9:], data)

	return v
}

// Used for creating a vector without an ID and version, e.g. for queries
func NewAnonymousCompressedVector(data []byte) CompressedVector {
	v := make(CompressedVector, 8+1+len(data))
	// id and version are zero
	copy(v[9:], data)

	return v
}

func (v CompressedVector) ID() uint64 {
	return binary.LittleEndian.Uint64(v[:8])
}

func (v CompressedVector) Version() VectorVersion {
	return VectorVersion(v[8])
}

func (v CompressedVector) Data() []byte {
	return v[8+1:]
}

func (v CompressedVector) Distance(distancer *Distancer, other Vector) (float32, error) {
	c, ok := other.(CompressedVector)
	if !ok {
		return 0, errors.New("other vector is not a CompressedVector")
	}

	return distancer.DistanceBetweenCompressedVectors(v.Data(), c.Data())
}

func (v CompressedVector) DistanceWithRaw(distancer *Distancer, other []byte) (float32, error) {
	return distancer.DistanceBetweenCompressedVectors(v.Data(), other)
}

type Posting interface {
	AddVector(v Vector)
	GarbageCollect(versionMap *VersionMap) Posting
	Len() int
	Iter() iter.Seq2[int, Vector]
	GetAt(i int) Vector
	Clone() Posting
}

var _ Posting = (*CompressedPosting)(nil)

// A Posting is a collection of vectors associated with the same centroid.
type CompressedPosting struct {
	// total size in bytes of each vector
	vectorSize int
	data       []byte
}

func (p *CompressedPosting) AddVector(v Vector) {
	c := v.(CompressedVector)
	p.data = append(p.data, c...)
}

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return the filtered posting.
// This method doesn't allocate a new slice, the filtering is done in-place.
func (p *CompressedPosting) GarbageCollect(versionMap *VersionMap) Posting {
	var i int
	step := 8 + 1 + p.vectorSize
	for i < len(p.data) {
		id := binary.LittleEndian.Uint64(p.data[i : i+8])
		version := versionMap.Get(id)
		if !version.Deleted() && version.Version() <= p.data[i+8] {
			i += step
			continue
		}

		// shift the data to the left
		copy(p.data[i:], p.data[i+step:])
		p.data = p.data[:len(p.data)-int(step)]
	}

	return p
}

func (p *CompressedPosting) Len() int {
	step := int(8 + 1 + p.vectorSize)
	var j int
	for i := 0; i < len(p.data); i += step {
		j++
	}

	return j
}

func (p *CompressedPosting) Iter() iter.Seq2[int, Vector] {
	step := 8 + 1 + p.vectorSize
	return func(yield func(int, Vector) bool) {
		var j int
		for i := 0; i < len(p.data); i += step {
			if !yield(j, CompressedVector(p.data[i:i+step])) {
				break
			}
			j++
		}
	}
}

func (p *CompressedPosting) GetAt(i int) Vector {
	step := int(8 + 1 + p.vectorSize)
	idx := i * step
	return CompressedVector(p.data[idx : idx+step])
}

func (p *CompressedPosting) Clone() Posting {
	return &CompressedPosting{
		vectorSize: p.vectorSize,
		data:       append([]byte(nil), p.data...),
	}
}

func (p *CompressedPosting) Uncompress(quantizer *compressionhelpers.RotationalQuantizer) [][]float32 {
	step := 8 + 1 + p.vectorSize
	l := p.Len()
	data := make([][]float32, 0, l)
	for i := 0; i < len(p.data); i += step {
		data = append(data, quantizer.Restore(p.data[i+9:i+step]))
	}

	return data
}

// A VectorVersion is a 1-byte value structured as follows:
// - 7 bits for the version number
// - 1 bit for the tombstone flag (0 = alive, 1 = deleted)
// TODO: versions can wrap around after 127 updates,
// we need a mechanism to handle this in the future (e.g. during snapshots perhaps, etc.)
type VectorVersion uint8

func (ve VectorVersion) Version() uint8 {
	return uint8(ve) & counterMask
}

func (ve VectorVersion) Deleted() bool {
	return (uint8(ve) & tombstoneMask) != 0
}

// VersionMap maps vector IDs to their latest version number.
// Because versions are stored as a single byte, we cannot use atomic operations
// to update them. Instead, we use a sharded locks to ensure that updates
// to the same ID are serialized.
type VersionMap struct {
	locks    *common.ShardedRWLocks
	versions *common.PagedArray[VectorVersion]
}

func NewVersionMap(pages, pageSize uint64) *VersionMap {
	// keep the number of mutexes reasonable by reducing it to the nearest
	// power of two <= 512
	locks := pages
	for locks > 512 {
		locks = locks >> 1
	}

	return &VersionMap{
		locks:    common.NewShardedRWLocksWith(locks, pageSize),
		versions: common.NewPagedArray[VectorVersion](pages, pageSize),
	}
}

func (v *VersionMap) Get(id uint64) VectorVersion {
	v.locks.RLock(id)
	ve := v.versions.Get(id)
	v.locks.RUnlock(id)
	return ve
}

// Delete removes the version entry for the given ID.
// Used when an insert fails and the vector was not added anywhere.
func (v *VersionMap) Delete(id uint64) {
	v.locks.Lock(id)
	v.versions.Delete(id)
	v.locks.Unlock(id)
}

func (v *VersionMap) Increment(previousVersion VectorVersion, id uint64) (VectorVersion, bool) {
	v.locks.Lock(id)
	defer v.locks.Unlock(id)

	ve := v.versions.Get(id)
	if ve.Deleted() || ve != previousVersion {
		return 0, false
	}

	delBit := uint8(ve) & tombstoneMask // 0x00 or 0x80
	counter := uint8(ve) & counterMask  // 0-127

	if counter < 127 {
		counter++
	} else {
		counter = 0 // wraparound behavior
	}

	newVE := VectorVersion(delBit | counter)
	v.versions.Set(id, newVE)

	return newVE, true
}

func (v *VersionMap) MarkDeleted(id uint64) VectorVersion {
	v.locks.Lock(id)
	defer v.locks.Unlock(id)

	ve := v.versions.Get(id)
	if ve == 0 {
		return 0
	}
	if ve.Deleted() {
		return ve // already deleted
	}

	counter := uint8(ve) & counterMask // 0-127

	newVE := VectorVersion(tombstoneMask | counter)
	v.versions.Set(id, newVE)
	return newVE
}

func (v *VersionMap) IsDeleted(id uint64) bool {
	v.locks.RLock(id)
	ve := v.versions.Get(id)
	v.locks.RUnlock(id)
	return ve.Deleted()
}

// AllocPageFor ensures that the version map has a page allocated for the given ID.
func (v *VersionMap) AllocPageFor(id uint64) {
	v.locks.Lock(id)
	v.versions.AllocPageFor(id)
	v.locks.Unlock(id)
}

// PostingSizes keeps track of the number of vectors in each posting.
type PostingSizes struct {
	m     sync.RWMutex // Ensures pages are allocated atomically
	sizes *common.PagedArray[uint32]
}

func NewPostingSizes(pages, pageSize uint64) *PostingSizes {
	return &PostingSizes{
		sizes: common.NewPagedArray[uint32](pages, pageSize),
	}
}

func (v *PostingSizes) Get(postingID uint64) uint32 {
	v.m.RLock()
	page, slot := v.sizes.GetPageFor(postingID)
	v.m.RUnlock()
	return atomic.LoadUint32(&page[slot])
}

func (v *PostingSizes) Set(postingID uint64, newSize uint32) {
	v.m.RLock()
	page, slot := v.sizes.GetPageFor(postingID)
	v.m.RUnlock()
	atomic.StoreUint32(&page[slot], newSize)
}

func (v *PostingSizes) Inc(postingID uint64, delta uint32) uint32 {
	v.m.RLock()
	page, slot := v.sizes.GetPageFor(postingID)
	v.m.RUnlock()
	return atomic.AddUint32(&page[slot], delta)
}

// AllocPageFor ensures the array has a page allocated for the given IDs.
func (v *PostingSizes) AllocPageFor(id ...uint64) {
	v.m.Lock()
	for _, id := range id {
		v.sizes.AllocPageFor(id)
	}
	v.m.Unlock()
}

type Distancer struct {
	quantizer *compressionhelpers.RotationalQuantizer
	distancer distancer.Provider
}

func (d *Distancer) DistanceBetweenCompressedVectors(a, b []byte) (float32, error) {
	return d.quantizer.DistanceBetweenCompressedVectors(a, b)
}

func (d *Distancer) DistanceBetweenVectors(a, b []float32) (float32, error) {
	return d.distancer.SingleDist(a, b)
}
