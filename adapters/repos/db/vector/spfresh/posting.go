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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
)

type Vector []byte

func NewVector(id uint64, version VectorVersion, data []byte) Vector {
	v := make(Vector, 8+1+len(data))
	binary.LittleEndian.PutUint64(v[:8], id)
	v[8] = byte(version)
	copy(v[9:], data)

	return v
}

func (v Vector) ID() uint64 {
	return binary.LittleEndian.Uint64(v[:8])
}

func (v Vector) Version() VectorVersion {
	return VectorVersion(v[8])
}

func (v Vector) Data() []byte {
	return v[8+1:]
}

// A Posting is a collection of vectors associated with the same centroid.
type Posting struct {
	// total size in bytes of each vector
	vectorSize int
	data       []byte
}

func (p *Posting) AddVector(v Vector) {
	p.data = append(p.data, v...)
}

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return the filtered posting.
// This method doesn't allocate a new slice, the filtering is done in-place.
func (p *Posting) GarbageCollect(versionMap *VersionMap) *Posting {
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

func (p *Posting) Len() int {
	step := int(8 + 1 + p.vectorSize)
	var j int
	for i := 0; i < len(p.data); i += step {
		j++
	}

	return j
}

func (p *Posting) Iter() iter.Seq2[int, Vector] {
	step := 8 + 1 + p.vectorSize
	return func(yield func(int, Vector) bool) {
		var j int
		for i := 0; i < len(p.data); i += step {
			if !yield(j, p.data[i:i+step]) {
				break
			}
			j++
		}
	}
}

func (p *Posting) GetAt(i int) Vector {
	step := int(8 + 1 + p.vectorSize)
	idx := i * step
	return p.data[idx : idx+step]
}

// A VectorVersion is a 1-byte value structured as follows:
// - 7 bits for the version number
// - 1 bit for the tombstone flag (0 = alive, 1 = deleted)
type VectorVersion uint8

func (ve VectorVersion) Version() uint8 {
	return uint8(ve) & counterMask
}

func (ve VectorVersion) Deleted() bool {
	return (uint8(ve) & tombstoneMask) != 0
}

// VersionMap maps vector IDs to their latest version number.
type VersionMap struct {
	locks    *common.ShardedRWLocks
	versions *common.PagedArray[VectorVersion]
}

func NewVersionMap(pages, pageSize uint64) *VersionMap {
	return &VersionMap{
		locks:    common.NewShardedRWLocks(512),
		versions: common.NewPagedArray[VectorVersion](pages, pageSize),
	}
}

func (v *VersionMap) Get(id uint64) VectorVersion {
	v.locks.RLock(id)
	ve := v.versions.Get(id)
	v.locks.RUnlock(id)
	return ve
}

func (v *VersionMap) Increment(id uint64) (VectorVersion, bool) {
	v.locks.Lock(id)
	defer v.locks.Unlock(id)

	ve := v.versions.Get(id)
	if ve.Deleted() {
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
