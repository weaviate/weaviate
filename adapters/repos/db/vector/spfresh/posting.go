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
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
)

type Vector struct {
	ID      uint64
	Version VectorVersion
	Data    []byte
}

// A Posting is a collection of vectors associated with the same centroid.
type Posting []Vector

// GarbageCollect filters out vectors that are marked as deleted in the version map
// and return a new Posting.
func (p Posting) GarbageCollect(versionMap *VersionMap) Posting {
	var filtered Posting

	for _, v := range p {
		version := versionMap.Get(v.ID)
		if version.Deleted() || version.Version() > v.Version.Version() {
			continue
		}

		filtered = append(filtered, Vector{
			ID:      v.ID,
			Version: version,
			Data:    v.Data,
		})
	}

	return filtered
}

// Dimensions returns the number of dimensions of the vectors in the posting.
// It returns 0 if the posting is empty.
func (p Posting) Dimensions() int {
	if len(p) == 0 {
		return 0
	}

	return len(p[0].Data)
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
