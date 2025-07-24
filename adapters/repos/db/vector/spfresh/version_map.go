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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	counterMask   = 0x7F // 0111 1111, masks out the lower 7 bits
	tombstoneMask = 0x80 // 1000 0000, masks out the highest bit
)

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

func (v *VersionMap) Get(vectorID uint64) VectorVersion {
	v.locks.RLock(vectorID)
	ve, _ := v.versions.Get(vectorID)
	v.locks.RUnlock(vectorID)
	return ve
}

func (v *VersionMap) Increment(vectorID uint64) (VectorVersion, bool) {
	v.locks.Lock(vectorID)
	defer v.locks.Unlock(vectorID)

	ve, _ := v.versions.Get(vectorID)
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
	v.versions.Set(vectorID, newVE)

	return newVE, true
}

func (v *VersionMap) MarkDeleted(vectorID uint64) VectorVersion {
	v.locks.Lock(vectorID)
	defer v.locks.Unlock(vectorID)

	ve, _ := v.versions.Get(vectorID)
	if ve == 0 {
		return 0
	}
	if ve.Deleted() {
		return ve // already deleted
	}

	counter := uint8(ve) & counterMask // 0-127

	newVE := VectorVersion(tombstoneMask | counter)
	v.versions.Set(vectorID, newVE)
	return newVE
}

// AllocPageFor ensures that the version map has a page allocated for the given ID.
// This call is not thread-safe and should be protected by the caller.
func (v *VersionMap) AllocPageFor(id uint64) {
	v.versions.AllocPageFor(id)
}
