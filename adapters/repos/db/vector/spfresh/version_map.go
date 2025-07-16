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

import "sync"

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
	mu sync.RWMutex

	versions map[uint64]VectorVersion
}

func (v *VersionMap) Get(vectorID uint64) VectorVersion {
	v.mu.RLock()
	defer v.mu.RUnlock()

	return v.versions[vectorID]
}

func (v *VersionMap) Increment(vectorID uint64) VectorVersion {
	v.mu.Lock()
	defer v.mu.Unlock()

	ve := v.versions[vectorID]          // zero value if unseen
	delBit := uint8(ve) & tombstoneMask // 0x00 or 0x80
	counter := uint8(ve) & counterMask  // 0-127

	if counter < 127 {
		counter++
	} else {
		counter = 0 // wraparound behavior
	}

	newVE := VectorVersion(delBit | counter)
	v.versions[vectorID] = newVE
	return newVE
}
