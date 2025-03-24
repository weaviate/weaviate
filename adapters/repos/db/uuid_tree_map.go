//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"encoding/binary"
	"fmt"
)

// UUIDTreeMap maps hash tree leaves to UUID ranges.
// It deterministically partitions the 128-bit UUID space into 2^N equal parts,
// where each leaf (numbered 0 to 2^N - 1) owns one partition.
//
// The first N bits of a UUID determine which partition (i.e., leaf node) it belongs to.
// This allows efficient synchronization of objects by UUID range — for instance, when using
// Merkle trees to compare which subsets of objects differ between distributed systems.
//
// Example: A tree height of 3 results in a Merkle tree with 2³ = 8 leaf nodes.
// As a result, 3 bits of the UUID determine the partitioning and allow us to map each UUID
// to one and only one leaf of the tree and vice versa.
//
// The result is a Merkle tree, with the UUID space divided as:
//
//   ┌────────────── Merkle Tree (Height = 3) ──────────────┐
//   │                                                      │
//   │                         Root                         │
//   │                          *                           │
//   │                         / \                          │
//   │                        *   *                         │
//   │                       / \ / \                        │
//   │                      *  * *  *                       │
//   │                 Leaf nodes ( 0 to 7 )                │
//   │                /  /  /  /  \  \  \  \                │
//   │               0  1  2  3    4  5  6  7               │
//   └──────────────────────────────────────────────────────┘
//
// Each UUID is mapped to one of these 8 leaves based on the **first 3 bits** of its value.
// Here's how the mapping works:
//
// - Leaf 0: UUIDs with first 3 bits = 000 → from 00000000-0000-... to 1FFFFFFF-FFFF-...
// - Leaf 1: UUIDs with first 3 bits = 001 → from 20000000-0000-... to 3FFFFFFF-FFFF-...
// - Leaf 2: UUIDs with first 3 bits = 010 → from 40000000-0000-... to 5FFFFFFF-FFFF-...
// - Leaf 3: UUIDs with first 3 bits = 011 → from 60000000-0000-... to 7FFFFFFF-FFFF-...
// - Leaf 4: UUIDs with first 3 bits = 100 → from 80000000-0000-... to 9FFFFFFF-FFFF-...
// - Leaf 5: UUIDs with first 3 bits = 101 → from A0000000-0000-... to BFFFFFFF-FFFF-...
// - Leaf 6: UUIDs with first 3 bits = 110 → from C0000000-0000-... to DFFFFFFF-FFFF-...
// - Leaf 7: UUIDs with first 3 bits = 111 → from E0000000-0000-... to FFFFFFFF-FFFF-...
//
// In binary (first byte):
// - Leaf 0: 0b000xxxxx... (000)
// - Leaf 1: 0b001xxxxx... (001)
// - ...
// - Leaf 7: 0b111xxxxx... (111)
//
// This allows the system to:
// - Identify which leaf (partition) a UUID belongs to
// - Compare only relevant tree branches during synchronization
// - Extract the full range of UUIDs under a specific leaf
//
// Example use case:
// Two nodes use a hash/Merkle tree to compare UUIDs they store. If the hash
// for leaf 5 differs, we know that UUIDs in the range A0000000... to BFFFFFFF...
// need to be checked or synchronized. This avoids comparing all 2^128 UUIDs.

type UUIDTreeMap struct {
	treeHeight int
}

const (
	uuidLen       = 16
	maxTreeHeight = 64
)

// UUID represents a UUID as 8 bytes (128-bit) identifier.
type UUID [uuidLen]byte

// LeafID represents an identified for the hash tree leaf
type LeafID uint64

// UUIDRange defines an inclusive range of UUIDs with a Start and End bound.
type UUIDRange struct {
	Leaf  LeafID
	Start UUID
	End   UUID
}

// NewUUIDTreeMap creates a new UUIDTreeMap with the given tree height.
// Panics if treeHeight is negative or greater than 64, since only the first 64 bits
// of a UUID are used to determine partitioning.
func NewUUIDTreeMap(treeHeight int) *UUIDTreeMap {
	if treeHeight < 0 || treeHeight > maxTreeHeight {
		panic(fmt.Sprintf("invalid tree height: %d (must be between 0 and %d)", treeHeight, maxTreeHeight))
	}
	return &UUIDTreeMap{
		treeHeight: treeHeight,
	}
}

// Range returns the UUID range (inclusive) for a given leaf node.
// The range includes all UUIDs whose first N bits (where N is the tree height)
// match the binary prefix corresponding to the given leaf index.
//
// Returns an error if the leaf index is greater than or equal to 2^N.
func (m *UUIDTreeMap) Range(leaf LeafID) (UUIDRange, error) {
	if m.treeHeight < maxTreeHeight && leaf >= (LeafID(1)<<m.treeHeight) {
		return UUIDRange{}, fmt.Errorf("leaf index out of range: %d (max: %d)", leaf, (uint64(1)<<m.treeHeight)-1)
	}
	// When treeHeight == 64, leaf is uint64 we do not shift by `treeHeight` to avoid getting a 0
	start := m.rangeStart(leaf)
	end := m.rangeEnd(leaf)

	return UUIDRange{
		Leaf:  leaf,
		Start: start,
		End:   end,
	}, nil
}

func (m *UUIDTreeMap) rangeStart(leaf LeafID) UUID {
	var uuidBytes [uuidLen]byte
	binary.BigEndian.PutUint64(uuidBytes[:], m.prefix(leaf))
	return uuidBytes
}

func (m *UUIDTreeMap) rangeEnd(leaf LeafID) UUID {
	var uuidBytes [uuidLen]byte
	prefix := m.prefix(leaf)
	suffix := (uint64(1) << (maxTreeHeight - m.treeHeight)) - 1
	binary.BigEndian.PutUint64(uuidBytes[:], prefix|suffix)

	// A UUID is 16 bytes (128 bits) while we only set 8 bytes (64 bits) above.
	// Here we fill the other remaining 64 bits so the upper bound has all trailing
	// bits set to 1 (8 bytes set to 0xFF).
	for i := 8; i < uuidLen; i++ {
		uuidBytes[i] = 0xFF
	}

	return uuidBytes
}

func (m *UUIDTreeMap) LeafID(uuid UUID) LeafID {
	prefix := binary.BigEndian.Uint64(uuid[:8])
	return LeafID(prefix >> (maxTreeHeight - m.treeHeight))
}

func (m *UUIDTreeMap) prefix(leaf LeafID) uint64 {
	return leaf.uint64() << (maxTreeHeight - m.treeHeight)
}

func (id LeafID) uint64() uint64 {
	return uint64(id)
}

func compareUUID(a, b UUID) int {
	for i := 0; i < uuidLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// Contains returns true if the given UUID u is within the range r (inclusive).
func (r UUIDRange) Contains(u UUID) bool {
	return compareUUID(u, r.Start) >= 0 && compareUUID(u, r.End) <= 0
}

// String returns a human-readable representation of the UUID range
// in the format "(leaf: <leaf>, start: <start>, end: <end>)".
func (r UUIDRange) String() string {
	return fmt.Sprintf("(leaf: %v, start: %v, end: %v)", r.Leaf, r.Start, r.End)
}

// String returns a LeafID string representation
func (id LeafID) String() string {
	return fmt.Sprintf("%d", id.uint64())
}

// String returns a standard UUID string representation of the 128-bit UUID.
func (u UUID) String() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(u[0:4]),
		binary.BigEndian.Uint16(u[4:6]),
		binary.BigEndian.Uint16(u[6:8]),
		binary.BigEndian.Uint16(u[8:10]),
		u[10:],
	)
}
