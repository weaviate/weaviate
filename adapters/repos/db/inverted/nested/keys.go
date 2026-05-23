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

package nested

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

const hashSize = 8 // xxHash64 produces 8 bytes

// hashKey is the shared primitive: allocate a buffer, write
// xxhash.Sum64String(input) as 8 big-endian bytes, then append suffix.
func hashKey(input string, suffix []byte) []byte {
	key := make([]byte, hashSize+len(suffix))
	binary.BigEndian.PutUint64(key, xxhash.Sum64String(input))
	copy(key[hashSize:], suffix)
	return key
}

// PathPrefix returns the 8-byte hash prefix for a dot-notation property path.
// Used as keyPrefix on RowReaderRoaringSet when reading from a nested bucket.
func PathPrefix(path string) []byte {
	return hashKey(path, nil)
}

// ValueKey builds the key for the nested value bucket:
// hash8(path) + analyzedValue.
func ValueKey(path string, analyzedValue []byte) []byte {
	return hashKey(path, analyzedValue)
}

// IdxKey builds the key for an _idx metadata entry:
// hash8("_idx." + path) + BE16(index).
func IdxKey(path string, index int) []byte {
	return hashKey("_idx."+path, binary.BigEndian.AppendUint16(nil, uint16(index)))
}

// ExistsKey builds the key for an _exists metadata entry:
// hash8("_exists." + path) for named paths, or hash8("_exists") for root.
func ExistsKey(path string) []byte {
	if path == "" {
		return hashKey("_exists", nil)
	}
	return hashKey("_exists."+path, nil)
}
