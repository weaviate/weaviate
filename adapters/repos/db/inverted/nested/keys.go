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

// ValueKey builds the key for the nested value bucket:
// hash8(path) + analyzedValue.
func ValueKey(path string, analyzedValue []byte) []byte {
	key := make([]byte, hashSize+len(analyzedValue))
	binary.BigEndian.PutUint64(key, xxhash.Sum64String(path))
	copy(key[hashSize:], analyzedValue)
	return key
}

// IdxKey builds the key for an _idx metadata entry:
// hash8("_idx." + path) + BE16(index).
func IdxKey(path string, index int) []byte {
	key := make([]byte, hashSize+2)
	binary.BigEndian.PutUint64(key, xxhash.Sum64String("_idx."+path))
	binary.BigEndian.PutUint16(key[hashSize:], uint16(index))
	return key
}

// ExistsKey builds the key for an _exists metadata entry:
// hash8("_exists." + path) for named paths, or hash8("_exists") for root.
func ExistsKey(path string) []byte {
	key := make([]byte, hashSize)
	if path == "" {
		binary.BigEndian.PutUint64(key, xxhash.Sum64String("_exists"))
	} else {
		binary.BigEndian.PutUint64(key, xxhash.Sum64String("_exists."+path))
	}
	return key
}
