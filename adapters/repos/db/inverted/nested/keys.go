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

	"github.com/zeebo/xxh3"
)

const (
	// hashSize is the 96-bit (12-byte) prefix derived from xxh3-128.
	// Birthday-paradox collision probability at 10^10 distinct paths is
	// ~10^-12 — comfortable headroom beyond any realistic schema size,
	// at +4 bytes per LSM key over the previous 64-bit (xxh64) prefix.
	// Backward-incompatible on-disk format change vs the 64-bit prefix;
	// the change is safe to land while nested filtering is preview-gated
	// (no production data exists yet).
	hashSize   = 12
	IdxKeySize = hashSize + 2 // hash12 + BE16(index)
)

// hashKey is the shared primitive: allocate a buffer, write the top
// 96 bits of xxh3-128(input) as big-endian bytes, then append suffix.
func hashKey(input string, suffix []byte) []byte {
	key := make([]byte, hashSize+len(suffix))
	writeHashPrefix(key, input)
	copy(key[hashSize:], suffix)
	return key
}

// writeHashPrefix writes the 12-byte (96-bit) xxh3-128 truncated prefix
// of input into the first hashSize bytes of dst. dst must be at least
// hashSize bytes.
func writeHashPrefix(dst []byte, input string) {
	sum := xxh3.HashString128(input)
	binary.BigEndian.PutUint64(dst, sum.Hi)
	binary.BigEndian.PutUint32(dst[8:], uint32(sum.Lo>>32))
}

// PathPrefix returns the hashSize-byte hash prefix for a dot-notation
// property path. Used as keyPrefix on RowReaderRoaringSet when reading
// from a nested bucket.
func PathPrefix(path string) []byte {
	return hashKey(path, nil)
}

// ValueKey builds the key for the nested value bucket:
// hash12(path) + analyzedValue.
func ValueKey(path string, analyzedValue []byte) []byte {
	return hashKey(path, analyzedValue)
}

// IdxKey builds the key for an _idx metadata entry:
// hash12("_idx." + path) + BE16(index) for named paths,
// or hash12("_idx") + BE16(index) for the root (path == "").
// Mirrors ExistsKey's empty-path handling.
func IdxKey(path string, index int) []byte {
	return hashKey(idxKeyPrefix(path), binary.BigEndian.AppendUint16(nil, uint16(index)))
}

// IdxKeyToBuf writes an _idx key into buf and returns the populated slice.
// buf must be at least IdxKeySize bytes. Use this in loops to avoid
// per-iteration allocation; declare a [IdxKeySize]byte on the stack and pass
// a slice of it.
func IdxKeyToBuf(path string, index int, buf []byte) []byte {
	buf = buf[:IdxKeySize]
	writeHashPrefix(buf, idxKeyPrefix(path))
	binary.BigEndian.PutUint16(buf[hashSize:], uint16(index))
	return buf
}

// idxKeyPrefix returns the string that's hashed for an _idx key. For the
// root (empty path) it's just "_idx" — matching ExistsKey's convention so
// the prefix conventions are consistent across the two key families.
func idxKeyPrefix(path string) string {
	if path == "" {
		return "_idx"
	}
	return "_idx." + path
}

// ExistsKey builds the key for an _exists metadata entry:
// hash12("_exists." + path) for named paths, or hash12("_exists") for root.
func ExistsKey(path string) []byte {
	if path == "" {
		return hashKey("_exists", nil)
	}
	return hashKey("_exists."+path, nil)
}
