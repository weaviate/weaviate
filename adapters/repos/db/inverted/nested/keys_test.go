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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wantHashPrefix mirrors writeHashPrefix and returns the 12-byte expected
// hash prefix for a given string. Used in the asserts below so the layout
// detail (xxh3-128 truncated to 96 bits) stays in one place — bumping
// hashSize again only requires updating writeHashPrefix and this helper.
func wantHashPrefix(s string) []byte {
	dst := make([]byte, hashSize)
	writeHashPrefix(dst, s)
	return dst
}

func TestPathPrefix(t *testing.T) {
	t.Run("returns hashSize bytes of hash of path", func(t *testing.T) {
		prefix := PathPrefix("addresses.city")
		require.Len(t, prefix, hashSize)
		assert.Equal(t, wantHashPrefix("addresses.city"), prefix)
	})

	t.Run("different paths produce different prefixes", func(t *testing.T) {
		assert.NotEqual(t, PathPrefix("addresses.city"), PathPrefix("addresses.postcode"))
	})

	t.Run("matches first hashSize bytes of ValueKey for same path", func(t *testing.T) {
		path := "addresses.city"
		assert.Equal(t, PathPrefix(path), ValueKey(path, []byte("Berlin"))[:hashSize])
	})
}

func TestValueKey(t *testing.T) {
	t.Run("hash prefix followed by value", func(t *testing.T) {
		value := []byte("Berlin")
		key := ValueKey("addresses.city", value)

		require.Len(t, key, hashSize+len(value))
		assert.Equal(t, wantHashPrefix("addresses.city"), key[:hashSize])
		assert.Equal(t, value, key[hashSize:])
	})

	t.Run("different paths same value produce different keys", func(t *testing.T) {
		k1 := ValueKey("addresses.city", []byte("Berlin"))
		k2 := ValueKey("addresses.postcode", []byte("Berlin"))
		assert.NotEqual(t, k1, k2)
	})

	t.Run("same path different values share hash prefix", func(t *testing.T) {
		k1 := ValueKey("addresses.city", []byte("Berlin"))
		k2 := ValueKey("addresses.city", []byte("Hamburg"))
		assert.Equal(t, k1[:hashSize], k2[:hashSize])
		assert.NotEqual(t, k1, k2)
	})
}

func TestIdxKey(t *testing.T) {
	t.Run("hash prefix followed by BE16 index", func(t *testing.T) {
		key := IdxKey("addresses", 0)

		require.Len(t, key, IdxKeySize)
		assert.Equal(t, wantHashPrefix("_idx.addresses"), key[:hashSize])
		assert.Equal(t, uint16(0), binary.BigEndian.Uint16(key[hashSize:]))
	})

	t.Run("same path different indices share hash prefix", func(t *testing.T) {
		k0 := IdxKey("addresses", 0)
		k1 := IdxKey("addresses", 1)
		assert.Equal(t, k0[:hashSize], k1[:hashSize])
		assert.Equal(t, uint16(1), binary.BigEndian.Uint16(k1[hashSize:]))
		assert.NotEqual(t, k0, k1)
	})

	t.Run("different paths produce different hash prefixes", func(t *testing.T) {
		k1 := IdxKey("addresses", 0)
		k2 := IdxKey("cars", 0)
		assert.NotEqual(t, k1[:hashSize], k2[:hashSize])
	})

	t.Run("root path hashes _idx without dot", func(t *testing.T) {
		key := IdxKey("", 0)
		require.Len(t, key, IdxKeySize)
		assert.Equal(t, wantHashPrefix("_idx"), key[:hashSize])
		assert.Equal(t, uint16(0), binary.BigEndian.Uint16(key[hashSize:]))
	})

	t.Run("root differs from named paths", func(t *testing.T) {
		assert.NotEqual(t, IdxKey("", 0)[:hashSize], IdxKey("addresses", 0)[:hashSize])
	})
}

func TestIdxKeyToBuf(t *testing.T) {
	t.Run("produces same result as IdxKey", func(t *testing.T) {
		var buf [IdxKeySize]byte
		assert.Equal(t, IdxKey("addresses", 0), IdxKeyToBuf("addresses", 0, buf[:]))
		assert.Equal(t, IdxKey("addresses", 3), IdxKeyToBuf("addresses", 3, buf[:]))
		assert.Equal(t, IdxKey("cars", 7), IdxKeyToBuf("cars", 7, buf[:]))
	})

	t.Run("buffer is reused across calls", func(t *testing.T) {
		var buf [IdxKeySize]byte
		k0 := IdxKeyToBuf("addresses", 0, buf[:])
		k1 := IdxKeyToBuf("addresses", 1, buf[:])
		// Both slices point into the same backing array — k0 reflects the last write.
		assert.Equal(t, k0, k1)
	})

	t.Run("result length is always IdxKeySize", func(t *testing.T) {
		var buf [IdxKeySize]byte
		assert.Len(t, IdxKeyToBuf("some.nested.path", 42, buf[:]), IdxKeySize)
	})

	t.Run("root path produces same result as IdxKey root", func(t *testing.T) {
		var buf [IdxKeySize]byte
		assert.Equal(t, IdxKey("", 0), IdxKeyToBuf("", 0, buf[:]))
		assert.Equal(t, IdxKey("", 7), IdxKeyToBuf("", 7, buf[:]))
	})
}

func TestIdxKeyPrefix(t *testing.T) {
	t.Run("matches first hashSize bytes of IdxKey for named path", func(t *testing.T) {
		path := "addresses"
		assert.Equal(t, IdxKeyPrefix(path), IdxKey(path, 0)[:hashSize])
		assert.Equal(t, IdxKeyPrefix(path), IdxKey(path, 42)[:hashSize])
	})

	t.Run("matches first hashSize bytes of IdxKey for root path", func(t *testing.T) {
		assert.Equal(t, IdxKeyPrefix(""), IdxKey("", 0)[:hashSize])
		assert.Equal(t, IdxKeyPrefix(""), IdxKey("", 7)[:hashSize])
	})

	t.Run("root path hashes _idx without dot (aligned with IdxKey)", func(t *testing.T) {
		assert.Equal(t, wantHashPrefix("_idx"), IdxKeyPrefix(""))
	})

	t.Run("named path hashes _idx-prefixed path", func(t *testing.T) {
		assert.Equal(t, wantHashPrefix("_idx.cars"), IdxKeyPrefix("cars"))
	})

	t.Run("root differs from named paths", func(t *testing.T) {
		assert.NotEqual(t, IdxKeyPrefix(""), IdxKeyPrefix("cars"))
	})
}

func TestExistsKey(t *testing.T) {
	t.Run("named path hashes _exists prefix with path", func(t *testing.T) {
		key := ExistsKey("owner.firstname")
		require.Len(t, key, hashSize)
		assert.Equal(t, wantHashPrefix("_exists.owner.firstname"), key)
	})

	t.Run("root path hashes _exists without dot", func(t *testing.T) {
		key := ExistsKey("")
		require.Len(t, key, hashSize)
		assert.Equal(t, wantHashPrefix("_exists"), key)
	})

	t.Run("different paths produce different keys", func(t *testing.T) {
		k1 := ExistsKey("owner.firstname")
		k2 := ExistsKey("owner.lastname")
		assert.NotEqual(t, k1, k2)
	})

	t.Run("root differs from named paths", func(t *testing.T) {
		assert.NotEqual(t, ExistsKey(""), ExistsKey("name"))
	})
}

func TestAnchorKey(t *testing.T) {
	t.Run("named path hashes _anchor prefix with path", func(t *testing.T) {
		key := AnchorKey("cars.tires")
		require.Len(t, key, hashSize)
		assert.Equal(t, wantHashPrefix("_anchor.cars.tires"), key)
	})

	t.Run("root path hashes _anchor without dot", func(t *testing.T) {
		key := AnchorKey("")
		require.Len(t, key, hashSize)
		assert.Equal(t, wantHashPrefix("_anchor"), key)
	})

	t.Run("different paths produce different keys", func(t *testing.T) {
		k1 := AnchorKey("cars")
		k2 := AnchorKey("cars.tires")
		assert.NotEqual(t, k1, k2)
	})

	t.Run("root differs from named paths", func(t *testing.T) {
		assert.NotEqual(t, AnchorKey(""), AnchorKey("cars"))
	})
}

// TestKeyFamiliesAreDistinct guards the shared-bucket layout: _idx, _exists,
// and _anchor all live in the same nested metadata bucket and rely on their
// hash-prefix family ("_idx.X" vs "_exists.X" vs "_anchor.X") to keep keys
// disjoint. A regression here would cause silent overwrites.
func TestKeyFamiliesAreDistinct(t *testing.T) {
	const path = "cars.tires"
	idx := IdxKey(path, 0)
	exists := ExistsKey(path)
	anchor := AnchorKey(path)

	// _idx is hashSize+2; _exists and _anchor are hashSize. Their hash
	// prefixes must all differ for the same path.
	assert.NotEqual(t, idx[:hashSize], exists)
	assert.NotEqual(t, idx[:hashSize], anchor)
	assert.NotEqual(t, exists, anchor)

	// Same check at the root path.
	assert.NotEqual(t, IdxKey("", 0)[:hashSize], ExistsKey(""))
	assert.NotEqual(t, IdxKey("", 0)[:hashSize], AnchorKey(""))
	assert.NotEqual(t, ExistsKey(""), AnchorKey(""))
}
