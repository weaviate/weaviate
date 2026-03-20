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

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueKey(t *testing.T) {
	t.Run("hash prefix followed by value", func(t *testing.T) {
		value := []byte("Berlin")
		key := ValueKey("addresses.city", value)

		require.Len(t, key, 8+len(value))
		assert.Equal(t, xxhash.Sum64String("addresses.city"), binary.BigEndian.Uint64(key[:8]))
		assert.Equal(t, value, key[8:])
	})

	t.Run("different paths same value produce different keys", func(t *testing.T) {
		k1 := ValueKey("addresses.city", []byte("Berlin"))
		k2 := ValueKey("addresses.postcode", []byte("Berlin"))
		assert.NotEqual(t, k1, k2)
	})

	t.Run("same path different values share hash prefix", func(t *testing.T) {
		k1 := ValueKey("addresses.city", []byte("Berlin"))
		k2 := ValueKey("addresses.city", []byte("Hamburg"))
		assert.Equal(t, k1[:8], k2[:8])
		assert.NotEqual(t, k1, k2)
	})
}

func TestIdxKey(t *testing.T) {
	t.Run("hash prefix followed by BE16 index", func(t *testing.T) {
		key := IdxKey("addresses", 0)

		require.Len(t, key, 10)
		assert.Equal(t, xxhash.Sum64String("_idx.addresses"), binary.BigEndian.Uint64(key[:8]))
		assert.Equal(t, uint16(0), binary.BigEndian.Uint16(key[8:]))
	})

	t.Run("same path different indices share hash prefix", func(t *testing.T) {
		k0 := IdxKey("addresses", 0)
		k1 := IdxKey("addresses", 1)
		assert.Equal(t, k0[:8], k1[:8])
		assert.Equal(t, uint16(1), binary.BigEndian.Uint16(k1[8:]))
		assert.NotEqual(t, k0, k1)
	})

	t.Run("different paths produce different hash prefixes", func(t *testing.T) {
		k1 := IdxKey("addresses", 0)
		k2 := IdxKey("cars", 0)
		assert.NotEqual(t, k1[:8], k2[:8])
	})
}

func TestExistsKey(t *testing.T) {
	t.Run("named path hashes _exists prefix with path", func(t *testing.T) {
		key := ExistsKey("owner.firstname")
		require.Len(t, key, 8)
		assert.Equal(t, xxhash.Sum64String("_exists.owner.firstname"), binary.BigEndian.Uint64(key))
	})

	t.Run("root path hashes _exists without dot", func(t *testing.T) {
		key := ExistsKey("")
		require.Len(t, key, 8)
		assert.Equal(t, xxhash.Sum64String("_exists"), binary.BigEndian.Uint64(key))
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
