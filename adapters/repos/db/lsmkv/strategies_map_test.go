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

package lsmkv

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapEncoderDecoderJourney(t *testing.T) {
	// this test first encodes the map pairs, then decodes them and replace
	// duplicates, remove tombstones, etc.
	type test struct {
		name string
		in   []MapPair
		out  []MapPair
	}

	tests := []test{
		{
			name: "single pair",
			in: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
			out: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
			},
		},
		{
			name: "single pair, updated value",
			in: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
				{
					Key:   []byte("foo"),
					Value: []byte("bar2"),
				},
			},
			out: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar2"),
				},
			},
		},
		{
			name: "single pair, tombstone added",
			in: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
				{
					Key:       []byte("foo"),
					Tombstone: true,
				},
			},
			out: []MapPair{},
		},
		{
			name: "single pair, tombstone added, same value added again",
			in: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar"),
				},
				{
					Key:       []byte("foo"),
					Tombstone: true,
				},
				{
					Key:   []byte("foo"),
					Value: []byte("bar2"),
				},
			},
			out: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("bar2"),
				},
			},
		},
		{
			name: "multiple values, combination of updates and tombstones",
			in: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("never-updated"),
				},
				{
					Key:   []byte("foo1"),
					Value: []byte("bar1"),
				},
				{
					Key:   []byte("foo2"),
					Value: []byte("bar2"),
				},
				{
					Key:   []byte("foo2"),
					Value: []byte("bar2.2"),
				},
				{
					Key:       []byte("foo1"),
					Tombstone: true,
				},
				{
					Key:   []byte("foo2"),
					Value: []byte("bar2.3"),
				},
				{
					Key:   []byte("foo1"),
					Value: []byte("bar1.2"),
				},
			},
			out: []MapPair{
				{
					Key:   []byte("foo"),
					Value: []byte("never-updated"),
				},
				{
					Key:   []byte("foo1"),
					Value: []byte("bar1.2"),
				},
				{
					Key:   []byte("foo2"),
					Value: []byte("bar2.3"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded := make([]value, len(test.in))
			for i, kv := range test.in {
				enc, err := newMapEncoder().Do(kv)
				require.Nil(t, err)
				encoded[i] = enc[0]
			}
			res, err := newMapDecoder().Do(encoded, false)
			require.Nil(t, err)
			// NOTE: we are accepting that the order can be lost on updates
			assert.ElementsMatch(t, test.out, res)
		})
	}
}

func TestDecoderRemoveTombstones(t *testing.T) {
	t.Run("single entry, no tombstones", func(t *testing.T) {
		m := newMapDecoder()
		input := mustEncode([]MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone", func(t *testing.T) {
		m := newMapDecoder()
		input := mustEncode([]MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
			{
				Key:       []byte("hello"),
				Tombstone: true,
			},
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []MapPair{}
		assert.Equal(t, expected, actual)
	})

	t.Run("single entry, single tombstone, then read", func(t *testing.T) {
		m := newMapDecoder()
		input := mustEncode([]MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
			{
				Key:       []byte("hello"),
				Tombstone: true,
			},
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("three entries, two tombstones at the end", func(t *testing.T) {
		m := newMapDecoder()
		input := mustEncode([]MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("bonjour"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("guten tag"),
				Value: []byte("world"),
			},
			{
				Key:       []byte("hello"),
				Tombstone: true,
			},
			{
				Key:       []byte("bonjour"),
				Tombstone: true,
			},
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("guten tag"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})

	t.Run("three entries, two tombstones at the end, then recreate the first", func(t *testing.T) {
		m := newMapDecoder()
		input := mustEncode([]MapPair{
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("bonjour"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("guten tag"),
				Value: []byte("world"),
			},
			{
				Key:       []byte("hello"),
				Tombstone: true,
			},
			{
				Key:       []byte("bonjour"),
				Tombstone: true,
			},
			{
				Key:   []byte("bonjour"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		})

		actual, err := m.doSimplified(input)
		require.Nil(t, err)

		expected := []MapPair{
			{
				Key:   []byte("guten tag"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("bonjour"),
				Value: []byte("world"),
			},
			{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		}
		assert.Equal(t, expected, actual)
	})
}

func mustEncode(kvs []MapPair) []value {
	res, err := newMapEncoder().DoMulti(kvs)
	if err != nil {
		panic(err)
	}

	return res
}

func Test_MapPair_EncodingBytes(t *testing.T) {
	kv := MapPair{
		Key:   []byte("hello-world-key1"),
		Value: []byte("this is the value ;-)"),
	}

	control, err := kv.Bytes()
	assert.Nil(t, err)

	encoded := make([]byte, kv.Size())
	err = kv.EncodeBytes(encoded)
	assert.Nil(t, err)

	assert.Equal(t, control, encoded)
}

// Key and value are each below the uint16 limit, but their combined encoded
// size is not.
func Test_MapPair_Bytes_CombinedSizeAboveUint16(t *testing.T) {
	tests := []struct {
		name     string
		keyLen   int
		valueLen int
	}{
		{name: "large value", keyLen: 8, valueLen: 65530},
		{name: "large key", keyLen: 65530, valueLen: 8},
		{name: "both large", keyLen: 40000, valueLen: 40000},
		{name: "encoded size exactly 65536", keyLen: 32766, valueLen: 32766},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := MapPair{
				Key:   bytes.Repeat([]byte{0xAB}, tt.keyLen),
				Value: bytes.Repeat([]byte{0xCD}, tt.valueLen),
			}

			var encoded []byte
			var err error
			require.NotPanics(t, func() { encoded, err = kv.Bytes() })
			require.NoError(t, err)
			require.Len(t, encoded, kv.Size())

			control := make([]byte, kv.Size())
			require.NoError(t, kv.EncodeBytes(control))
			assert.Equal(t, control, encoded)

			var decoded MapPair
			require.NoError(t, decoded.FromBytes(encoded, false))
			assert.Equal(t, kv.Key, decoded.Key)
			assert.Equal(t, kv.Value, decoded.Value)

			var reusable MapPair
			require.NoError(t, reusable.FromBytesReusable(encoded, false))
			assert.Equal(t, kv.Key, reusable.Key)
			assert.Equal(t, kv.Value, reusable.Value)
		})
	}
}
