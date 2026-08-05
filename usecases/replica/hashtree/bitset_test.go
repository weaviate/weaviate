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

package hashtree

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitSet(t *testing.T) {
	bsetSize := 2 << 15

	bset := NewBitset(bsetSize)

	require.Zero(t, bset.SetCount())

	for i := 0; i < bsetSize; i++ {
		require.False(t, bset.IsSet(i))
	}

	require.False(t, bset.AllSet())

	for i := 0; i < bsetSize; i++ {
		bset.Set(i)
		require.True(t, bset.IsSet(i))
	}

	require.True(t, bset.AllSet())
	require.Equal(t, bsetSize, bset.SetCount())

	bset.Reset()
	require.Zero(t, bset.SetCount())

	require.Panics(t, func() {
		bset.IsSet(bsetSize)
	})

	require.Panics(t, func() {
		bset.Set(bsetSize)
	})

	require.Panics(t, func() {
		bset.Unset(bsetSize)
	})
}

func TestBitsetUnmarshalRoundTrip(t *testing.T) {
	testCases := []struct {
		name string
		bset *Bitset
	}{
		{"empty", NewBitset(1)},
		{"single bit", NewBitset(1).Set(0)},
		{"sparse bits", NewBitset(100).Set(3).Set(50).Set(99)},
		{"set all word aligned", NewBitset(64).SetAll()},
		{"set all with trailing bits", NewBitset(100).SetAll()},
		{"set all tiny", NewBitset(8).SetAll()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := tc.bset.Marshal()
			require.NoError(t, err)

			var decoded Bitset
			require.NoError(t, decoded.Unmarshal(b))
			require.Equal(t, tc.bset.Size(), decoded.Size())
			require.Equal(t, tc.bset.SetCount(), decoded.SetCount())
			for i := 0; i < tc.bset.Size(); i++ {
				require.Equal(t, tc.bset.IsSet(i), decoded.IsSet(i))
			}
		})
	}
}

func TestBitsetUnmarshalRejectsInconsistentSetCount(t *testing.T) {
	testCases := []struct {
		name         string
		claimedCount uint32
	}{
		{"understated count", 0},
		{"partially understated count", 2},
		{"overstated count", 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBitset(100).Set(3).Set(50).Set(99).Marshal()
			require.NoError(t, err)

			binary.BigEndian.PutUint32(b[4:], tc.claimedCount)

			var decoded Bitset
			require.ErrorIs(t, decoded.Unmarshal(b), ErrInvalidBsetSerialization)
		})
	}
}
