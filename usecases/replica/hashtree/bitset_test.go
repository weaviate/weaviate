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
	"encoding/hex"
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

func TestBitsetUnmarshalFailureLeavesReceiverUntouched(t *testing.T) {
	valid, err := NewBitset(100).Set(3).Set(50).Set(99).Marshal()
	require.NoError(t, err)

	badCount := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badCount[4:], 50)

	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badLength, 200)

	testCases := []struct {
		name    string
		payload []byte
	}{
		{"short header", []byte{0x01}},
		{"length mismatch", badLength},
		{"count mismatch", badCount},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bset := NewBitset(8).Set(1).Set(5)
			require.ErrorIs(t, bset.Unmarshal(tc.payload), ErrInvalidBsetSerialization)
			require.Equal(t, 8, bset.Size())
			require.Equal(t, 2, bset.SetCount())
			for i := 0; i < bset.Size(); i++ {
				require.Equal(t, i == 1 || i == 5, bset.IsSet(i))
			}
		})
	}
}

func TestBitsetWireFormatGolden(t *testing.T) {
	testCases := []struct {
		name    string
		hexData string
		size    int
		wantSet func(i int) bool
	}{
		{
			"root discriminant",
			"00000001" + "00000001" + "0000000000000001",
			1, func(i int) bool { return i == 0 },
		},
		{
			"empty",
			"00000001" + "00000000" + "0000000000000000",
			1, func(int) bool { return false },
		},
		{
			"sparse",
			"00000064" + "00000003" + "0004000000000008" + "0000000800000000",
			100, func(i int) bool { return i == 3 || i == 50 || i == 99 },
		},
		{
			"set all word aligned",
			"00000040" + "00000040" + "ffffffffffffffff",
			64, func(int) bool { return true },
		},
		{
			"set all with trailing bits",
			"00000064" + "00000064" + "ffffffffffffffff" + "ffffffffffffffff",
			100, func(int) bool { return true },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := hex.DecodeString(tc.hexData)
			require.NoError(t, err)

			var decoded Bitset
			require.NoError(t, decoded.Unmarshal(payload))
			require.Equal(t, tc.size, decoded.Size())

			wantCount := 0
			for i := 0; i < tc.size; i++ {
				require.Equal(t, tc.wantSet(i), decoded.IsSet(i), "bit %d", i)
				if tc.wantSet(i) {
					wantCount++
				}
			}
			require.Equal(t, wantCount, decoded.SetCount())

			remarshalled, err := decoded.Marshal()
			require.NoError(t, err)
			require.Equal(t, payload, remarshalled)
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
