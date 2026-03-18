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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name    string
		root    uint16
		leaf    uint16
		docID   uint64
	}{
		{
			name:  "basic values",
			root:  1,
			leaf:  1,
			docID: 42,
		},
		{
			name:  "max root",
			root:  MaxRoots - 1,
			leaf:  1,
			docID: 100,
		},
		{
			name:  "max leaf",
			root:  1,
			leaf:  MaxLeavesPerRoot - 1,
			docID: 100,
		},
		{
			name:  "max docID",
			root:  1,
			leaf:  1,
			docID: MaxDocID,
		},
		{
			name:  "all max values",
			root:  MaxRoots - 1,
			leaf:  MaxLeavesPerRoot - 1,
			docID: MaxDocID,
		},
		{
			name:  "typical nested object",
			root:  1,
			leaf:  5,
			docID: 12345,
		},
		{
			name:  "object array second element",
			root:  2,
			leaf:  3,
			docID: 999,
		},
		{
			name:  "zero docID",
			root:  1,
			leaf:  1,
			docID: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := Encode(tc.root, tc.leaf, tc.docID)
			assert.Equal(t, tc.root, DecodeRootIdx(encoded))
			assert.Equal(t, tc.leaf, DecodeLeafIdx(encoded))
			assert.Equal(t, tc.docID, DecodeDocID(encoded))
		})
	}
}

func TestEncodeBitLayout(t *testing.T) {
	// root=1, leaf=1, docID=0 should set bit 50 and bit 36
	pos := Encode(1, 1, 0)
	assert.Equal(t, uint16(1), DecodeRootIdx(pos))
	assert.Equal(t, uint16(1), DecodeLeafIdx(pos))
	assert.Equal(t, uint64(0), DecodeDocID(pos))

	// Verify the actual bit positions
	assert.Equal(t, uint64(1)<<50|uint64(1)<<36, pos)

	// root=2, leaf=3, docID=42
	pos2 := Encode(2, 3, 42)
	assert.Equal(t, uint64(2)<<50|uint64(3)<<36|42, pos2)
}

func TestEncodePositions(t *testing.T) {
	docIDs := []uint64{10, 20, 30}
	positions := EncodePositions(1, 5, docIDs)

	require.Len(t, positions, 3)
	for i, pos := range positions {
		assert.Equal(t, uint16(1), DecodeRootIdx(pos))
		assert.Equal(t, uint16(5), DecodeLeafIdx(pos))
		assert.Equal(t, docIDs[i], DecodeDocID(pos))
	}
}

func TestOrDocID(t *testing.T) {
	// Create position templates with docID=0
	templates := []uint64{
		Encode(1, 1, 0),
		Encode(1, 2, 0),
		Encode(1, 3, 0),
	}

	result := OrDocID(templates, 42)

	require.Len(t, result, 3)
	for i, pos := range result {
		assert.Equal(t, uint16(1), DecodeRootIdx(pos))
		assert.Equal(t, uint16(uint16(i)+1), DecodeLeafIdx(pos))
		assert.Equal(t, uint64(42), DecodeDocID(pos))
	}

	// Original templates should not be modified
	for _, pos := range templates {
		assert.Equal(t, uint64(0), DecodeDocID(pos))
	}
}

func TestOrDocIDMasksDocID(t *testing.T) {
	// docID exceeding 36 bits should be masked
	templates := []uint64{Encode(1, 1, 0)}
	result := OrDocID(templates, MaxDocID+1)
	assert.Equal(t, uint64(0), DecodeDocID(result[0]))
}

func TestValidateRootIdx(t *testing.T) {
	assert.NoError(t, ValidateRootIdx(1))
	assert.NoError(t, ValidateRootIdx(MaxRoots-1))
	assert.Error(t, ValidateRootIdx(0))
	assert.Error(t, ValidateRootIdx(MaxRoots))
	assert.Error(t, ValidateRootIdx(-1))
}

func TestValidateLeafIdx(t *testing.T) {
	assert.NoError(t, ValidateLeafIdx(1))
	assert.NoError(t, ValidateLeafIdx(MaxLeavesPerRoot-1))
	assert.Error(t, ValidateLeafIdx(0))
	assert.Error(t, ValidateLeafIdx(MaxLeavesPerRoot))
	assert.Error(t, ValidateLeafIdx(-1))
}

func TestMaskLeafPositions(t *testing.T) {
	bm := sroar.NewBitmap()
	// addresses[0].city="Berlin" in doc 123 with positions l3, l4
	bm.Set(Encode(1, 3, 123))
	bm.Set(Encode(1, 4, 123))
	// addresses[1].city="Hamburg" in doc 123 with position l5
	bm.Set(Encode(1, 5, 123))
	// Same in doc 456
	bm.Set(Encode(1, 3, 456))

	masked := MaskLeafPositions(bm)

	// After masking leaf bits, all positions with same root+docID collapse
	arr := masked.ToArray()
	assert.Len(t, arr, 2) // r1|d123 and r1|d456
	for _, v := range arr {
		assert.Equal(t, uint16(0), DecodeLeafIdx(v))
		assert.Equal(t, uint16(1), DecodeRootIdx(v))
	}
}

func TestMaskAllPositions(t *testing.T) {
	bm := sroar.NewBitmap()
	// Object array: root=1 in doc 999, root=2 in doc 999
	bm.Set(Encode(1, 3, 999))
	bm.Set(Encode(1, 4, 999))
	bm.Set(Encode(2, 1, 999))
	// Different doc
	bm.Set(Encode(1, 1, 888))

	stripped := MaskAllPositions(bm)

	arr := stripped.ToArray()
	assert.Len(t, arr, 2) // d999 and d888
	for _, v := range arr {
		assert.Equal(t, uint16(0), DecodeRootIdx(v))
		assert.Equal(t, uint16(0), DecodeLeafIdx(v))
	}

	docIDs := make([]uint64, len(arr))
	for i, v := range arr {
		docIDs[i] = DecodeDocID(v)
	}
	assert.ElementsMatch(t, []uint64{888, 999}, docIDs)
}

func TestDirectAND(t *testing.T) {
	// addresses.city="Madrid" AND addresses.postcode="28001" (same element)
	// Both have positions {r1|l2|d124}
	cityBm := sroar.NewBitmap()
	cityBm.Set(Encode(1, 2, 124))

	postcodeBm := sroar.NewBitmap()
	postcodeBm.Set(Encode(1, 2, 124))

	result := cityBm.And(postcodeBm)
	arr := result.ToArray()
	require.Len(t, arr, 1)
	assert.Equal(t, Encode(1, 2, 124), arr[0])
}

func TestDirectAND_DifferentElements(t *testing.T) {
	// addresses.city="Berlin" (root=1) AND addresses.city="Hamburg" (root=2)
	// Should not match since different root elements
	berlinBm := sroar.NewBitmap()
	berlinBm.Set(Encode(1, 3, 124))
	berlinBm.Set(Encode(1, 4, 124))

	hamburgBm := sroar.NewBitmap()
	hamburgBm.Set(Encode(2, 5, 124))

	result := berlinBm.And(hamburgBm)
	assert.Equal(t, 0, result.GetCardinality())
}

func TestMaskPosThenAND_CrossSubtree(t *testing.T) {
	// addresses.city="Berlin" AND cars.make="BMW" (sibling subtrees under root)
	// Different leaf positions but same root+docID
	cityBm := sroar.NewBitmap()
	cityBm.Set(Encode(1, 3, 123))
	cityBm.Set(Encode(1, 4, 123))

	makeBm := sroar.NewBitmap()
	makeBm.Set(Encode(1, 7, 123))
	makeBm.Set(Encode(1, 8, 123))

	// MaskLeafPositions to erase leaf differences
	maskedCity := MaskLeafPositions(cityBm)
	maskedMake := MaskLeafPositions(makeBm)

	result := maskedCity.And(maskedMake)
	arr := result.ToArray()
	require.Len(t, arr, 1)
	assert.Equal(t, uint16(1), DecodeRootIdx(arr[0]))
	assert.Equal(t, uint64(123), DecodeDocID(arr[0]))
}

func TestConstants(t *testing.T) {
	assert.Equal(t, 1<<14, MaxRoots)
	assert.Equal(t, 1<<14, MaxLeavesPerRoot)
	assert.Equal(t, uint64(0x0000000FFFFFFFFF), uint64(MaxDocID))
}
