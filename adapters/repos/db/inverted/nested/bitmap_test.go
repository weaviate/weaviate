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
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// requireBitmapValid asserts that bm's backing buffer has not been zeroed by
// the tracking pool's release function. sroar always initialises the bitmap
// with one sentinel container (key=0x00), so NumContainers() >= 1 indicates a
// live buffer; a zeroed buffer reports 0.
func requireBitmapValid(t *testing.T, bm *sroar.Bitmap) {
	t.Helper()
	require.NotNil(t, bm)
	require.Positive(t, bm.NumContainers(), "bitmap backing buffer is zeroed — premature release?")
}

func newTrackingOps(t *testing.T) *BitmapOps {
	t.Helper()
	pool := roaringset.NewBitmapBufPoolTracking()
	t.Cleanup(func() {
		if n := pool.Outstanding(); n != 0 {
			t.Errorf("pool: %d bitmap buffer(s) not released", n)
		}
	})
	return NewBitmapOps(pool)
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		root  uint16
		leaf  uint16
		docID uint64
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

func TestEncodeOutOfRangeClipped(t *testing.T) {
	// leafIdx with upper bits set would shift into the root field without
	// masking. Using MaxLeavesPerRoot+5 (clips to 5) proves the lower bits are
	// preserved and the overflow bits are dropped, not that the result is zero.
	oversizedLeaf := uint16(MaxLeavesPerRoot + 5) // clips to 5
	encoded := Encode(1, oversizedLeaf, 0)
	assert.Equal(t, uint16(1), DecodeRootIdx(encoded), "oversized leafIdx must not bleed into root field")
	assert.Equal(t, uint16(5), DecodeLeafIdx(encoded), "oversized leafIdx lower bits are preserved")

	// rootIdx with upper bits set is clipped; lower bits must not spill into
	// leaf. Using MaxRoots+3 (clips to 3) proves the same.
	oversizedRoot := uint16(MaxRoots + 3) // clips to 3
	encoded2 := Encode(oversizedRoot, 1, 0)
	assert.Equal(t, uint16(3), DecodeRootIdx(encoded2), "oversized rootIdx lower bits are preserved")
	assert.Equal(t, uint16(1), DecodeLeafIdx(encoded2), "oversized rootIdx must not bleed into leaf field")

	// Both oversized simultaneously: each field clips independently.
	encoded3 := Encode(oversizedRoot, oversizedLeaf, 0)
	assert.Equal(t, uint16(3), DecodeRootIdx(encoded3), "oversized rootIdx lower bits are preserved")
	assert.Equal(t, uint16(5), DecodeLeafIdx(encoded3), "oversized leafIdx lower bits are preserved")
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

func TestConstants(t *testing.T) {
	assert.Equal(t, 1<<14, MaxRoots)
	assert.Equal(t, 1<<14, MaxLeavesPerRoot)
	assert.Equal(t, uint64(0x0000000FFFFFFFFF), uint64(MaxDocID))
}

func TestBitmapOps_NewEmpty(t *testing.T) {
	ops := newTrackingOps(t)

	result, release := ops.NewEmpty(1024)
	defer release()

	requireBitmapValid(t, result)
	assert.NotNil(t, result)
	assert.True(t, result.IsEmpty())

	result.Set(Encode(1, 2, 42))
	assert.False(t, result.IsEmpty())
}

func TestBitmapOps_MaskLeaf(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("leaf bits zeroed, root and docID preserved", func(t *testing.T) {
		bm := sroar.NewBitmap()
		// addresses[0].city="Berlin" in doc 123 with positions l3, l4
		bm.Set(Encode(1, 3, 123))
		bm.Set(Encode(1, 4, 123))
		// addresses[1].city="Hamburg" in doc 123 with position l5
		bm.Set(Encode(1, 5, 123))
		// Same in doc 456
		bm.Set(Encode(1, 3, 456))

		masked, release := ops.MaskLeaf(bm)
		defer release()

		requireBitmapValid(t, masked)
		// After masking leaf bits, all positions with same root+docID collapse
		arr := masked.ToArray()
		assert.Len(t, arr, 2) // r1|d123 and r1|d456
		for _, v := range arr {
			assert.Equal(t, uint16(0), DecodeLeafIdx(v))
			assert.Equal(t, uint16(1), DecodeRootIdx(v))
		}
	})

	t.Run("input not modified", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(1, 3, 123))
		original := bm.ToArray()

		_, release := ops.MaskLeaf(bm)
		defer release()

		assert.Equal(t, original, bm.ToArray())
	})

	t.Run("cross-subtree masking enables AND", func(t *testing.T) {
		// addresses.city="Berlin" AND cars.make="BMW" (sibling subtrees under root)
		// Different leaf positions but same root+docID
		cityBm := sroar.NewBitmap()
		cityBm.Set(Encode(1, 3, 123))
		cityBm.Set(Encode(1, 4, 123))

		makeBm := sroar.NewBitmap()
		makeBm.Set(Encode(1, 7, 123))
		makeBm.Set(Encode(1, 8, 123))

		maskedCity, relCity := ops.MaskLeaf(cityBm)
		defer relCity()
		maskedMake, relMake := ops.MaskLeaf(makeBm)
		defer relMake()

		requireBitmapValid(t, maskedCity)
		requireBitmapValid(t, maskedMake)
		result := maskedCity.And(maskedMake)
		arr := result.ToArray()
		require.Len(t, arr, 1)
		assert.Equal(t, uint16(1), DecodeRootIdx(arr[0]))
		assert.Equal(t, uint64(123), DecodeDocID(arr[0]))
	})
}

func TestBitmapOps_MaskRootLeaf(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("root and leaf bits zeroed, only docID remains", func(t *testing.T) {
		bm := sroar.NewBitmap()
		// Object array: root=1 in doc 999, root=2 in doc 999
		bm.Set(Encode(1, 3, 999))
		bm.Set(Encode(1, 4, 999))
		bm.Set(Encode(2, 1, 999))
		// Different doc
		bm.Set(Encode(1, 1, 888))

		stripped, release := ops.MaskRootLeaf(bm)
		defer release()

		requireBitmapValid(t, stripped)
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
	})

	t.Run("input not modified", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(2, 5, 100))
		original := bm.ToArray()

		_, release := ops.MaskRootLeaf(bm)
		defer release()

		assert.Equal(t, original, bm.ToArray())
	})
}

func TestBitmapOps_AndAll(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("empty input returns empty bitmap", func(t *testing.T) {
		result, release := ops.AndAll(nil, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("single bitmap cloned", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.SetMany([]uint64{1, 2, 3})
		result, release := ops.AndAll([]*sroar.Bitmap{bm}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, bm.ToArray(), result.ToArray())
		// result is a clone — mutating it must not affect the original
		result.Remove(2)
		assert.Equal(t, []uint64{1, 2, 3}, bm.ToArray())
	})

	t.Run("intersection of two bitmaps", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.SetMany([]uint64{1, 2, 3, 4})
		b := sroar.NewBitmap()
		b.SetMany([]uint64{2, 4, 6})
		result, release := ops.AndAll([]*sroar.Bitmap{a, b}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{2, 4}, result.ToArray())
		// inputs unmodified
		assert.Equal(t, []uint64{1, 2, 3, 4}, a.ToArray())
	})

	t.Run("three bitmaps intersection", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.SetMany([]uint64{1, 2, 3, 4, 5})
		b := sroar.NewBitmap()
		b.SetMany([]uint64{2, 3, 4})
		c := sroar.NewBitmap()
		c.SetMany([]uint64{3, 4, 5})
		result, release := ops.AndAll([]*sroar.Bitmap{a, b, c}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{3, 4}, result.ToArray())
	})
}

func TestBitmapOps_AndAllMaskLeaf(t *testing.T) {
	ops := newTrackingOps(t)

	doc := uint64(42)
	posA := Encode(1, 3, doc) // root=1 leaf=3 doc=42
	posB := Encode(1, 7, doc) // root=1 leaf=7 doc=42 (different leaf)
	posC := Encode(2, 3, doc) // root=2 leaf=3 doc=42 (different root)

	t.Run("empty input returns empty bitmap", func(t *testing.T) {
		result, release := ops.AndAllMaskLeaf(nil, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("same root different leaf — AND succeeds after masking", func(t *testing.T) {
		bmA := sroar.NewBitmap()
		bmA.Set(posA)
		bmB := sroar.NewBitmap()
		bmB.Set(posB)
		result, release := ops.AndAllMaskLeaf([]*sroar.Bitmap{bmA, bmB}, 1)
		defer release()
		requireBitmapValid(t, result)
		// After masking leaf bits: both become Encode(1, 0, 42) → AND non-empty
		require.False(t, result.IsEmpty())
		// Result contains root+docID only (leaf zeroed)
		assert.Equal(t, uint16(0), DecodeLeafIdx(result.ToArray()[0]))
		assert.Equal(t, uint16(1), DecodeRootIdx(result.ToArray()[0]))
	})

	t.Run("different root — AND gives empty after masking", func(t *testing.T) {
		bmA := sroar.NewBitmap()
		bmA.Set(posA) // root=1
		bmC := sroar.NewBitmap()
		bmC.Set(posC) // root=2
		result, release := ops.AndAllMaskLeaf([]*sroar.Bitmap{bmA, bmC}, 1)
		defer release()
		requireBitmapValid(t, result)
		// After masking: root=1|doc=42 vs root=2|doc=42 → no overlap
		assert.True(t, result.IsEmpty())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		bmA := sroar.NewBitmap()
		bmA.Set(posA)
		_, release := ops.AndAllMaskLeaf([]*sroar.Bitmap{bmA}, 1)
		defer release()
		assert.Equal(t, []uint64{posA}, bmA.ToArray())
	})
}

func TestBitmapOps_MaskLeafAnd(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("intersection of same-element positions, leaf zeroed", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 3, 10))
		a.Set(Encode(1, 3, 20))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 3, 20))
		b.Set(Encode(1, 3, 30))

		result, release := ops.MaskLeafAnd(a, b)
		defer release()

		requireBitmapValid(t, result)
		arr := result.ToArray()
		require.Len(t, arr, 1)
		assert.Equal(t, uint16(0), DecodeLeafIdx(arr[0]))
		assert.Equal(t, uint64(20), DecodeDocID(arr[0]))
	})

	t.Run("no intersection returns empty", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 10))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 2, 20))

		result, release := ops.MaskLeafAnd(a, b)
		defer release()

		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 3, 10))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 3, 10))
		origA := a.ToArray()
		origB := b.ToArray()

		_, release := ops.MaskLeafAnd(a, b)
		defer release()

		assert.Equal(t, origA, a.ToArray())
		assert.Equal(t, origB, b.ToArray())
	})
}

func TestBitmapOps_IntersectsMaskedLeaf(t *testing.T) {
	ops := newTrackingOps(t)

	// rootDoc is leaf-masked (e.g. preFilter from AndAllMaskLeaf); raw has raw
	// positions (e.g. elemBitmap). Returns true if they share at least one
	// (root, docID) pair after zeroing raw's leaf bits. No allocation.
	t.Run("true when root+docID overlap regardless of raw's leaf", func(t *testing.T) {
		rootDoc := sroar.NewBitmap()
		rootDoc.Set(Encode(1, 0, 10)) // doc 10, leaf=0 (pre-masked)
		rootDoc.Set(Encode(1, 0, 20)) // doc 20, leaf=0 (pre-masked)

		raw := sroar.NewBitmap()
		raw.Set(Encode(1, 3, 10)) // doc 10 at leaf=3 (raw position)
		raw.Set(Encode(1, 5, 30)) // doc 30 at leaf=5 (not in rootDoc)

		assert.True(t, ops.IntersectsMaskedLeaf(rootDoc, raw))
	})

	t.Run("false when root differs", func(t *testing.T) {
		rootDoc := sroar.NewBitmap()
		rootDoc.Set(Encode(1, 0, 10))

		raw := sroar.NewBitmap()
		raw.Set(Encode(2, 3, 10)) // same docID, different root

		assert.False(t, ops.IntersectsMaskedLeaf(rootDoc, raw))
	})

	t.Run("inputs not modified", func(t *testing.T) {
		rootDoc := sroar.NewBitmap()
		rootDoc.Set(Encode(1, 0, 10))
		raw := sroar.NewBitmap()
		raw.Set(Encode(1, 3, 10))
		origRootDoc := rootDoc.ToArray()
		origRaw := raw.ToArray()

		ops.IntersectsMaskedLeaf(rootDoc, raw)

		assert.Equal(t, origRootDoc, rootDoc.ToArray())
		assert.Equal(t, origRaw, raw.ToArray())
	})
}

func TestBitmapOps_OrAll(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("empty input returns empty bitmap", func(t *testing.T) {
		result, release := ops.OrAll(nil, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("single bitmap returns its values", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.SetMany([]uint64{1, 2, 3})
		result, release := ops.OrAll([]*sroar.Bitmap{bm}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 2, 3}, result.ToArray())
		// result is not the input — mutating must not affect the original
		result.Set(99)
		assert.Equal(t, []uint64{1, 2, 3}, bm.ToArray())
	})

	t.Run("union of two overlapping bitmaps", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.SetMany([]uint64{1, 2, 3})
		b := sroar.NewBitmap()
		b.SetMany([]uint64{2, 3, 4})
		result, release := ops.OrAll([]*sroar.Bitmap{a, b}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 2, 3, 4}, result.ToArray())
		// inputs unmodified
		assert.Equal(t, []uint64{1, 2, 3}, a.ToArray())
		assert.Equal(t, []uint64{2, 3, 4}, b.ToArray())
	})

	t.Run("union of disjoint bitmaps", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.SetMany([]uint64{1, 2})
		b := sroar.NewBitmap()
		b.SetMany([]uint64{10, 20})
		result, release := ops.OrAll([]*sroar.Bitmap{a, b}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 2, 10, 20}, result.ToArray())
	})

	t.Run("union of three bitmaps", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.SetMany([]uint64{1, 2})
		b := sroar.NewBitmap()
		b.SetMany([]uint64{3})
		c := sroar.NewBitmap()
		c.SetMany([]uint64{2, 4, 5})
		result, release := ops.OrAll([]*sroar.Bitmap{a, b, c}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 2, 3, 4, 5}, result.ToArray())
	})

	t.Run("union with an empty input among many", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.SetMany([]uint64{1, 2})
		empty := sroar.NewBitmap()
		c := sroar.NewBitmap()
		c.SetMany([]uint64{3, 4})
		result, release := ops.OrAll([]*sroar.Bitmap{a, empty, c}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 2, 3, 4}, result.ToArray())
	})

	t.Run("union of position-encoded bitmaps preserves leaves", func(t *testing.T) {
		// B = cars.tires.width=205 → leaf 2 in doc 100
		// C = cars.colors=red      → leaf 3 in doc 100
		b := sroar.NewBitmap()
		b.Set(Encode(1, 2, 100))
		c := sroar.NewBitmap()
		c.Set(Encode(1, 3, 100))
		result, release := ops.OrAll([]*sroar.Bitmap{b, c}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(1, 2, 100), Encode(1, 3, 100)}, result.ToArray())
	})
}

func TestBitmapOps_CopresenceByMaskAll(t *testing.T) {
	ops := newTrackingOps(t)

	// Co-presence groups by (root, doc) via the package-level zeroLeafBits
	// mask, which satisfies sroar's mask shape requirement (low 16 bits
	// inside the docID region are all set).

	t.Run("empty input returns empty bitmap", func(t *testing.T) {
		result, release := ops.CopresenceByMaskAll(nil, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("single input is clone", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(1, 1, 100))
		bm.Set(Encode(1, 2, 100))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{bm}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(1, 1, 100), Encode(1, 2, 100)}, result.ToArray())
	})

	t.Run("one input empty returns empty", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 100))
		empty := sroar.NewBitmap()
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, empty}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("single car satisfies all — union of contributing leaves", func(t *testing.T) {
		// A_in_car = spoiler accessory leaf;
		// OR_in_car = 205 tire leaf + red color leaf.
		// All in same (root=1, doc=1). Co-presence = {(1, 0, 1)}.
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 1))
		or := sroar.NewBitmap()
		or.Set(Encode(1, 2, 1))
		or.Set(Encode(1, 3, 1))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{
			Encode(1, 1, 1), Encode(1, 2, 1), Encode(1, 3, 1),
		}, result.ToArray())
	})

	t.Run("cross-country split — different root_idx, no co-presence", func(t *testing.T) {
		// A in country 0 (root=1); OR in country 1 (root=2). Same doc.
		// Co-presence on (root, doc): {(1,0,8)} ∩ {(2,0,8)} = ∅.
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 8))
		or := sroar.NewBitmap()
		or.Set(Encode(2, 1, 8))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("cross-doc — same root different docs, no co-presence", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 1))
		or := sroar.NewBitmap()
		or.Set(Encode(1, 2, 2))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("multi-doc — only co-present (root, doc) pairs survive", func(t *testing.T) {
		// A in docs 1, 2, 3. OR only in doc 2. Only doc 2 co-presents.
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 1))
		a.Set(Encode(1, 1, 2))
		a.Set(Encode(1, 1, 3))
		or := sroar.NewBitmap()
		or.Set(Encode(1, 2, 2))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(1, 1, 2), Encode(1, 2, 2)}, result.ToArray())
	})

	t.Run("two countries both co-present — both contribute", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 1))
		a.Set(Encode(2, 1, 1))
		or := sroar.NewBitmap()
		or.Set(Encode(1, 2, 1))
		or.Set(Encode(2, 2, 1))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{
			Encode(1, 1, 1), Encode(1, 2, 1),
			Encode(2, 1, 1), Encode(2, 2, 1),
		}, result.ToArray())
	})

	t.Run("two countries, only one co-present — filter drops the other", func(t *testing.T) {
		// A in countries 1, 2. OR only in country 2.
		// Co-presence = {(2, 0, 9)}. (1, 1, 9) drops.
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 9))
		a.Set(Encode(2, 1, 9))
		or := sroar.NewBitmap()
		or.Set(Encode(2, 2, 9))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(2, 1, 9), Encode(2, 2, 9)}, result.ToArray())
	})

	t.Run("27-car doc K=0 L=1 — co-presence only via root=3", func(t *testing.T) {
		// Mirrors the iteration trace from the 27-car doc 100 walkthrough.
		// A_in_car = country 2's spoiler at (3, 3, 100).
		// OR_in_car = country 0's 205 tire at (1, 5, 100) + country 2's red
		// color at (3, 4, 100). Only (3, 0, 100) is co-present.
		// (1, 5, 100) drops out.
		a := sroar.NewBitmap()
		a.Set(Encode(3, 3, 100))
		or := sroar.NewBitmap()
		or.Set(Encode(1, 5, 100))
		or.Set(Encode(3, 4, 100))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(3, 3, 100), Encode(3, 4, 100)}, result.ToArray())
	})

	t.Run("three-way — all inputs at same (root, doc)", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 5))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 2, 5))
		c := sroar.NewBitmap()
		c.Set(Encode(1, 3, 5))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, b, c}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{
			Encode(1, 1, 5), Encode(1, 2, 5), Encode(1, 3, 5),
		}, result.ToArray())
	})

	t.Run("three-way — one input at wrong root, no co-presence", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 5))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 2, 5))
		c := sroar.NewBitmap()
		c.Set(Encode(2, 3, 5))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, b, c}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("three-way — partial co-presence, only complete group survives", func(t *testing.T) {
		// Country 1 has all three operands. Country 2 missing the third.
		// Only (1, 0, 5) is co-present in all three; (2, 0, 5) drops.
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 5))
		a.Set(Encode(2, 1, 5))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 2, 5))
		b.Set(Encode(2, 2, 5))
		c := sroar.NewBitmap()
		c.Set(Encode(1, 3, 5))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, b, c}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{
			Encode(1, 1, 5), Encode(1, 2, 5), Encode(1, 3, 5),
		}, result.ToArray())
	})

	t.Run("batch of 5 docs, only docs 2 and 4 co-present", func(t *testing.T) {
		a := sroar.NewBitmap()
		for _, doc := range []uint64{1, 2, 3, 4, 5} {
			a.Set(Encode(1, 1, doc))
		}
		or := sroar.NewBitmap()
		or.Set(Encode(1, 2, 2))
		or.Set(Encode(1, 2, 4))
		result, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		requireBitmapValid(t, result)
		// ToArray returns values in ascending uint64 order. With leaf bits at
		// positions 49-36 and docID at 35-0, all leaf=1 positions precede all
		// leaf=2 positions.
		assert.Equal(t, []uint64{
			Encode(1, 1, 2), Encode(1, 1, 4),
			Encode(1, 2, 2), Encode(1, 2, 4),
		}, result.ToArray())
	})

	t.Run("inputs unmodified", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 1, 1))
		aBefore := a.ToArray()
		or := sroar.NewBitmap()
		or.Set(Encode(1, 2, 1))
		orBefore := or.ToArray()
		_, release := ops.CopresenceByMaskAll([]*sroar.Bitmap{a, or}, zeroLeafBits)
		defer release()
		assert.Equal(t, aBefore, a.ToArray())
		assert.Equal(t, orBefore, or.ToArray())
	})
}

// TestBitmapOps_CopresenceByMaskAll_IdxLoopSimulation simulates the
// position-level eval cars-level AND combining step end-to-end. For
// each (garage K, car L) iteration, it narrows the per-condition raw
// bitmaps by car_scope = _idx.garages.cars[L] ∩ _idx.garages[K] and
// applies CopresenceByMaskAll. Per-iteration results are OR'd into the
// accumulator. Scenarios mirror the doc walkthroughs from the
// position-level eval discussion.
func TestBitmapOps_CopresenceByMaskAll_IdxLoopSimulation(t *testing.T) {
	ops := newTrackingOps(t)
	// Co-presence groups by (root, doc) via the package-level zeroLeafBits.

	docID := uint64(100)
	// pos packs a (root, leaf, doc=100) position for human-readable test data.
	pos := func(root, leaf uint16) uint64 { return Encode(root, leaf, docID) }

	// narrow simulates car_scope = idxKey ∩ garageScope. Returns a fresh
	// bitmap; inputs are unmodified.
	narrow := func(idxKey, garageScope *sroar.Bitmap) *sroar.Bitmap {
		clone := idxKey.Clone()
		clone.And(garageScope)
		return clone
	}

	// simulate runs the cars idx-loop. For each car_scope iteration, it
	// narrows each condition by car_scope (raw AND) and feeds the narrowed
	// bitmaps to CopresenceByMaskAll. Per-iteration results are OR'd into
	// the accumulator. Returns the accumulator's values.
	simulate := func(t *testing.T, conds []*sroar.Bitmap, carScopes []*sroar.Bitmap) []uint64 {
		t.Helper()
		accumulator := sroar.NewBitmap()
		for _, carScope := range carScopes {
			narrowed := make([]*sroar.Bitmap, len(conds))
			for i, cond := range conds {
				clone := cond.Clone()
				clone.And(carScope)
				narrowed[i] = clone
			}
			result, release := ops.CopresenceByMaskAll(narrowed, zeroLeafBits)
			accumulator.Or(result)
			release()
		}
		out := accumulator.ToArray()
		if out == nil {
			return []uint64{}
		}
		return out
	}

	t.Run("2x2x2 doc, rotation pattern — every car has A-only or OR-only, no match", func(t *testing.T) {
		// Country 1 (root=1) leaves:
		//   c1.g0.cars[0]: A → leaf 1
		//   c1.g0.cars[1]: OR → leaf 2
		//   c1.g1.cars[0]: OR → leaf 3
		//   c1.g1.cars[1]: A → leaf 4
		// Country 2 (root=2) leaves reset:
		//   c2.g0.cars[0]: OR → leaf 1
		//   c2.g0.cars[1]: A → leaf 2
		//   c2.g1.cars[0]: A → leaf 3
		//   c2.g1.cars[1]: OR → leaf 4
		aRaw := roaringset.NewBitmap(pos(1, 1), pos(1, 4), pos(2, 2), pos(2, 3))
		orRaw := roaringset.NewBitmap(pos(1, 2), pos(1, 3), pos(2, 1), pos(2, 4))

		garage0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2), pos(2, 1), pos(2, 2))
		garage1 := roaringset.NewBitmap(pos(1, 3), pos(1, 4), pos(2, 3), pos(2, 4))
		car0 := roaringset.NewBitmap(pos(1, 1), pos(1, 3), pos(2, 1), pos(2, 3))
		car1 := roaringset.NewBitmap(pos(1, 2), pos(1, 4), pos(2, 2), pos(2, 4))

		carScopes := []*sroar.Bitmap{
			narrow(car0, garage0), // K=0, L=0
			narrow(car1, garage0), // K=0, L=1
			narrow(car0, garage1), // K=1, L=0
			narrow(car1, garage1), // K=1, L=1
		}

		result := simulate(t, []*sroar.Bitmap{aRaw, orRaw}, carScopes)
		assert.Empty(t, result, "no single car has both A and OR; result must be empty")
	})

	t.Run("same 2x2x2 doc with c2.g1.cars[1] changed to A AND OR — single car flips it to match", func(t *testing.T) {
		// c2.g1.cars[1] now contains both spoiler (A) and 205 tire (B in OR).
		// Updated country 2 leaves:
		//   c2.g0.cars[0]: OR → leaf 1
		//   c2.g0.cars[1]: A → leaf 2
		//   c2.g1.cars[0]: A → leaf 3
		//   c2.g1.cars[1]: A AND OR → A at leaf 4, OR at leaf 5
		aRaw := roaringset.NewBitmap(pos(1, 1), pos(1, 4), pos(2, 2), pos(2, 3), pos(2, 4))
		orRaw := roaringset.NewBitmap(pos(1, 2), pos(1, 3), pos(2, 1), pos(2, 5))

		garage0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2), pos(2, 1), pos(2, 2))
		// c2.garages[1] descendants now span leaves 3, 4, 5
		garage1 := roaringset.NewBitmap(pos(1, 3), pos(1, 4), pos(2, 3), pos(2, 4), pos(2, 5))
		car0 := roaringset.NewBitmap(pos(1, 1), pos(1, 3), pos(2, 1), pos(2, 3))
		// c2.cars[1] in garages[1] occupies leaves 4 and 5
		car1 := roaringset.NewBitmap(pos(1, 2), pos(1, 4), pos(2, 2), pos(2, 4), pos(2, 5))

		carScopes := []*sroar.Bitmap{
			narrow(car0, garage0),
			narrow(car1, garage0),
			narrow(car0, garage1),
			narrow(car1, garage1), // matching iteration
		}

		result := simulate(t, []*sroar.Bitmap{aRaw, orRaw}, carScopes)
		// Co-presence at (2, 0, 100) in the K=1 L=1 iteration yields both
		// leaves of c2.g1.cars[1].
		assert.Equal(t, []uint64{pos(2, 4), pos(2, 5)}, result)
	})

	t.Run("cross-country split at same (K, L) — co-presence on (root, doc) rejects", func(t *testing.T) {
		// A only in c1.g0.cars[0] (root=1); OR only in c2.g0.cars[0] (root=2).
		// Both at L=0 in their respective country's garage[0]. Within the
		// single (K=0, L=0) iteration, both A_in_car and OR_in_car are
		// non-empty — but they project to different (root, doc) keys, so
		// co-presence is empty.
		aRaw := roaringset.NewBitmap(pos(1, 1))
		orRaw := roaringset.NewBitmap(pos(2, 1))
		garage0 := roaringset.NewBitmap(pos(1, 1), pos(2, 1))
		car0 := roaringset.NewBitmap(pos(1, 1), pos(2, 1))

		carScopes := []*sroar.Bitmap{narrow(car0, garage0)}

		result := simulate(t, []*sroar.Bitmap{aRaw, orRaw}, carScopes)
		assert.Empty(t, result, "different root_idx in same iteration must not co-present")
	})

	t.Run("cross-garage split (same country, same L) — parent garage scope rejects", func(t *testing.T) {
		// A only in c1.g0.cars[0]; OR only in c1.g1.cars[0]. Both at L=0
		// but different garages. The parent garage idx-loop narrows each
		// K's iteration to one physical garage, so each cars-level
		// iteration sees only one side.
		aRaw := roaringset.NewBitmap(pos(1, 1))  // c1.g0.cars[0]
		orRaw := roaringset.NewBitmap(pos(1, 3)) // c1.g1.cars[0]
		garage0 := roaringset.NewBitmap(pos(1, 1))
		garage1 := roaringset.NewBitmap(pos(1, 3))
		car0 := roaringset.NewBitmap(pos(1, 1), pos(1, 3)) // _idx.cars[0] mixes both garages' cars[0]

		carScopes := []*sroar.Bitmap{
			narrow(car0, garage0), // K=0, L=0 — only A holds
			narrow(car0, garage1), // K=1, L=0 — only OR holds
		}

		result := simulate(t, []*sroar.Bitmap{aRaw, orRaw}, carScopes)
		assert.Empty(t, result, "different garages must not produce a co-presence match — parent scope narrows")
	})

	t.Run("cross-car within same garage — different L iterations reject", func(t *testing.T) {
		// A only in c1.g0.cars[0]; OR only in c1.g0.cars[1]. Same country,
		// same garage, different cars. The cars idx-loop visits each L
		// separately so neither iteration has both.
		aRaw := roaringset.NewBitmap(pos(1, 1))
		orRaw := roaringset.NewBitmap(pos(1, 2))
		garage0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2))
		car0 := roaringset.NewBitmap(pos(1, 1))
		car1 := roaringset.NewBitmap(pos(1, 2))

		carScopes := []*sroar.Bitmap{
			narrow(car0, garage0),
			narrow(car1, garage0),
		}

		result := simulate(t, []*sroar.Bitmap{aRaw, orRaw}, carScopes)
		assert.Empty(t, result)
	})

	t.Run("two matching cars in same doc — each contributes its leaves", func(t *testing.T) {
		// Two cars satisfy A AND OR in the same doc:
		//   c1.g0.cars[0]: spoiler (leaf 1) + 205 tire (leaf 2)
		//   c2.g1.cars[1]: spoiler (leaf 4) + 205 tire (leaf 5)
		// Other cars have nothing — get their own leaves via Phase 2.
		// Country 1 layout: c1.g0.cars[0]=A+OR(1,2); c1.g0.cars[1]=∅(3);
		//                   c1.g1.cars[0]=∅(4); c1.g1.cars[1]=∅(5)
		// Country 2 layout: c2.g0.cars[0]=∅(1); c2.g0.cars[1]=∅(2);
		//                   c2.g1.cars[0]=∅(3); c2.g1.cars[1]=A+OR(4,5)
		aRaw := roaringset.NewBitmap(pos(1, 1), pos(2, 4))
		orRaw := roaringset.NewBitmap(pos(1, 2), pos(2, 5))
		garage0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2), pos(1, 3), pos(2, 1), pos(2, 2))
		garage1 := roaringset.NewBitmap(pos(1, 4), pos(1, 5), pos(2, 3), pos(2, 4), pos(2, 5))
		car0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2), pos(1, 4), pos(2, 1), pos(2, 3))
		car1 := roaringset.NewBitmap(pos(1, 3), pos(1, 5), pos(2, 2), pos(2, 4), pos(2, 5))

		carScopes := []*sroar.Bitmap{
			narrow(car0, garage0), // c1.g0.cars[0] matches; c2.g0.cars[0] empty
			narrow(car1, garage0), // c1.g0.cars[1] and c2.g0.cars[1] both empty
			narrow(car0, garage1), // c1.g1.cars[0] and c2.g1.cars[0] both empty
			narrow(car1, garage1), // c1.g1.cars[1] empty; c2.g1.cars[1] matches
		}

		result := simulate(t, []*sroar.Bitmap{aRaw, orRaw}, carScopes)
		assert.Equal(t, []uint64{
			pos(1, 1), pos(1, 2), // c1.g0.cars[0]'s contributing leaves
			pos(2, 4), pos(2, 5), // c2.g1.cars[1]'s contributing leaves
		}, result)
	})

	t.Run("three-way AND simulating sibling sub-arrays — same car wins, cross-car loses", func(t *testing.T) {
		// Three operands (e.g., accessories, tires, colors). One car has
		// all three; another country's car has two of three.
		// Country 1: c1.g0.cars[0] has all three (leaves 1, 2, 3)
		// Country 2: c2.g0.cars[0] has only A and B (leaves 1, 2); missing C
		// Country 2: c2.g0.cars[1] has only C (leaf 3) — different car
		//
		// Only c1.g0.cars[0] should match at K=0, L=0.
		aRaw := roaringset.NewBitmap(pos(1, 1), pos(2, 1))
		bRaw := roaringset.NewBitmap(pos(1, 2), pos(2, 2))
		cRaw := roaringset.NewBitmap(pos(1, 3), pos(2, 3))

		garage0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2), pos(1, 3), pos(2, 1), pos(2, 2), pos(2, 3))
		car0 := roaringset.NewBitmap(pos(1, 1), pos(1, 2), pos(1, 3), pos(2, 1), pos(2, 2))
		car1 := roaringset.NewBitmap(pos(2, 3))

		carScopes := []*sroar.Bitmap{
			narrow(car0, garage0), // K=0, L=0
			narrow(car1, garage0), // K=0, L=1
		}

		result := simulate(t, []*sroar.Bitmap{aRaw, bRaw, cRaw}, carScopes)
		// c1.g0.cars[0] has all three at root=1; that's the only co-presence.
		// At L=0, c2.g0.cars[0] has A and B but not C — co-presence on
		// (2, 0, 100) fails because cRaw has no value at root=2 with leaf
		// inside car_scope.
		assert.Equal(t, []uint64{pos(1, 1), pos(1, 2), pos(1, 3)}, result)
	})
}
