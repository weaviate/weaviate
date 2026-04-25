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
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// requireBitmapValid asserts that bm's backing buffer has not been zeroed by
// the tracking pool's release function. It reads n[indexNumKeys] — the second
// uint64 in bm.data — which sroar always initialises to 1 (sentinel key=0x00
// container). A zeroed buffer has 0, indicating premature release.
//
// TODO aliszka:nested_filtering replace unsafe read with bm.NumKeys() once
// that method is exposed by the sroar library.
func requireBitmapValid(t *testing.T, bm *sroar.Bitmap) {
	t.Helper()
	require.NotNil(t, bm)
	// TODO aliszka:nested_filtering remove unsafe once sroar exposes NumKeys().
	// bm.data []uint16 is the first field of sroar.Bitmap. Its backing array
	// pointer is at offset 0 of the struct. n[indexNumKeys] is at byte offset 8
	// (second uint64) within that array.
	dataPtr := *(*unsafe.Pointer)(unsafe.Pointer(bm))
	numKeys := *(*uint64)(unsafe.Pointer(uintptr(dataPtr) + 8))
	require.Positive(t, numKeys, "bitmap backing buffer is zeroed — premature release?")
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

func TestBitmapOps_AndMaskLeaf(t *testing.T) {
	ops := newTrackingOps(t)

	// a is leaf-masked (e.g. preFilter from AndAllMaskLeaf), b has raw positions
	// (e.g. elemBitmap). Result keeps values from a whose (root, docID) appear
	// in b at any leaf. CloneToBuf(a) + AndMaskedConc(b) avoids re-masking a.
	t.Run("matches on root+docID regardless of leaf in b", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 0, 10)) // doc 10, leaf=0 (pre-masked)
		a.Set(Encode(1, 0, 20)) // doc 20, leaf=0 (pre-masked)

		b := sroar.NewBitmap()
		b.Set(Encode(1, 3, 10)) // doc 10 at leaf=3 (raw position)
		b.Set(Encode(1, 5, 30)) // doc 30 at leaf=5 (not in a)

		result, release := ops.AndMaskLeaf(a, b, 1)
		defer release()

		requireBitmapValid(t, result)
		arr := result.ToArray()
		require.Len(t, arr, 1)
		assert.Equal(t, uint16(1), DecodeRootIdx(arr[0]))
		assert.Equal(t, uint16(0), DecodeLeafIdx(arr[0])) // retains a's leaf=0
		assert.Equal(t, uint64(10), DecodeDocID(arr[0]))
	})

	t.Run("MaskLeafAnd gives empty when a is pre-masked, AndMaskLeaf does not", func(t *testing.T) {
		// MaskLeafAnd = MaskLeaf(a ∩ b): requires exact key match, so pre-masked
		// a (leaf=0) never matches raw b (leaf≠0) → always empty.
		// AndMaskLeaf = a AND MaskLeaf(b): correct for pre-masked a.
		a := sroar.NewBitmap()
		a.Set(Encode(1, 0, 42)) // leaf=0 (pre-masked)

		b := sroar.NewBitmap()
		b.Set(Encode(1, 7, 42)) // leaf=7 (raw position)

		wrong, rel1 := ops.MaskLeafAnd(a, b)
		defer rel1()
		requireBitmapValid(t, wrong)
		assert.True(t, wrong.IsEmpty())

		correct, rel2 := ops.AndMaskLeaf(a, b, 1)
		defer rel2()
		requireBitmapValid(t, correct)
		assert.False(t, correct.IsEmpty())
	})

	t.Run("no match when root differs", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 0, 10))

		b := sroar.NewBitmap()
		b.Set(Encode(2, 3, 10)) // same docID, different root

		result, release := ops.AndMaskLeaf(a, b, 1)
		defer release()

		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		a := sroar.NewBitmap()
		a.Set(Encode(1, 0, 10))
		b := sroar.NewBitmap()
		b.Set(Encode(1, 3, 10))
		origA := a.ToArray()
		origB := b.ToArray()

		_, release := ops.AndMaskLeaf(a, b, 1)
		defer release()

		assert.Equal(t, origA, a.ToArray())
		assert.Equal(t, origB, b.ToArray())
	})
}
