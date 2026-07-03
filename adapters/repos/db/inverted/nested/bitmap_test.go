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

func TestConstants(t *testing.T) {
	assert.Equal(t, 1<<16, MaxElems)
	assert.Equal(t, uint64(0x0000FFFFFFFFFFFF), uint64(MaxDocID))
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name    string
		elemIdx uint32
		docID   uint64
	}{
		{
			name:    "basic values",
			elemIdx: 1,
			docID:   42,
		},
		{
			name:    "max elemIdx",
			elemIdx: MaxElems - 1,
			docID:   100,
		},
		{
			name:    "max docID",
			elemIdx: 1,
			docID:   MaxDocID,
		},
		{
			name:    "all max values",
			elemIdx: MaxElems - 1,
			docID:   MaxDocID,
		},
		{
			name:    "typical nested object",
			elemIdx: 5,
			docID:   12345,
		},
		{
			name:    "second top-level element",
			elemIdx: 4,
			docID:   999,
		},
		{
			name:    "zero docID",
			elemIdx: 1,
			docID:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := Encode(tc.elemIdx, tc.docID)
			assert.Equal(t, tc.elemIdx, DecodeElemIdx(encoded))
			assert.Equal(t, tc.docID, DecodeDocID(encoded))
		})
	}
}

func TestEncodeBitLayout(t *testing.T) {
	// Encode(1, 0) must occupy exactly bit 48.
	pos := Encode(1, 0)
	assert.Equal(t, uint64(1)<<48, pos)
	assert.Equal(t, uint32(1), DecodeElemIdx(pos))
	assert.Equal(t, uint64(0), DecodeDocID(pos))

	// Encode(2, 42) packs elemIdx at bits 63-48 and docID in bits 47-0.
	pos2 := Encode(2, 42)
	assert.Equal(t, uint64(2)<<48|42, pos2)
	assert.Equal(t, uint32(2), DecodeElemIdx(pos2))
	assert.Equal(t, uint64(42), DecodeDocID(pos2))

	// Encode(MaxElems-1, MaxDocID) saturates both fields.
	posMax := Encode(MaxElems-1, MaxDocID)
	assert.Equal(t, uint64(MaxElems-1)<<48|uint64(MaxDocID), posMax)
}

func TestEncodeOutOfRangeClipped(t *testing.T) {
	// elemIdx with upper bits set must be clipped to the low elemBits bits.
	// MaxElems+5 = 65536+5; the low 16 bits are 5.
	oversized := uint32(MaxElems + 5)
	encoded := Encode(oversized, 0)
	assert.Equal(t, uint32(5), DecodeElemIdx(encoded), "oversized elemIdx: lower bits preserved")
	assert.Equal(t, uint64(0), DecodeDocID(encoded), "docID must be unaffected")
	assert.Equal(t, uint64(5)<<elemShift, encoded, "clipped value must sit at bits 63-48 only")
}

func TestPositionsWithDocID(t *testing.T) {
	// Raw element indices are encoded with a real docID via PositionsWithDocID.
	idxs := []ElemIdx{1, 2, 3}

	result := PositionsWithDocID(42, idxs...)

	require.Len(t, result, 3)
	for i, pos := range result {
		assert.Equal(t, uint32(i+1), DecodeElemIdx(pos))
		assert.Equal(t, uint64(42), DecodeDocID(pos))
	}

	// Original indices must not be modified (they are value types, not pointers).
	assert.Equal(t, []ElemIdx{1, 2, 3}, idxs)
}

func TestPositionsWithDocIDMasksDocID(t *testing.T) {
	// docID exceeding 48 bits is clipped by Encode's docMask.
	idxs := []ElemIdx{1}
	result := PositionsWithDocID(MaxDocID+1, idxs...)
	assert.Equal(t, uint64(0), DecodeDocID(result[0]))
}

func TestBitmapOps_NewEmpty(t *testing.T) {
	ops := newTrackingOps(t)

	result, release := ops.NewEmpty(1024)
	defer release()

	requireBitmapValid(t, result)
	assert.True(t, result.IsEmpty())

	result.Set(Encode(2, 42))
	assert.False(t, result.IsEmpty())
}

func TestBitmapOps_MaskElem(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("elem bits zeroed, only docID remains", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(3, 999))
		bm.Set(Encode(4, 999))
		bm.Set(Encode(1, 999))
		bm.Set(Encode(1, 888))

		stripped, release := ops.MaskElem(bm)
		defer release()

		requireBitmapValid(t, stripped)
		arr := stripped.ToArray()
		assert.Len(t, arr, 2) // d999 and d888
		for _, v := range arr {
			assert.Equal(t, uint32(0), DecodeElemIdx(v))
		}

		docIDs := make([]uint64, len(arr))
		for i, v := range arr {
			docIDs[i] = DecodeDocID(v)
		}
		assert.ElementsMatch(t, []uint64{888, 999}, docIDs)
	})

	t.Run("deduplicates multiple elems for same doc", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(1, 42))
		bm.Set(Encode(2, 42))
		bm.Set(Encode(5, 42))

		masked, release := ops.MaskElem(bm)
		defer release()

		requireBitmapValid(t, masked)
		arr := masked.ToArray()
		assert.Len(t, arr, 1) // all three collapse to elemIdx=0, docID=42
		assert.Equal(t, uint32(0), DecodeElemIdx(arr[0]))
		assert.Equal(t, uint64(42), DecodeDocID(arr[0]))
	})

	t.Run("input not modified", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(2, 100))
		original := bm.ToArray()

		_, release := ops.MaskElem(bm)
		defer release()

		assert.Equal(t, original, bm.ToArray())
	})

	t.Run("DecodeDocID survives masking", func(t *testing.T) {
		bm := sroar.NewBitmap()
		bm.Set(Encode(7, 12345))

		masked, release := ops.MaskElem(bm)
		defer release()

		requireBitmapValid(t, masked)
		arr := masked.ToArray()
		require.Len(t, arr, 1)
		assert.Equal(t, uint64(12345), DecodeDocID(arr[0]))
	})
}

func TestBitmapOps_LiftToAncestor(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("empty input returns empty bitmap", func(t *testing.T) {
		result, release := ops.LiftToAncestor(sroar.NewBitmap(), sroar.NewBitmap())
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("lifts child markers to owning parent markers", func(t *testing.T) {
		// doc=42
		// garage g1 marker at elem 1; its cars at elems 2 and 3
		// garage g2 marker at elem 10; its car at elem 11
		parentAnchor := sroar.NewBitmap()
		parentAnchor.Set(Encode(1, 42))
		parentAnchor.Set(Encode(10, 42))

		children := sroar.NewBitmap()
		children.Set(Encode(2, 42))
		children.Set(Encode(3, 42))
		children.Set(Encode(11, 42))

		result, release := ops.LiftToAncestor(children, parentAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.ElementsMatch(t,
			[]uint64{Encode(1, 42), Encode(10, 42)},
			result.ToArray(),
		)
	})

	t.Run("does not cross docs", func(t *testing.T) {
		// Two children in different docs must lift only to the parent in their
		// own doc, never across doc boundaries.
		parentAnchor := sroar.NewBitmap()
		parentAnchor.Set(Encode(1, 42))
		parentAnchor.Set(Encode(1, 99))

		children := sroar.NewBitmap()
		children.Set(Encode(5, 42))
		children.Set(Encode(5, 99))

		result, release := ops.LiftToAncestor(children, parentAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.ElementsMatch(t,
			[]uint64{Encode(1, 42), Encode(1, 99)},
			result.ToArray(),
		)
	})

	t.Run("does not cross docs when uint64 order interleaves same-elem positions", func(t *testing.T) {
		// In the encoded uint64 space, items with the same elemIdx but
		// different docIDs are adjacent (docID is the low-order field). The
		// bucket check (p & zeroElemBits == c & zeroElemBits) must still find
		// the correct ancestor in the same docID bucket.
		parentAnchor := sroar.NewBitmap()
		parentAnchor.Set(Encode(1, 100))
		parentAnchor.Set(Encode(1, 200))
		parentAnchor.Set(Encode(10, 100))
		parentAnchor.Set(Encode(10, 200))

		children := sroar.NewBitmap()
		children.Set(Encode(2, 100))
		children.Set(Encode(11, 200))

		result, release := ops.LiftToAncestor(children, parentAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.ElementsMatch(t,
			[]uint64{Encode(1, 100), Encode(10, 200)},
			result.ToArray(),
		)
	})

	t.Run("drops child whose doc has no parent in parentAnchor", func(t *testing.T) {
		parentAnchor := sroar.NewBitmap()
		parentAnchor.Set(Encode(1, 100))

		children := sroar.NewBitmap()
		children.Set(Encode(5, 100)) // doc 100 has a parent
		children.Set(Encode(5, 200)) // doc 200 absent from parentAnchor

		result, release := ops.LiftToAncestor(children, parentAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.ElementsMatch(t,
			[]uint64{Encode(1, 100)},
			result.ToArray(),
		)
	})

	t.Run("dedupes when multiple children share a parent", func(t *testing.T) {
		// Three cars under the same garage must lift to exactly one garage
		// marker — the bitmap naturally deduplicates via Set idempotency.
		parentAnchor := sroar.NewBitmap()
		parentAnchor.Set(Encode(1, 42))

		children := sroar.NewBitmap()
		children.Set(Encode(2, 42))
		children.Set(Encode(3, 42))
		children.Set(Encode(4, 42))

		result, release := ops.LiftToAncestor(children, parentAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(1, 42)}, result.ToArray())
		assert.Equal(t, 1, result.GetCardinality())
	})

	t.Run("lifts directly to grandparent skipping intermediate level", func(t *testing.T) {
		// Single doc. DFS elemIdx allocation (walker emits self before descendants):
		//   country @ 1
		//   garage_0 @ 2, cars @ 3,4,5
		//   garage_1 @ 6, cars @ 7,8
		//   garage_2 @ 9, cars @ 10,11,12,13
		//   garage_3 @ 14, cars @ 15,16
		// All 11 cars must lift to the single country self marker.
		countryAnchor := sroar.NewBitmap()
		countryAnchor.Set(Encode(1, 42))

		children := sroar.NewBitmap()
		for _, elem := range []uint32{3, 4, 5, 7, 8, 10, 11, 12, 13, 15, 16} {
			children.Set(Encode(elem, 42))
		}

		result, release := ops.LiftToAncestor(children, countryAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(1, 42)}, result.ToArray())
		assert.Equal(t, 1, result.GetCardinality())
	})

	t.Run("multi-level lift equals chained single-level lifts", func(t *testing.T) {
		// Same layout as above. Lifting cars→country directly must match the
		// result of lifting cars→garages then garages→country (two passes).
		countryAnchor := sroar.NewBitmap()
		countryAnchor.Set(Encode(1, 42))

		garageAnchor := sroar.NewBitmap()
		for _, elem := range []uint32{2, 6, 9, 14} {
			garageAnchor.Set(Encode(elem, 42))
		}

		children := sroar.NewBitmap()
		for _, elem := range []uint32{3, 4, 5, 7, 8, 10, 11, 12, 13, 15, 16} {
			children.Set(Encode(elem, 42))
		}

		direct, releaseDirect := ops.LiftToAncestor(children, countryAnchor)
		defer releaseDirect()

		garages, releaseG := ops.LiftToAncestor(children, garageAnchor)
		defer releaseG()
		chained, releaseC := ops.LiftToAncestor(garages, countryAnchor)
		defer releaseC()

		requireBitmapValid(t, direct)
		requireBitmapValid(t, chained)
		assert.ElementsMatch(t, direct.ToArray(), chained.ToArray())
	})

	t.Run("multi-level lift across 5 multi-doc buckets", func(t *testing.T) {
		// Five docs, each with a country at elem 1 and varying numbers of cars
		// at elems 3..N. Each car must lift to the country self in its own doc.
		docCars := map[uint64]int{
			100: 3,
			200: 1,
			300: 4,
			400: 2,
			500: 3,
		}

		countryAnchor := sroar.NewBitmap()
		children := sroar.NewBitmap()
		expected := make([]uint64, 0, len(docCars))
		for docID, n := range docCars {
			countryAnchor.Set(Encode(1, docID))
			expected = append(expected, Encode(1, docID))
			for k := 0; k < n; k++ {
				children.Set(Encode(uint32(3+k), docID))
			}
		}

		result, release := ops.LiftToAncestor(children, countryAnchor)
		defer release()

		requireBitmapValid(t, result)
		assert.ElementsMatch(t, expected, result.ToArray())
		assert.Equal(t, len(docCars), result.GetCardinality(),
			"all 5 doc-level countries must appear exactly once")
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

func TestBitmapOps_AndNot(t *testing.T) {
	ops := newTrackingOps(t)

	t.Run("subtracts subtract from base", func(t *testing.T) {
		base := sroar.NewBitmap()
		base.SetMany([]uint64{1, 2, 3, 4, 5})
		sub := sroar.NewBitmap()
		sub.SetMany([]uint64{2, 4})
		result, release := ops.AndNot(base, sub, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 3, 5}, result.ToArray())
	})

	t.Run("subtract empty returns clone of base", func(t *testing.T) {
		base := sroar.NewBitmap()
		base.SetMany([]uint64{1, 2, 3})
		result, release := ops.AndNot(base, sroar.NewBitmap(), 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{1, 2, 3}, result.ToArray())
	})

	t.Run("subtract everything returns empty", func(t *testing.T) {
		base := sroar.NewBitmap()
		base.SetMany([]uint64{1, 2, 3})
		result, release := ops.AndNot(base, base, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.True(t, result.IsEmpty())
	})

	t.Run("inputs not modified", func(t *testing.T) {
		base := sroar.NewBitmap()
		base.SetMany([]uint64{1, 2, 3})
		sub := sroar.NewBitmap()
		sub.SetMany([]uint64{2})
		origBase := base.ToArray()
		origSub := sub.ToArray()
		_, release := ops.AndNot(base, sub, 1)
		defer release()
		assert.Equal(t, origBase, base.ToArray())
		assert.Equal(t, origSub, sub.ToArray())
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

	t.Run("union of position-encoded bitmaps preserves elems", func(t *testing.T) {
		// elem 2 at doc 100, elem 3 at doc 100 — both must appear in the union.
		b := sroar.NewBitmap()
		b.Set(Encode(2, 100))
		c := sroar.NewBitmap()
		c.Set(Encode(3, 100))
		result, release := ops.OrAll([]*sroar.Bitmap{b, c}, 1)
		defer release()
		requireBitmapValid(t, result)
		assert.Equal(t, []uint64{Encode(2, 100), Encode(3, 100)}, result.ToArray())
	})
}
