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

package roaringsetrange

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

// 32 seeds balance differential coverage against the race-build guard's
// whole-plane cost per seed (~17s here vs ~2s unseeded).
const cascadeSeeds = 32

// cascadeEdgeValues pins the cases where the seed index is degenerate: no set
// bit at all, the lowest bit, the highest bit, and every bit set.
var cascadeEdgeValues = []uint64{
	0,
	1,
	2,
	3,
	1 << 63,
	1<<63 | 1,
	math.MaxUint64,
	math.MaxUint64 - 1,
	cascadeEncodeInt64(101), // an int64 range predicate as the inverted index encodes it
}

// cascadeEncodeInt64 mirrors entities/inverted.LexicographicallySortableInt64
// read back as a big-endian uint64: the sign bit is flipped, so a non-negative
// predicate always sets bit 63 and plane 64 ends up a copy of plane 0.
func cascadeEncodeInt64(v int64) uint64 { return uint64(v ^ math.MinInt64) }

func TestPlanesStaySubsetsOfPlaneZero(t *testing.T) {
	for seed := int64(0); seed < cascadeSeeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			// the fixture asserts the invariant after every write it performs
			newCascadeFixture(t, seed)
		})
	}
}

func TestSeededCascadeMatchesUnseededCascade(t *testing.T) {
	bufPool := roaringset.NewBitmapBufPoolNoop()
	operators := []filters.Operator{
		filters.OperatorEqual,
		filters.OperatorNotEqual,
		filters.OperatorLessThan,
		filters.OperatorLessThanEqual,
		filters.OperatorGreaterThan,
		filters.OperatorGreaterThanEqual,
	}

	t.Run("empty segment", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		assertCascadesAgree(t, NewSegmentInMemory(logger), bufPool, operators, cascadeEdgeValues, 0)
	})

	for seed := int64(0); seed < cascadeSeeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			seg := newCascadeFixture(t, seed)

			values := append([]uint64{}, cascadeEdgeValues...)
			for i := 0; i < 10; i++ {
				values = append(values, cascadeRandomValue(rng))
			}
			assertCascadesAgree(t, seg, bufPool, operators, values, seed)
		})
	}
}

func assertCascadesAgree(t *testing.T, seg *SegmentInMemory, bufPool roaringset.BitmapBufPool,
	operators []filters.Operator, values []uint64, seed int64,
) {
	t.Helper()

	readers, release := seg.Readers(bufPool)
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	for _, value := range values {
		for _, conc := range []int{1, 4} {
			got, gotRelease := reader.mergeGreaterThanEqual(value, conc)
			require.Equalf(t, canonicalBytes(unseededGreaterThanEqual(reader.bitmaps, value, conc)),
				canonicalBytes(got),
				"mergeGreaterThanEqual disagrees; seed=%d value=%d(%#016x) conc=%d", seed, value, value, conc)
			gotRelease()

			// mergeBetween's callers only ever ask for [v, v+1), but the two
			// cascades inside it seed independently, so widen the window too
			for _, width := range []uint64{1, 2, 1 << 40} {
				maxExc := value + width
				got, gotRelease := reader.mergeBetween(value, maxExc, conc)
				require.Equalf(t, canonicalBytes(unseededBetween(reader.bitmaps, value, maxExc, conc)),
					canonicalBytes(got),
					"mergeBetween disagrees; seed=%d min=%d(%#016x) maxExc=%d(%#016x) conc=%d",
					seed, value, value, maxExc, maxExc, conc)
				gotRelease()
			}
		}

		for _, operator := range operators {
			layer, layerRelease, err := reader.Read(context.Background(), value, operator)
			require.NoError(t, err)
			require.Equalf(t, canonicalBytes(unseededRead(reader.bitmaps, value, operator)),
				canonicalBytes(layer.Additions),
				"Read disagrees; seed=%d value=%d(%#016x) operator=%s", seed, value, value, operator.Name())
			layerRelease()
		}
	}
}

// canonicalBytes re-serializes into a right-sized arena so the differential
// compares set membership, not arena layout (see
// TestSeededCascadeLeavesADifferentArena).
func canonicalBytes(bm *sroar.Bitmap) []byte {
	return sroar.FromSortedList(bm.ToArray()).ToBuffer()
}

// sroar's flat arena places containers at different offsets depending on the
// cascade's start plane, so raw ToBuffer() differs even when the sets match.
func TestSeededCascadeLeavesADifferentArena(t *testing.T) {
	seg := newCascadeFixture(t, 7)
	readers, release := seg.Readers(roaringset.NewBitmapBufPoolNoop())
	defer release()
	reader := readers[0].(*segmentInMemoryReader)

	value := cascadeEncodeInt64(101)
	want := unseededGreaterThanEqual(reader.bitmaps, value, 1)
	got, gotRelease := reader.mergeGreaterThanEqual(value, 1)
	defer gotRelease()

	require.Equal(t, canonicalBytes(want), canonicalBytes(got))
	require.NotEqual(t, want.ToBuffer(), got.ToBuffer())
}

// unseededGreaterThanEqual is mergeGreaterThanEqual exactly as v1.37 ships it:
// clone plane 0, then let the cascade AND the first set bit's plane away.
func unseededGreaterThanEqual(bitmaps rangeBitmaps, value uint64, conc int) *sroar.Bitmap {
	result := bitmaps[0].Clone()
	ANDed := false

	for bit := 1; bit < len(bitmaps); bit++ {
		if value&(1<<(bit-1)) != 0 {
			result.AndConc(bitmaps[bit], conc)
			ANDed = true
		} else if ANDed {
			result.OrConc(bitmaps[bit], conc)
		}
	}
	return result
}

// unseededBetween is mergeBetween exactly as v1.37 ships it.
func unseededBetween(bitmaps rangeBitmaps, valueMinInc, valueMaxExc uint64, conc int) *sroar.Bitmap {
	resultMin := bitmaps[0].Clone()
	resultMax := bitmaps[0].Clone()
	ANDedMin := false
	ANDedMax := false

	for bit := 1; bit < len(bitmaps); bit++ {
		var b uint64 = 1 << (bit - 1)

		if valueMinInc&b != 0 {
			resultMin.AndConc(bitmaps[bit], conc)
			ANDedMin = true
		} else if ANDedMin {
			resultMin.OrConc(bitmaps[bit], conc)
		}

		if valueMaxExc&b != 0 {
			resultMax.AndConc(bitmaps[bit], conc)
			ANDedMax = true
		} else if ANDedMax {
			resultMax.OrConc(bitmaps[bit], conc)
		}
	}

	return resultMin.AndNotConc(resultMax, conc)
}

// unseededRead mirrors the operator dispatch on top of the unseeded cascades,
// so the differential covers the AndNot wrappers as well as the cascades.
func unseededRead(bitmaps rangeBitmaps, value uint64, operator filters.Operator) *sroar.Bitmap {
	conc := 1

	switch operator {
	case filters.OperatorEqual:
		switch value {
		case 0:
			return unseededRead(bitmaps, value, filters.OperatorLessThanEqual)
		case math.MaxUint64:
			return unseededRead(bitmaps, value, filters.OperatorGreaterThanEqual)
		}
		return unseededBetween(bitmaps, value, value+1, conc)

	case filters.OperatorNotEqual:
		switch value {
		case 0:
			return unseededRead(bitmaps, value, filters.OperatorGreaterThan)
		case math.MaxUint64:
			return unseededRead(bitmaps, value, filters.OperatorLessThan)
		}
		neq := bitmaps[0].Clone()
		return neq.AndNotConc(unseededBetween(bitmaps, value, value+1, conc), conc)

	case filters.OperatorLessThan:
		if value == 0 {
			return sroar.NewBitmap()
		}
		lt := bitmaps[0].Clone()
		return lt.AndNotConc(unseededGreaterThanEqual(bitmaps, value, conc), conc)

	case filters.OperatorLessThanEqual:
		if value == math.MaxUint64 {
			return bitmaps[0].Clone()
		}
		lte := bitmaps[0].Clone()
		return lte.AndNotConc(unseededGreaterThanEqual(bitmaps, value+1, conc), conc)

	case filters.OperatorGreaterThan:
		if value == math.MaxUint64 {
			return sroar.NewBitmap()
		}
		return unseededGreaterThanEqual(bitmaps, value+1, conc)

	case filters.OperatorGreaterThanEqual:
		if value == 0 {
			return bitmaps[0].Clone()
		}
		return unseededGreaterThanEqual(bitmaps, value, conc)

	default:
		panic(fmt.Sprintf("unsupported operator %v", operator))
	}
}

// -----------------------------------------------------------------------------

// newCascadeFixture writes planes via the three production paths (merged
// memtable, flushed segment, compacted segment), asserting the subset
// invariant after each.
func newCascadeFixture(t *testing.T, seed int64) *SegmentInMemory {
	t.Helper()

	logger, _ := test.NewNullLogger()
	rng := rand.New(rand.NewSource(seed))
	seg := NewSegmentInMemory(logger)

	for round := 0; round < 5; round++ {
		switch rng.Intn(3) {
		case 0:
			seg.MergeMemtableEventually(cascadeRandomMemtable(t, rng))
			waitUntilMemtablesMerged(t, seg)

		case 1:
			data := cascadeSegmentBytes(t, cascadeRandomMemtable(t, rng))
			require.NoError(t, seg.MergeSegmentByCursor(NewSegmentCursorMmap(data)))

		case 2:
			left := cascadeSegmentBytes(t, cascadeRandomMemtable(t, rng))
			right := cascadeSegmentBytes(t, cascadeRandomMemtable(t, rng))
			require.NoError(t, seg.MergeSegmentByCursor(
				NewSegmentCursorMmap(cascadeCompact(t, rng, left, right))))
		}

		requirePlanesAreSubsetsOfPlaneZero(t, seg, seed, round)
	}

	// without this the differential could pass over 65 empty planes
	require.False(t, seg.bitmaps[0].IsEmpty(), "fixture built an empty segment; seed=%d", seed)
	populated := 0
	for plane := 1; plane < len(seg.bitmaps); plane++ {
		if !seg.bitmaps[plane].IsEmpty() {
			populated++
		}
	}
	require.Greaterf(t, populated, 32, "fixture left too few populated planes; seed=%d", seed)

	return seg
}

func requirePlanesAreSubsetsOfPlaneZero(t *testing.T, seg *SegmentInMemory, seed int64, round int) {
	t.Helper()

	for plane := 1; plane < len(seg.bitmaps); plane++ {
		outside := seg.bitmaps[plane].Clone()
		outside.AndNot(seg.bitmaps[0])
		require.Truef(t, outside.IsEmpty(),
			"plane %d escapes plane 0 (seed=%d round=%d, %d stray docs)",
			plane, seed, round, outside.GetCardinality())
	}
}

func cascadeRandomMemtable(t *testing.T, rng *rand.Rand) *Memtable {
	t.Helper()

	logger, _ := test.NewNullLogger()
	mt := NewMemtable(logger)

	// a narrow docID space so later rounds genuinely overwrite and delete
	for i := 0; i < 300; i++ {
		docID := uint64(rng.Intn(400))
		if rng.Intn(6) == 0 {
			mt.Delete(0, []uint64{docID})
			continue
		}
		mt.Insert(cascadeRandomValue(rng), []uint64{docID})
	}
	return mt
}

// cascadeRandomValue mixes dense and sparse bit patterns so the cascade's first
// set bit lands anywhere from bit 0 to bit 63.
func cascadeRandomValue(rng *rand.Rand) uint64 {
	switch rng.Intn(5) {
	case 0:
		return 0
	case 1:
		return math.MaxUint64
	case 2:
		return 1 << uint(rng.Intn(64))
	case 3:
		return cascadeEncodeInt64(rng.Int63n(1<<20) - (1 << 19))
	default:
		return rng.Uint64()
	}
}

func cascadeSegmentBytes(t *testing.T, mt *Memtable) []byte {
	t.Helper()

	out := []byte{}
	for _, node := range mt.Nodes() {
		sn, err := NewSegmentNode(node.Key, node.Additions, node.Deletions)
		require.NoError(t, err)
		out = append(out, sn.ToBuffer()...)
	}
	return out
}

func cascadeCompact(t *testing.T, rng *rand.Rand, left, right []byte) []byte {
	t.Helper()

	path := filepath.Join(t.TempDir(), fmt.Sprintf("compacted-%d.db", time.Now().UnixNano()))
	f, err := os.Create(path)
	require.NoError(t, err)

	c := NewCompactor(f, NewSegmentCursorMmap(left), NewSegmentCursorMmap(right),
		1, rng.Intn(2) == 0, false, int64(len(left)+len(right))+segmentindex.HeaderSize)
	require.NoError(t, c.Do(context.Background()))
	require.NoError(t, f.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	header, err := segmentindex.ParseHeader(data[:segmentindex.HeaderSize])
	require.NoError(t, err)
	return data[segmentindex.HeaderSize:header.IndexStart]
}
