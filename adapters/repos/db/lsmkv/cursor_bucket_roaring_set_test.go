package lsmkv

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestRoaringSetCursorConsistentView(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	// Initial disk: two segments A (key1->{1}) and B (key2->{2})
	diskSegments := &SegmentGroup{
		logger: logger,
		segments: []Segment{
			newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
				"key1": bitmapFromSlice([]uint64{1}),
			}),
			newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
				"key2": bitmapFromSlice([]uint64{2}),
			}),
		},
	}

	// Active memtable contains key3->{3}
	initialMemtable := newTestMemtableRoaringSet(map[string][]uint64{
		"key3": {3},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyRoaringSet,
	}

	// Open the cursor that should see key1..key3 only and stay stable
	cur := b.CursorRoaringSet()
	validateOriginalCursorView := func(t *testing.T, c CursorRoaringSet) {
		expected := map[string][]uint64{
			"key1": {1},
			"key2": {2},
			"key3": {3},
		}

		actual := map[string][]uint64{}
		for k, bm := c.First(); k != nil; k, bm = c.Next() {
			// Flatten creates a materialized copy; ToArray returns sorted values.
			actual[string(k)] = bm.ToArray()
		}

		require.Equal(t, expected, actual)
	}
	validateOriginalCursorView(t, cur)

	// 2) Switch memtables (new empty active, old active -> flushing)
	switched, err := b.atomicallySwitchMemtable(func() (*Memtable, error) {
		return newTestMemtableRoaringSet(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// Cursor remains stable (still key1..key3)
	validateOriginalCursorView(t, cur)

	// 3) New write to the new active: key4->{4}
	require.NoError(t, b.RoaringSetAddBitmap([]byte("key4"), bitmapFromSlice([]uint64{4})))

	// Cursor must still NOT see key4
	validateOriginalCursorView(t, cur)

	// 4) Flush the flushing memtable (which holds key3) to disk as segment C
	segC := flushRoaringSetTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(segC)

	// Cursor remains unchanged
	validateOriginalCursorView(t, cur)

	// 5) Compact disk segments while cursor is open:
	//    initial: A, B, C
	//    compaction #1: A+B, C
	segAB := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
		"key2": bitmapFromSlice([]uint64{2}),
	})
	newSegmentReplacer(b.disk, 0, 1, segAB).switchInMemory(b.disk.segments[0], b.disk.segments[1])

	//    compaction #2: (A+B) + C  => ABC
	segABC := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
		"key2": bitmapFromSlice([]uint64{2}),
		"key3": bitmapFromSlice([]uint64{3}),
	})
	newSegmentReplacer(b.disk, 0, 1, segABC).switchInMemory(b.disk.segments[0], b.disk.segments[1])

	// Cursor still sees only key1..key3
	validateOriginalCursorView(t, cur)
	cur.Close()

	// 6) A new cursor now sees the latest state: key1..key4 (key4 is in active)
	cur2 := b.CursorRoaringSet()
	defer cur2.Close()

	expected := map[string][]uint64{
		"key1": {1},
		"key2": {2},
		"key3": {3},
		"key4": {4},
	}

	actual := map[string][]uint64{}
	for k, bm := cur2.First(); k != nil; k, bm = cur2.Next() {
		actual[string(k)] = bm.ToArray()
	}
	require.Equal(t, expected, actual)
}
