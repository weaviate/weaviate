package lsmkv

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// TestReplaceCursorConsistentView verifies that a cursor opened on a Bucket with
// the "replace" strategy provides a stable, snapshot-like view of the data,
// unaffected by concurrent modifications. The timeline:
//
//  1. Initial state: disk has key1, key2; active memtable has key3. Cursor sees
//     all three.
//  2. Memtable switch: a new (empty) active memtable is installed.
//     - Existing cursor still sees key1–key3 only.
//  3. New write: key4 is written to the new memtable.
//     - Existing cursor remains unchanged, does not see key4.
//  4. Flush: flushing memtable is persisted to disk.
//     - Cursor still sees only its original snapshot.
//  5. Compactions: disk segments are merged (A+B, then A+B+C).
//     - Cursor view remains stable throughout.
//  6. Final state: a *new* cursor sees the full dataset (key1–key4).
//
// In summary, this test proves that cursors maintain a consistent view across
// memtable switches, flushes, and segment compactions, while new cursors see the
// latest state. In addition, cursors do not block any of the operations
// outlined above.
func TestReplaceCursorConsistentView(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	diskSegments := &SegmentGroup{
		logger: logger,
		segments: []Segment{
			newFakeReplaceSegment(map[string][]byte{
				"key1": []byte("value1"),
			}),
			newFakeReplaceSegment(map[string][]byte{
				"key2": []byte("value2"),
			}),
		},
	}

	initialMemtable := newTestMemtableReplace(map[string][]byte{
		"key3": []byte("value3"),
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyReplace,
	}

	cursor := b.Cursor()
	validateOriginalCursorView := func(t *testing.T, c *CursorReplace) {
		expected := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		actual := map[string]string{}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// the string transformation also creates a copy, so we can be sure there
			// is no accidental memory reuse
			actual[string(k)] = string(v)
		}

		require.Equal(t, expected, actual)
	}
	validateOriginalCursorView(t, cursor)

	// switch memtables while the cursor is open
	switched, err := b.atomicallySwitchMemtable(func() (*Memtable, error) {
		return newTestMemtableReplace(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// check that cursor is not affected
	validateOriginalCursorView(t, cursor)

	// write something to the new memtable
	require.NoError(t, b.Put([]byte("key4"), []byte("value4")))

	// check that the cursor still has a consistent view (it should miss the new
	// write)
	validateOriginalCursorView(t, cursor)

	// flush the memtable to disk and validate again
	seg := flushReplaceTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateOriginalCursorView(t, cursor)

	// finally compact all disk segments while the cursor is still open
	// initial state: A, B, C
	// first compaction A+B, C
	// second compaction A+B+C
	segAB := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	})
	newSegmentReplacer(b.disk, 0, 1, segAB).switchInMemory(b.disk.segments[0], b.disk.segments[1])
	segABC := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	})
	newSegmentReplacer(b.disk, 0, 1, segABC).switchInMemory(b.disk.segments[0], b.disk.segments[1])

	// final validation
	validateOriginalCursorView(t, cursor)
	cursor.Close()

	// now open a new cursor and validate it sees everything (including the new
	// write
	cursor2 := b.Cursor()
	defer cursor2.Close()

	expected := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
	}

	actual := map[string]string{}
	for k, v := cursor2.First(); k != nil; k, v = cursor2.Next() {
		// the string transformation also creates a copy, so we can be sure there
		// is no accidental memory reuse
		actual[string(k)] = string(v)
	}
	require.Equal(t, expected, actual)
}
