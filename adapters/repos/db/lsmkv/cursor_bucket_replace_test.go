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
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
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
		logger:   nullLogger(),
	}

	cursor := b.Cursor()
	diskCursor := b.CursorOnDisk()
	validateOriginalCursorView := func(t *testing.T, c, cd *CursorReplace) {
		// regular cursor
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

		// disk-only cursor (does not see key3)
		delete(expected, "key3")
		actual = map[string]string{}
		for k, v := cd.First(); k != nil; k, v = cd.Next() {
			// the string transformation also creates a copy, so we can be sure there
			// is no accidental memory reuse
			actual[string(k)] = string(v)
		}
	}
	validateOriginalCursorView(t, cursor, diskCursor)

	// switch memtables while the cursor is open
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableReplace(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// check that cursor is not affected
	validateOriginalCursorView(t, cursor, diskCursor)

	// write something to the new memtable
	require.NoError(t, b.Put([]byte("key4"), []byte("value4")))

	// check that the cursor still has a consistent view (it should miss the new
	// write)
	validateOriginalCursorView(t, cursor, diskCursor)

	// flush the memtable to disk and validate again
	seg := flushReplaceTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(seg)
	validateOriginalCursorView(t, cursor, diskCursor)

	// finally compact all disk segments while the cursor is still open
	// initial state: A, B, C
	// first compaction A+B, C
	// second compaction A+B+C
	segAB := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	})
	newSegmentReplacer(b.disk, 0, 1, segAB).switchInMemory()
	segABC := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	})
	newSegmentReplacer(b.disk, 0, 1, segABC).switchInMemory()

	// final validation
	validateOriginalCursorView(t, cursor, diskCursor)
	cursor.Close()
	diskCursor.Close()

	// now open a new cursor and validate it sees everything (including the new
	// write
	cursor2 := b.Cursor()
	defer cursor2.Close()
	diskCursor2 := b.CursorOnDisk()
	defer diskCursor2.Close()

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

	// disk cursor (does not see key4)
	delete(expected, "key4")
	actual = map[string]string{}
	for k, v := diskCursor2.First(); k != nil; k, v = diskCursor2.Next() {
		// the string transformation also creates a copy, so we can be sure there
		// is no accidental memory reuse
		actual[string(k)] = string(v)
	}
	require.Equal(t, expected, actual)
}

// TestCursorInMemWithTombstones checks CursorInMemWithTombstones yields deleted keys with a literal nil value ("ccc" sorts after live keys to pin nil vs the reused buffer); plain CursorInMem skips them.
func TestCursorInMemWithTombstones(t *testing.T) {
	t.Parallel()

	active := newTestMemtableReplace(map[string][]byte{
		"aaa": []byte("va"),
		"bbb": []byte("vb"),
	})
	active.key.setTombstone([]byte("ccc"), nil, nil)

	b := Bucket{active: active, disk: &SegmentGroup{}, strategy: StrategyReplace, logger: nullLogger()}

	// plain in-mem cursor skips the tombstone
	plain := map[string]string{}
	c := b.CursorInMem()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		plain[string(k)] = string(v)
	}
	c.Close()
	require.Equal(t, map[string]string{"aaa": "va", "bbb": "vb"}, plain)

	// tombstone-emitting cursor surfaces "ccc" with a literal nil value
	seen := map[string]bool{}
	ct := b.CursorInMemWithTombstones()
	for k, v := ct.First(); k != nil; k, v = ct.Next() {
		seen[string(k)] = true
		if string(k) == "ccc" {
			require.Nil(t, v, "tombstone must yield a literal nil value, not a zero-length slice")
		} else {
			require.NotNil(t, v, "live entry must retain its value")
		}
	}
	ct.Close()
	require.Equal(t, map[string]bool{"aaa": true, "bbb": true, "ccc": true}, seen)
}

// TestCursorWithSecondaryIndexOnDiskSegment smoke-tests the one cursor that
// walks a segment by its secondary index, and the only reader of
// diskIndex.Next: secondary keys do not follow payload order, so it cannot scan
// the data section sequentially and takes a fresh index descent per row.
func TestCursorWithSecondaryIndexOnDiskSegment(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	// the secondary order is the reverse of the primary one, so a cursor that
	// walked the payload instead of the secondary index would be visibly wrong
	records := []struct{ primary, secondary string }{
		{"pk-1", "sec-8"},
		{"pk-2", "sec-6"},
		{"pk-3", "sec-4"},
		{"pk-4", "sec-2"},
	}
	for _, r := range records {
		require.NoError(t, b.Put([]byte(r.primary), []byte("value-"+r.primary),
			WithSecondaryKey(0, []byte(r.secondary))))
	}
	require.NoError(t, b.FlushAndSwitch())

	t.Run("walk from the first secondary key", func(t *testing.T) {
		c := b.CursorWithSecondaryIndex(0)
		defer c.Close()

		var seen []string
		for key, value := c.First(); key != nil; key, value = c.Next() {
			seen = append(seen, string(key))
			require.NotEmpty(t, value)
			// a cursor that re-answers the same key would otherwise spin until
			// the package's test budget runs out
			require.LessOrEqual(t, len(seen), len(records), "cursor did not advance")
		}
		require.Equal(t, []string{"sec-2", "sec-4", "sec-6", "sec-8"}, seen)
	})

	t.Run("seek lands on the first key at or above the probe", func(t *testing.T) {
		tests := []struct {
			name    string
			seek    string
			wantKey string
			wantVal string
		}{
			{name: "exact match", seek: "sec-4", wantKey: "sec-4", wantVal: "value-pk-3"},
			{name: "between two keys", seek: "sec-5", wantKey: "sec-6", wantVal: "value-pk-2"},
			{name: "below the smallest", seek: "sec-0", wantKey: "sec-2", wantVal: "value-pk-4"},
			{name: "past the highest", seek: "sec-9"},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := b.CursorWithSecondaryIndex(0)
				defer c.Close()

				key, value := c.Seek([]byte(test.seek))
				if test.wantKey == "" {
					require.Nil(t, key)
					return
				}
				require.Equal(t, test.wantKey, string(key))
				require.Equal(t, test.wantVal, string(value))
			})
		}
	})
}
