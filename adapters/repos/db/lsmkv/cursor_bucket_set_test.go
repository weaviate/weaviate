//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"encoding/binary"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// Previous implementation of cursor called recursively Next() when empty entry occurred,
// which could lead to stack overflow. This test prevents a regression.
func TestSetCursor_StackOverflow(t *testing.T) {
	cursor := &CursorSet{
		unlock:       func() {},
		innerCursors: []innerCursorCollection{&emptyInnerCursorSet{}},
		keyOnly:      false,
	}

	k, vals := cursor.First()
	assert.Nil(t, k)
	assert.Nil(t, vals)
}

type emptyInnerCursorSet struct {
	key uint64
}

func (c *emptyInnerCursorSet) first() ([]byte, []value, error) {
	c.key = 0
	return c.bytes(), []value{}, nil
}

func (c *emptyInnerCursorSet) next() ([]byte, []value, error) {
	if c.key >= 1<<22 {
		return nil, nil, lsmkv.NotFound
	}
	c.key++
	return c.bytes(), []value{}, nil
}

func (c *emptyInnerCursorSet) seek(key []byte) ([]byte, []value, error) {
	return c.first()
}

func (c *emptyInnerCursorSet) bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, c.key)
	return b
}

func TestSetCursorConsistentView(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	// Initial disk: two segments A (key1->{1}) and B (key2->{2})
	diskSegments := &SegmentGroup{
		logger: logger,
		segments: []Segment{
			newFakeSetSegment(map[string][][]byte{
				"key1": {[]byte("value1")},
			}),
			newFakeSetSegment(map[string][][]byte{
				"key2": {[]byte("value2")},
			}),
		},
	}

	// Active memtable contains key3->{3}
	initialMemtable := newTestMemtableSet(map[string][][]byte{
		"key3": {[]byte("value3")},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategySetCollection,
	}

	// Open the cursor that should see key1..key3 only and stay stable
	cur := b.SetCursor()
	validateOriginalCursorView := func(t *testing.T, c *CursorSet) {
		expected := map[string][][]byte{
			"key1": {[]byte("value1")},
			"key2": {[]byte("value2")},
			"key3": {[]byte("value3")},
		}

		actual := map[string][][]byte{}
		for k, vs := c.First(); k != nil; k, vs = c.Next() {
			actual[string(k)] = vs
		}

		require.Equal(t, expected, actual)
	}
	validateOriginalCursorView(t, cur)

	// 2) Switch memtables (new empty active, old active -> flushing)
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableSet(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// Cursor remains stable (still key1..key3)
	validateOriginalCursorView(t, cur)

	// 3) New write to the new active: key4->{4}
	require.NoError(t, b.SetAdd([]byte("key4"), [][]byte{[]byte("value4")}))

	// Cursor must still NOT see key4
	validateOriginalCursorView(t, cur)

	// 4) Flush the flushing memtable (which holds key3) to disk as segment C
	segC := flushSetTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(segC)

	// Cursor remains unchanged
	validateOriginalCursorView(t, cur)

	// 5) Compact disk segments while cursor is open:
	//    initial: A, B, C
	//    compaction #1: A+B, C
	segAB := newFakeSetSegment(map[string][][]byte{
		"key1": {[]byte("value1")},
		"key2": {[]byte("value2")},
	})
	newSegmentReplacer(b.disk, 0, 1, segAB).switchInMemory(b.disk.segments[0], b.disk.segments[1])

	//    compaction #2: (A+B) + C  => ABC
	segABC := newFakeSetSegment(map[string][][]byte{
		"key1": {[]byte("value1")},
		"key2": {[]byte("value2")},
		"key3": {[]byte("value3")},
	})
	newSegmentReplacer(b.disk, 0, 1, segABC).switchInMemory(b.disk.segments[0], b.disk.segments[1])

	// Cursor still sees only key1..key3
	validateOriginalCursorView(t, cur)
	cur.Close()

	// 6) A new cursor now sees the latest state: key1..key4 (key4 is in active)
	cur2 := b.SetCursor()
	defer cur2.Close()

	expected := map[string][][]byte{
		"key1": {[]byte("value1")},
		"key2": {[]byte("value2")},
		"key3": {[]byte("value3")},
		"key4": {[]byte("value4")},
	}

	actual := map[string][][]byte{}
	for k, v := cur2.First(); k != nil; k, v = cur2.Next() {
		actual[string(k)] = v
	}
	require.Equal(t, expected, actual)
}
