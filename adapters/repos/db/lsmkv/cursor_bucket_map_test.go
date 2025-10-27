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
	"context"
	"encoding/binary"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// Previous implementation of cursor called recursively Next() when empty entry occurred,
// which could lead to stack overflow. This test prevents a regression.
func TestMapCursor_StackOverflow(t *testing.T) {
	cursor := &CursorMap{
		unlock:       func() {},
		innerCursors: []innerCursorMap{&emptyInnerCursorMap{}},
		listCfg:      MapListOptionConfig{},
		keyOnly:      false,
	}

	k, mp := cursor.First(context.Background())
	assert.Nil(t, k)
	assert.Nil(t, mp)
}

type emptyInnerCursorMap struct {
	key uint64
}

func (c *emptyInnerCursorMap) first() ([]byte, []MapPair, error) {
	c.key = 0
	return c.bytes(), []MapPair{}, nil
}

func (c *emptyInnerCursorMap) next() ([]byte, []MapPair, error) {
	if c.key >= 1<<20 {
		return nil, nil, lsmkv.NotFound
	}
	c.key++
	return c.bytes(), []MapPair{}, nil
}

func (c *emptyInnerCursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	return c.first()
}

func (c *emptyInnerCursorMap) bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, c.key)
	return b
}

func TestMapCursorConsistentView(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()

	// Initial disk: two segments A (key1->{1}) and B (key2->{2})
	diskSegments := &SegmentGroup{
		logger: logger,
		segments: []Segment{
			newFakeMapSegment(map[string][]MapPair{
				"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
			}),
			newFakeMapSegment(map[string][]MapPair{
				"key2": {{Key: []byte("dk2"), Value: []byte("dv2")}},
			}),
		},
		segmentsWithRefs: map[string]Segment{},
	}

	// Active memtable contains key3->{3}
	initialMemtable := newTestMemtableMap(map[string][]MapPair{
		"key3": {{Key: []byte("ak1"), Value: []byte("av1")}},
	})

	b := Bucket{
		active:   initialMemtable,
		disk:     diskSegments,
		strategy: StrategyMapCollection,
	}

	// Open the cursor that should see key1..key3 only and stay stable
	cur := b.MapCursor()
	validateOriginalCursorView := func(t *testing.T, c *CursorMap) {
		expected := map[string][]MapPair{
			"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
			"key2": {{Key: []byte("dk2"), Value: []byte("dv2")}},
			"key3": {{Key: []byte("ak1"), Value: []byte("av1")}},
		}

		actual := map[string][]MapPair{}
		for k, vs := c.First(ctx); k != nil; k, vs = c.Next(ctx) {
			actual[string(k)] = vs
		}

		require.Equal(t, expected, actual)
	}
	validateOriginalCursorView(t, cur)

	// 2) Switch memtables (new empty active, old active -> flushing)
	switched, err := b.atomicallySwitchMemtable(func() (memtable, error) {
		return newTestMemtableMap(nil), nil
	})
	require.NoError(t, err)
	require.True(t, switched)

	// Cursor remains stable (still key1..key3)
	validateOriginalCursorView(t, cur)

	// 3) New write to the new active: key4->{4}
	require.NoError(t, b.MapSet([]byte("key4"), MapPair{
		Key: []byte("ak2"), Value: []byte("av2"),
	}))

	// Cursor must still NOT see key4
	validateOriginalCursorView(t, cur)

	// 4) Flush the flushing memtable (which holds key3) to disk as segment C
	segC := flushMapTestMemtableIntoTestSegment(b.flushing)
	b.atomicallyAddDiskSegmentAndRemoveFlushing(segC)

	// Cursor remains unchanged
	validateOriginalCursorView(t, cur)

	// 5) Compact disk segments while cursor is open:
	//    initial: A, B, C
	//    compaction #1: A+B, C
	segAB := newFakeMapSegment(map[string][]MapPair{
		"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
		"key2": {{Key: []byte("dk2"), Value: []byte("dv2")}},
	})
	newSegmentReplacer(b.disk, 0, 1, segAB).switchInMemory()

	//    compaction #2: (A+B) + C  => ABC
	segABC := newFakeMapSegment(map[string][]MapPair{
		"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
		"key2": {{Key: []byte("dk2"), Value: []byte("dv2")}},
		"key3": {{Key: []byte("ak1"), Value: []byte("av1")}},
	})
	newSegmentReplacer(b.disk, 0, 1, segABC).switchInMemory()

	// Cursor still sees only key1..key3
	validateOriginalCursorView(t, cur)
	cur.Close()

	// 6) A new cursor now sees the latest state: key1..key4 (key4 is in active)
	cur2 := b.MapCursor()
	defer cur2.Close()

	expected := map[string][]MapPair{
		"key1": {{Key: []byte("dk1"), Value: []byte("dv1")}},
		"key2": {{Key: []byte("dk2"), Value: []byte("dv2")}},
		"key3": {{Key: []byte("ak1"), Value: []byte("av1")}},
		"key4": {{Key: []byte("ak2"), Value: []byte("av2")}},
	}

	actual := map[string][]MapPair{}
	for k, v := cur2.First(ctx); k != nil; k, v = cur2.Next(ctx) {
		actual[string(k)] = v
	}
	require.Equal(t, expected, actual)
}
