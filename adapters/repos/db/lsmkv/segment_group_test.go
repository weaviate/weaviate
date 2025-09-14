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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// This test proves two things:
//
// 1. If you have a consistent view of segments, any change of the segment list
// will not affect you.
// 2. Having a consistent view does not block the addition of new segments,
// those can happen concurrently.
func TestSegmentGroup_Replace_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	// initial segment
	segmentData := map[string][]byte{
		"key1": []byte("value1"),
	}
	sg := &SegmentGroup{
		segments: []Segment{newFakeReplaceSegment(segmentData)},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()
	v, err := sg.getWithSegmentList([]byte("key1"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v, "k==v on initial state")

	// append new segment which overrides our key with a new value
	segment2Data := map[string][]byte{
		"key1": []byte("value2"),
	}
	sg.addInitializedSegment(newFakeReplaceSegment(segment2Data))

	// prove that our consistent view still shows the old value
	v, err = sg.getWithSegmentList([]byte("key1"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v, "k==v on changed state")

	// prove that new readers will see the most recent view
	segments, release = sg.getConsistentViewOfSegments()
	defer release()
	v, err = sg.getWithSegmentList([]byte("key1"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), v, "k==v on initial state")
}

func TestSegmentGroup_Replace_ConsistentViewAcrossSegmentSwitch(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	// initial segment
	segA := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
	})
	segB := newFakeReplaceSegment(map[string][]byte{
		"key2": []byte("value2"),
	})
	sg := &SegmentGroup{
		segments: []Segment{segA, segB},
		logger:   logger,
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	v, err := sg.getWithSegmentList([]byte("key1"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v, "k==v on initial state")
	require.Equal(t, 1, segA.getCounter)
	v, err = sg.getWithSegmentList([]byte("key2"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), v, "k==v on initial state")
	require.Equal(t, 2, segB.getCounter)

	// prep compacted segment
	segAB := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	})

	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory(segA, segB)
	require.Equal(t, []Segment{segAB}, sg.segments)

	// prove that our consistent view still works
	v, err = sg.getWithSegmentList([]byte("key1"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v, "k==v on original view after compaction")
	require.Equal(t, 2, segA.getCounter)
	v, err = sg.getWithSegmentList([]byte("key2"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), v, "k==v on original view after compaction")
	require.Equal(t, 4, segB.getCounter)

	// prove that a new view also works
	segments, release = sg.getConsistentViewOfSegments()
	defer release()
	v, err = sg.getWithSegmentList([]byte("key1"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v, "k==v on new view after compaction")
	require.Equal(t, 2, segA.getCounter, "old segment should not have received call")
	require.Equal(t, 1, segAB.getCounter, "new segment should have received call")
	v, err = sg.getWithSegmentList([]byte("key2"), segments)
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), v, "k==v on new view after compaction")
	require.Equal(t, 4, segB.getCounter, "old segment should not have received call")
	require.Equal(t, 2, segAB.getCounter, "new segment should have received call")
}

func TestSegmentGroup_RoaringSet_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	// initial segment
	segmentData := map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
	}
	sg := &SegmentGroup{
		segments: []Segment{newFakeRoaringSetSegment(segmentData)},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()
	v, _, err := sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected := []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "k==v on initial state")

	// append new segment
	segment2Data := map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{2}),
	}
	sg.addInitializedSegment(newFakeRoaringSetSegment(segment2Data))

	// prove that our consistent view still shows the old value
	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "k==v after segment addition on old view")

	// prove that new readers will see the most recent view
	segments, release = sg.getConsistentViewOfSegments()
	defer release()
	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = []uint64{1, 2}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "k==v on new view after segment addition")
}

func TestSegmentGroup_RoaringSet_ConsistentViewAcrossSegmentSwitch(t *testing.T) {
	t.Parallel()
	logger, _ := test.NewNullLogger()

	// initial two segments: segA (key1 -> {1}), segB (key2 -> {2})
	segA := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
	})
	segB := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"key2": bitmapFromSlice([]uint64{2}),
	})
	sg := &SegmentGroup{
		segments: []Segment{segA, segB},
		logger:   logger,
	}

	// control: take a consistent view before any switch
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	// On the original view, key1 should be {1} (from segA), key2 should be {2} (from segB)
	v, _, err := sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected := []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "key1 on initial state")

	v, _, err = sg.roaringSetGetWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	expected = []uint64{2}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "key2 on initial state")

	// prepare compacted segment that merges segA and segB
	segAB := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
		"key2": bitmapFromSlice([]uint64{2}),
	})

	// perform in-memory switch (like compaction)
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory(segA, segB)
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should contain only the compacted segment")

	// prove that the old consistent view is still valid (reads must not be affected)
	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "key1 on original view after compaction")

	v, _, err = sg.roaringSetGetWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	expected = []uint64{2}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "key2 on original view after compaction")
	require.Equal(t, 0, segAB.getCounter, "new segment should not have received call")

	// prove that a new consistent view sees the (compacted) latest state
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "key1 on new view after compaction")

	v, _, err = sg.roaringSetGetWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	expected = []uint64{2}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "key2 on new view after compaction")
	require.Greater(t, segAB.getCounter, 0, "new segment should have received calls")
}

func TestSegmentGroup_Set_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	// initial segment
	segmentData := map[string][][]byte{
		"key1": {[]byte("v1")},
	}
	sg := &SegmentGroup{
		segments: []Segment{newFakeSetSegment(segmentData)},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	raw, err := sg.getCollectionWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	got := newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("v1")}, got, "k==v on initial state")

	// append new segment (Set semantics: union/accumulation)
	segment2Data := map[string][][]byte{
		"key1": {[]byte("v2")},
	}
	sg.addInitializedSegment(newFakeSetSegment(segment2Data))

	// old view remains unchanged
	raw, err = sg.getCollectionWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("v1")}, got, "old view unchanged after segment addition")

	// new readers see union from both segments
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	raw, err = sg.getCollectionWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("v1"), []byte("v2")}, got, "new view sees accumulated values")
}

func TestSegmentGroup_Set_ConsistentViewAcrossSegmentSwitch(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	// initial two segments
	segA := newFakeSetSegment(map[string][][]byte{
		"key1": {[]byte("a1")},
	})
	segB := newFakeSetSegment(map[string][][]byte{
		"key2": {[]byte("b2")},
	})

	sg := &SegmentGroup{
		segments: []Segment{segA, segB},
		logger:   logger,
	}

	// take a consistent view before switch
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	// old view should read from segA/segB as expected
	raw, err := sg.getCollectionWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	got := newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("a1")}, got, "key1 on initial view")

	raw, err = sg.getCollectionWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("b2")}, got, "key2 on initial view")

	// compacted segment containing both keys
	segAB := newFakeSetSegment(map[string][][]byte{
		"key1": {[]byte("a1")},
		"key2": {[]byte("b2")},
	})

	// perform in-memory switch
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory(segA, segB)
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should now be the compacted one")

	// prove the old view still works (must not require segAB)
	raw, err = sg.getCollectionWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("a1")}, got, "key1 on original view after compaction")

	raw, err = sg.getCollectionWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("b2")}, got, "key2 on original view after compaction")

	require.Equal(t, 0, segAB.getCounter, "new segment should not have received calls on old view")

	// new consistent view should hit segAB
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	raw, err = sg.getCollectionWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("a1")}, got, "key1 on new view after compaction")

	raw, err = sg.getCollectionWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("b2")}, got, "key2 on new view after compaction")

	require.Greater(t, segAB.getCounter, 0, "compacted segment should have received calls on new view")
}
