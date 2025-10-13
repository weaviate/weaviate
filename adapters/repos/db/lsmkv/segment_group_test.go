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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/filters"
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
		strategy:         StrategyReplace,
		segments:         []Segment{newFakeReplaceSegment(segmentData)},
		segmentsWithRefs: map[string]Segment{},
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
		logger:           logger,
		strategy:         StrategyReplace,
		segments:         []Segment{segA, segB},
		segmentsWithRefs: map[string]Segment{},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	validateView := func(t *testing.T, segments []Segment) {
		v, err := sg.getWithSegmentList([]byte("key1"), segments)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v, "k==v on initial state")
		v, err = sg.getWithSegmentList([]byte("key2"), segments)
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), v, "k==v on initial state")

		n := sg.countWithSegmentList(segments)
		require.Equal(t, 2, n)
	}
	validateView(t, segments)

	// prep compacted segment
	segAB := newFakeReplaceSegment(map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	})

	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory()
	require.Equal(t, []Segment{segAB}, sg.segments)

	// prove that our consistent view still works
	validateView(t, segments)
	require.Equal(t, 0, segAB.getCounter, "new segment should not have received call")

	// prove that a new view also works
	segments, release = sg.getConsistentViewOfSegments()
	defer release()
	validateView(t, segments)
	require.Greater(t, segAB.getCounter, 0, "new segment should have received call")
}

func TestSegmentGroup_RoaringSet_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	// initial segment
	segmentData := map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
	}
	sg := &SegmentGroup{
		segments:         []Segment{newFakeRoaringSetSegment(segmentData)},
		segmentsWithRefs: map[string]Segment{},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()
	v, _, err := sg.roaringSetGet([]byte("key1"), segments)
	require.NoError(t, err)
	expected := []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "k==v on initial state")

	// append new segment
	segment2Data := map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{2}),
	}
	sg.addInitializedSegment(newFakeRoaringSetSegment(segment2Data))

	// prove that our consistent view still shows the old value
	v, _, err = sg.roaringSetGet([]byte("key1"), segments)
	require.NoError(t, err)
	expected = []uint64{1}
	require.Equal(t, expected, v.Flatten(true).ToArray(), "k==v after segment addition on old view")

	// prove that new readers will see the most recent view
	segments, release = sg.getConsistentViewOfSegments()
	defer release()
	v, _, err = sg.roaringSetGet([]byte("key1"), segments)
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
		logger:           logger,
		segments:         []Segment{segA, segB},
		segmentsWithRefs: map[string]Segment{},
	}

	// control: take a consistent view before any switch
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	// On the original view, key1 should be {1} (from segA), key2 should be {2} (from segB)
	validateView := func(t *testing.T, segments []Segment) {
		v, _, err := sg.roaringSetGet([]byte("key1"), segments)
		require.NoError(t, err)
		expected := []uint64{1}
		require.Equal(t, expected, v.Flatten(true).ToArray(), "key1 on initial state")

		v, _, err = sg.roaringSetGet([]byte("key2"), segments)
		require.NoError(t, err)
		expected = []uint64{2}
		require.Equal(t, expected, v.Flatten(true).ToArray(), "key2 on initial state")
	}
	validateView(t, segments)

	// prepare compacted segment that merges segA and segB
	segAB := newFakeRoaringSetSegment(map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{1}),
		"key2": bitmapFromSlice([]uint64{2}),
	})

	// perform in-memory switch (like compaction)
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory()
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should contain only the compacted segment")

	// prove that the old consistent view is still valid (reads must not be affected)
	validateView(t, segments)
	require.Equal(t, segAB.getCounter, 0, "new segment should not have received calls")

	// prove that a new consistent view sees the (compacted) latest state
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	validateView(t, segments)
	require.Greater(t, segAB.getCounter, 0, "new segment should have received calls")
}

func TestSegmentGroup_RoaringSetRange_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	key1 := uint64(1)

	// initial segment
	segmentData := map[uint64]*sroar.Bitmap{
		key1: roaringset.NewBitmap(1),
	}
	sg := &SegmentGroup{
		segments:         []Segment{newFakeRoaringSetRangeSegment(segmentData, sroar.NewBitmap())},
		segmentsWithRefs: map[string]Segment{},
	}

	createReaderFromConsistentViewOfSegments := func() ReaderRoaringSetRange {
		segments, release := sg.getConsistentViewOfSegments()
		readers := make([]roaringsetrange.InnerReader, len(segments))
		for i := range segments {
			readers[i] = segments[i].newRoaringSetRangeReader()
		}
		return roaringsetrange.NewCombinedReader(readers, release, 1, logger)
	}

	// control before segment changes
	reader := createReaderFromConsistentViewOfSegments()
	defer reader.Close()

	v, release, err := reader.Read(context.Background(), key1, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, v.ToArray(), "k==v on initial state")
	release()

	// append new segment
	segment2Data := map[uint64]*sroar.Bitmap{
		key1: roaringset.NewBitmap(2),
	}
	sg.addInitializedSegment(newFakeRoaringSetRangeSegment(segment2Data, sroar.NewBitmap()))

	// prove that our consistent view still shows the old value
	v, release, err = reader.Read(context.Background(), key1, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, v.ToArray(), "k==v after segment addition on old view")
	release()

	// prove that new readers will see the most recent view
	reader2 := createReaderFromConsistentViewOfSegments()
	defer reader2.Close()

	v, release, err = reader2.Read(context.Background(), key1, filters.OperatorEqual)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, v.ToArray(), "k==v on new view after segment addition")
	release()
}

func TestSegmentGroup_RoaringSetRange_ConsistentViewAcrossSegmentSwitch(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	key1, key2 := uint64(1), uint64(2)

	// initial two segments: segA (key1 -> {1}), segB (key2 -> {2})
	segA := newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
		key1: roaringset.NewBitmap(1),
	}, sroar.NewBitmap())
	segB := newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
		key2: roaringset.NewBitmap(2),
	}, sroar.NewBitmap())
	sg := &SegmentGroup{
		logger:           logger,
		segments:         []Segment{segA, segB},
		segmentsWithRefs: map[string]Segment{},
	}

	createReaderFromConsistentViewOfSegments := func() ReaderRoaringSetRange {
		segments, release := sg.getConsistentViewOfSegments()
		readers := make([]roaringsetrange.InnerReader, len(segments))
		for i := range segments {
			readers[i] = segments[i].newRoaringSetRangeReader()
		}
		return roaringsetrange.NewCombinedReader(readers, release, 1, logger)
	}

	// control: take a consistent view before any switch
	reader := createReaderFromConsistentViewOfSegments()
	defer reader.Close()

	// On the original view, key1 should be {1} (from segA), key2 should be {2} (from segB)
	validateReader := func(t *testing.T, reader ReaderRoaringSetRange) {
		t.Helper()

		v, release, err := reader.Read(context.Background(), key1, filters.OperatorEqual)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, v.ToArray())
		release()

		v, release, err = reader.Read(context.Background(), key2, filters.OperatorEqual)
		require.NoError(t, err)
		require.Equal(t, []uint64{2}, v.ToArray())
		release()
	}
	validateReader(t, reader)

	// prepare compacted segment that merges segA and segB
	segAB := newFakeRoaringSetRangeSegment(map[uint64]*sroar.Bitmap{
		key1: roaringset.NewBitmap(1),
		key2: roaringset.NewBitmap(2),
	}, sroar.NewBitmap())

	// perform in-memory switch (like compaction)
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory()
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should contain only the compacted segment")

	// prove that the old consistent view is still valid (reads must not be affected)
	validateReader(t, reader)
	require.Equal(t, segAB.getCounter, 0, "new segment should not have received calls")
	countA := segA.getCounter
	countB := segB.getCounter

	// prove that a new consistent view sees the (compacted) latest state
	reader2 := createReaderFromConsistentViewOfSegments()
	defer reader2.Close()

	validateReader(t, reader2)
	require.Greater(t, segAB.getCounter, 0, "new segment should have received calls")
	require.Equal(t, countA, segA.getCounter, "old segmentA should not have received calls")
	require.Equal(t, countB, segB.getCounter, "old segmentB should not have received calls")
}

func TestSegmentGroup_Set_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	// initial segment
	segmentData := map[string][][]byte{
		"key1": {[]byte("v1")},
	}
	sg := &SegmentGroup{
		segments:         []Segment{newFakeSetSegment(segmentData)},
		segmentsWithRefs: map[string]Segment{},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	raw, err := sg.getCollection([]byte("key1"), segments)
	require.NoError(t, err)
	got := newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("v1")}, got, "k==v on initial state")

	// append new segment (Set semantics: union/accumulation)
	segment2Data := map[string][][]byte{
		"key1": {[]byte("v2")},
	}
	sg.addInitializedSegment(newFakeSetSegment(segment2Data))

	// old view remains unchanged
	raw, err = sg.getCollection([]byte("key1"), segments)
	require.NoError(t, err)
	got = newSetDecoder().Do(raw)
	require.ElementsMatch(t, [][]byte{[]byte("v1")}, got, "old view unchanged after segment addition")

	// new readers see union from both segments
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	raw, err = sg.getCollection([]byte("key1"), segments)
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
		logger:           logger,
		segments:         []Segment{segA, segB},
		segmentsWithRefs: map[string]Segment{},
	}

	// take a consistent view before switch
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	// old view should read from segA/segB as expected
	validateView := func(t *testing.T, segments []Segment) {
		raw, err := sg.getCollection([]byte("key1"), segments)
		require.NoError(t, err)
		got := newSetDecoder().Do(raw)
		require.ElementsMatch(t, [][]byte{[]byte("a1")}, got, "key1 on initial view")

		raw, err = sg.getCollection([]byte("key2"), segments)
		require.NoError(t, err)
		got = newSetDecoder().Do(raw)
		require.ElementsMatch(t, [][]byte{[]byte("b2")}, got, "key2 on initial view")
	}
	validateView(t, segments)

	// compacted segment containing both keys
	segAB := newFakeSetSegment(map[string][][]byte{
		"key1": {[]byte("a1")},
		"key2": {[]byte("b2")},
	})

	// perform in-memory switch
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory()
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should now be the compacted one")

	// prove the old view still works (must not require segAB)
	validateView(t, segments)
	require.Equal(t, 0, segAB.getCounter, "new segment should not have received calls on old view")

	// new consistent view should hit segAB
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	validateView(t, segments)
	require.Greater(t, segAB.getCounter, 0, "compacted segment should have received calls on new view")
}

func TestSegmentGroup_Map_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()

	// initial segment
	segmentData := map[string][]MapPair{
		"key1": {{Key: []byte("k1"), Value: []byte("v1")}},
	}
	sg := &SegmentGroup{
		segments:         []Segment{newFakeMapSegment(segmentData)},
		segmentsWithRefs: map[string]Segment{},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	raw, err := sg.getCollection([]byte("key1"), segments)
	require.NoError(t, err)
	got, err := newMapDecoder().Do(raw, false)
	require.NoError(t, err)
	expected := []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	require.ElementsMatch(t, expected, got, "k==v on initial state")

	// append new segment (Map semantics: union/accumulation)
	segment2Data := map[string][]MapPair{
		"key1": {{Key: []byte("k2"), Value: []byte("v2")}},
	}
	sg.addInitializedSegment(newFakeMapSegment(segment2Data))

	// old view remains unchanged
	raw, err = sg.getCollection([]byte("key1"), segments)
	require.NoError(t, err)
	got, err = newMapDecoder().Do(raw, false)
	require.NoError(t, err)

	expected = []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	require.ElementsMatch(t, expected, got, "old view unchanged after segment addition")

	// new readers see union from both segments
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	raw, err = sg.getCollection([]byte("key1"), segments)
	require.NoError(t, err)
	got, err = newMapDecoder().Do(raw, false)
	require.NoError(t, err)
	require.NoError(t, err)

	expected = []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	require.ElementsMatch(t, expected, got, "new readers see both pairs")
}

func TestSegmentGroup_Map_ConsistentViewAcrossSegmentSwitch(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	// initial two segments
	segA := newFakeMapSegment(map[string][]MapPair{
		"key1": {{Key: []byte("ka1"), Value: []byte("va1")}},
	})
	segB := newFakeMapSegment(map[string][]MapPair{
		"key2": {{Key: []byte("kb1"), Value: []byte("vb1")}},
	})

	sg := &SegmentGroup{
		logger:           logger,
		segments:         []Segment{segA, segB},
		segmentsWithRefs: map[string]Segment{},
	}

	// take a consistent view before switch
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	validateView := func(t *testing.T, segments []Segment) {
		raw, err := sg.getCollection([]byte("key1"), segments)
		require.NoError(t, err)
		got, err := newMapDecoder().Do(raw, false)
		require.NoError(t, err)
		expected := []MapPair{{Key: []byte("ka1"), Value: []byte("va1")}}
		require.ElementsMatch(t, expected, got, "key1 on initial view")

		raw, err = sg.getCollection([]byte("key2"), segments)
		require.NoError(t, err)
		got, err = newMapDecoder().Do(raw, false)
		require.NoError(t, err)
		expected = []MapPair{{Key: []byte("kb1"), Value: []byte("vb1")}}
		require.ElementsMatch(t, expected, got, "key2 on initial view")
	}
	validateView(t, segments)

	// compacted segment containing both keys
	segAB := newFakeMapSegment(map[string][]MapPair{
		"key1": {{Key: []byte("ka1"), Value: []byte("va1")}},
		"key2": {{Key: []byte("kb1"), Value: []byte("vb1")}},
	})

	// perform in-memory switch
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory()
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should now be the compacted one")

	// prove the old view still works (must not require segAB)
	validateView(t, segments)

	require.Equal(t, 0, segAB.getCounter, "new segment should not have received calls on old view")

	// new consistent view should hit segAB
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	validateView(t, segments)
	require.Greater(t, segAB.getCounter, 0, "compacted segment should have received calls on new view")
}

func TestSegmentGroup_Inverted_ConsistentViewAcrossSegmentAddition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// initial segment
	segmentData := map[string][]MapPair{
		"key1": {NewMapPairFromDocIdAndTf(0, 2, 1, false)},
	}
	sg := &SegmentGroup{
		segments:         []Segment{newFakeInvertedSegment(segmentData)},
		segmentsWithRefs: map[string]Segment{},
	}

	// control before segment changes
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	require.NoError(t, validateMapPairListVsBlockMaxSearchFromSingleSegment(t, ctx, segments[0], []kv{
		{key: []byte("key1"), values: []MapPair{NewMapPairFromDocIdAndTf(0, 2, 1, false)}},
	}))

	// append new segment (Inverted semantics: union/accumulation)
	segment2Data := map[string][]MapPair{
		"key1": {NewMapPairFromDocIdAndTf(1, 2, 1, false)},
	}
	sg.addInitializedSegment(newFakeInvertedSegment(segment2Data))

	// old view remains unchanged
	require.NoError(t, validateMapPairListVsBlockMaxSearchFromSingleSegment(t, ctx, segments[0], []kv{
		{key: []byte("key1"), values: []MapPair{NewMapPairFromDocIdAndTf(0, 2, 1, false)}},
	}))

	// new readers see union from both segments
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	require.NoError(t, validateMapPairListVsBlockMaxSearchFromSegments(t, ctx, segments, []kv{
		{key: []byte("key1"), values: []MapPair{
			NewMapPairFromDocIdAndTf(0, 2, 1, false),
			NewMapPairFromDocIdAndTf(1, 2, 1, false),
		}},
	}))
}

func TestSegmentGroup_Inverted_ConsistentViewAcrossSegmentSwitch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	logger, _ := test.NewNullLogger()

	// initial two segments
	segA := newFakeInvertedSegment(map[string][]MapPair{
		"key1": {NewMapPairFromDocIdAndTf(0, 2, 1, false)},
	})
	segB := newFakeInvertedSegment(map[string][]MapPair{
		"key1": {NewMapPairFromDocIdAndTf(1, 2, 1, false)},
	})

	sg := &SegmentGroup{
		logger:           logger,
		segments:         []Segment{segA, segB},
		segmentsWithRefs: map[string]Segment{},
	}

	// take a consistent view before switch
	segments, release := sg.getConsistentViewOfSegments()
	defer release()

	validateView := func(t *testing.T, segments []Segment) {
		require.NoError(t, validateMapPairListVsBlockMaxSearchFromSegments(t, ctx, segments, []kv{
			{key: []byte("key1"), values: []MapPair{
				NewMapPairFromDocIdAndTf(0, 2, 1, false),
				NewMapPairFromDocIdAndTf(1, 2, 1, false),
			}},
		}))
	}
	validateView(t, segments)

	// compacted segment containing both keys
	segAB := newFakeInvertedSegment(map[string][]MapPair{
		"key1": {NewMapPairFromDocIdAndTf(0, 2, 1, false), NewMapPairFromDocIdAndTf(1, 2, 1, false)},
	})

	// perform in-memory switch
	newSegmentReplacer(sg, 0, 1, segAB).switchInMemory()
	require.Equal(t, []Segment{segAB}, sg.segments, "segment list should now be the compacted one")

	// prove the old view still works (must not require segAB)
	validateView(t, segments)

	require.Equal(t, 0, segAB.getCounter, "new segment should not have received calls on old view")

	// new consistent view should hit segAB
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	validateView(t, segments)
	require.Greater(t, segAB.getCounter, 0, "compacted segment should have received calls on new view")
}
