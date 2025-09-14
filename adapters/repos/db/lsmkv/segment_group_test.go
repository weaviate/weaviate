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
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/lsmkv"
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
	expected := roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{1}),
		},
	}
	require.Equal(t, expected, v, "k==v on initial state")

	// append new segment
	segment2Data := map[string]*sroar.Bitmap{
		"key1": bitmapFromSlice([]uint64{2}),
	}
	sg.addInitializedSegment(newFakeRoaringSetSegment(segment2Data))

	// prove that our consistent view still shows the old value
	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{1}),
		},
	}
	require.Equal(t, expected, v, "k==v after segment addition on old view")

	// prove that new readers will see the most recent view
	segments, release = sg.getConsistentViewOfSegments()
	defer release()
	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{1, 2}),
		},
	}
	require.Equal(t, expected, v, "k==v on new view after segment addition")
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
	expected := roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{1}),
		},
	}
	require.Equal(t, expected, v, "key1 on initial state")

	v, _, err = sg.roaringSetGetWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{2}),
		},
	}
	require.Equal(t, expected, v, "key2 on initial state")

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
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{1}),
		},
	}
	require.Equal(t, expected, v, "key1 on original view after compaction")

	v, _, err = sg.roaringSetGetWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{2}),
		},
	}
	require.Equal(t, expected, v, "key2 on original view after compaction")
	require.Equal(t, 0, segAB.getCounter, "new segment should not have received call")

	// prove that a new consistent view sees the (compacted) latest state
	segments, release = sg.getConsistentViewOfSegments()
	defer release()

	v, _, err = sg.roaringSetGetWithSegments([]byte("key1"), segments)
	require.NoError(t, err)
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{1}),
		},
	}
	require.Equal(t, expected, v, "key1 on new view after compaction")

	v, _, err = sg.roaringSetGetWithSegments([]byte("key2"), segments)
	require.NoError(t, err)
	expected = roaringset.BitmapLayers{
		{
			Additions: bitmapFromSlice([]uint64{2}),
		},
	}
	require.Equal(t, expected, v, "key2 on new view after compaction")
	require.Greater(t, segAB.getCounter, 0, "new segment should have received calls")
}

func newFakeReplaceSegment(kv map[string][]byte) *fakeSegment {
	return &fakeSegment{segmentType: StrategyReplace, replaceStore: kv}
}

func newFakeRoaringSetSegment(kv map[string]*sroar.Bitmap) *fakeSegment {
	return &fakeSegment{segmentType: StrategyRoaringSet, roaringStore: kv}
}

type fakeSegment struct {
	segmentType  string
	replaceStore map[string][]byte
	roaringStore map[string]*sroar.Bitmap
	refs         int
	path         string
	getCounter   int
}

func (f *fakeSegment) getPath() string {
	return f.path
}

func (f *fakeSegment) setPath(path string) {
	f.path = path
}

func (f *fakeSegment) getStrategy() segmentindex.Strategy {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) getSecondaryIndexCount() uint16 {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) getLevel() uint16 {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) getSize() int64 {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) setSize(size int64) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) incRef() {
	f.refs++
}

func (f *fakeSegment) decRef() {
	f.refs--
}

func (f *fakeSegment) getRefs() int {
	return f.refs
}

func (f *fakeSegment) PayloadSize() int {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) close() error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) dropMarked() error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) get(key []byte) ([]byte, error) {
	f.getCounter++

	keyStr := string(key)
	if f.segmentType != StrategyReplace {
		return nil, fmt.Errorf("not a replace segment")
	}

	if val, ok := f.replaceStore[keyStr]; ok {
		return val, nil
	}

	return nil, lsmkv.NotFound
}

func (f *fakeSegment) getBySecondaryIntoMemory(pos int, key []byte, buffer []byte) ([]byte, []byte, []byte, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) getCollection(key []byte) ([]value, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) getInvertedData() *segmentInvertedData {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) getSegment() *segment {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) isLoaded() bool {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) markForDeletion() error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) MergeTombstones(other *sroar.Bitmap) (*sroar.Bitmap, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newCollectionCursor() *segmentCursorCollection {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newCollectionCursorReusable() *segmentCursorCollectionReusable {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newCursor() innerCursorReplaceAllKeys {
	return newFakeSegmentCursorReplace(f.replaceStore)
}

func (f *fakeSegment) newCursorWithSecondaryIndex(pos int) *segmentCursorReplace {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newMapCursor() *segmentCursorMap {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newNodeReader(offset nodeOffset, operation string) (*nodeReader, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newRoaringSetCursor() *roaringset.SegmentCursor {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newRoaringSetRangeCursor() roaringsetrange.SegmentCursor {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) newRoaringSetRangeReader() *roaringsetrange.SegmentReader {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) quantileKeys(q int) [][]byte {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) ReadOnlyTombstones() (*sroar.Bitmap, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) replaceStratParseData(in []byte) ([]byte, []byte, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) roaringSetGet(key []byte, bitmapBufPool roaringset.BitmapBufPool) (roaringset.BitmapLayer, func(), error) {
	f.getCounter++
	if f.segmentType != StrategyRoaringSet {
		return roaringset.BitmapLayer{}, nil, fmt.Errorf("not a roaring set segment")
	}

	if val, ok := f.roaringStore[string(key)]; ok {
		return roaringset.BitmapLayer{
			Additions: val.Clone(),
		}, func() {}, nil
	}

	return roaringset.BitmapLayer{}, nil, lsmkv.NotFound
}

func (f *fakeSegment) roaringSetMergeWith(key []byte, input roaringset.BitmapLayer, bitmapBufPool roaringset.BitmapBufPool) error {
	f.getCounter++
	layer, _, err := f.roaringSetGet(key, bitmapBufPool)
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return nil
		}
	}

	input.Additions.
		AndNotConc(layer.Deletions, concurrency.SROAR_MERGE).
		OrConc(layer.Additions, concurrency.SROAR_MERGE)

	return nil
}

type fakeSegmentCursorReplace struct {
	keys   [][]byte
	values [][]byte
	pos    int
}

func newFakeSegmentCursorReplace(kv map[string][]byte) *fakeSegmentCursorReplace {
	type kvPair struct {
		key   []byte
		value []byte
	}

	pairs := make([]kvPair, 0, len(kv))
	for k, v := range kv {
		pairs = append(pairs, kvPair{key: []byte(k), value: v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].key, pairs[j].key) < 0
	})

	c := &fakeSegmentCursorReplace{
		keys:   make([][]byte, 0, len(kv)),
		values: make([][]byte, 0, len(kv)),
	}

	for _, p := range pairs {
		c.keys = append(c.keys, p.key)
		c.values = append(c.values, p.value)
	}

	return c
}

func (c *fakeSegmentCursorReplace) first() ([]byte, []byte, error) {
	c.pos = 0
	if len(c.keys) == 0 {
		return nil, nil, lsmkv.NotFound
	}

	return c.keys[c.pos], c.values[c.pos], nil
}

func (c *fakeSegmentCursorReplace) seek(key []byte) ([]byte, []byte, error) {
	panic("not implemented")
}

func (c *fakeSegmentCursorReplace) next() ([]byte, []byte, error) {
	c.pos++
	if c.pos >= len(c.keys) {
		return nil, nil, lsmkv.NotFound
	}

	return c.keys[c.pos], c.values[c.pos], nil
}

func (c *fakeSegmentCursorReplace) firstWithAllKeys() (segmentReplaceNode, error) {
	panic("not implemented")
}

func (c *fakeSegmentCursorReplace) nextWithAllKeys() (segmentReplaceNode, error) {
	panic("not implemented")
}

func bitmapFromSlice(input []uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	for _, v := range input {
		bm.Set(v)
	}
	return bm
}
