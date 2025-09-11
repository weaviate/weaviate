package lsmkv

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// This test proves two things:
//
// 1. If you have a consistent view of segments, any change of the segment list
// will not affect you.
// 2. Having a consistent view does not block the addition of new segments,
// those can happen concurrently.
func TestSegmentGroup_ConsistentViewAcrossSegmentAddition(t *testing.T) {
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

func TestSegmentGroup_ConsistentViewAcrossSegmentReplace(t *testing.T) {
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

func newFakeReplaceSegment(kv map[string][]byte) *fakeSegment {
	return &fakeSegment{segmentType: StrategyReplace, replaceStore: kv}
}

type fakeSegment struct {
	segmentType  string
	replaceStore map[string][]byte
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

func (f *fakeSegment) newCursor() *segmentCursorReplace {
	panic("not implemented") // TODO: Implement
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
	panic("not implemented") // TODO: Implement
}

func (f *fakeSegment) roaringSetMergeWith(key []byte, input roaringset.BitmapLayer, bitmapBufPool roaringset.BitmapBufPool) error {
	panic("not implemented") // TODO: Implement
}
