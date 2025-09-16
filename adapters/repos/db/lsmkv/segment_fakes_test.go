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

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func newFakeReplaceSegment(kv map[string][]byte) *fakeSegment {
	return &fakeSegment{strategy: segmentindex.StrategyReplace, replaceStore: kv}
}

func newFakeRoaringSetSegment(kv map[string]*sroar.Bitmap) *fakeSegment {
	return &fakeSegment{strategy: segmentindex.StrategyRoaringSet, roaringStore: kv}
}

func newFakeSetSegment(kv map[string][][]byte) *fakeSegment {
	store := make(map[string][]value, len(kv))
	for k, v := range kv {
		values := make([]value, 0, len(v))
		for _, single := range v {
			values = append(values, value{value: single})
		}
		store[k] = values
	}
	return &fakeSegment{strategy: segmentindex.StrategySetCollection, collectionStore: store}
}

func newFakeMapSegment(kv map[string][]MapPair) *fakeSegment {
	store := make(map[string][]value, len(kv))
	for k, v := range kv {
		values := make([]value, 0, len(v))
		for _, single := range v {
			mBytes, err := single.Bytes()
			if err != nil {
				panic(err)
			}

			values = append(values, value{value: mBytes})
		}
		store[k] = values
	}
	return &fakeSegment{strategy: segmentindex.StrategyMapCollection, collectionStore: store}
}

type fakeSegment struct {
	strategy        segmentindex.Strategy
	replaceStore    map[string][]byte
	roaringStore    map[string]*sroar.Bitmap
	collectionStore map[string][]value
	refs            int
	path            string
	getCounter      int
}

func (f *fakeSegment) getPath() string {
	return f.path
}

func (f *fakeSegment) setPath(path string) {
	f.path = path
}

func (f *fakeSegment) getStrategy() segmentindex.Strategy {
	return f.strategy
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
	if f.strategy != segmentindex.StrategyReplace {
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
	f.getCounter++

	keyStr := string(key)
	if f.strategy != segmentindex.StrategySetCollection && f.strategy != segmentindex.StrategyMapCollection {
		return nil, fmt.Errorf("not a collection segment")
	}

	if val, ok := f.collectionStore[keyStr]; ok {
		return val, nil
	}

	return nil, lsmkv.NotFound
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

func (f *fakeSegment) newCollectionCursor() innerCursorCollection {
	// Build a stable, sorted key list for deterministic iteration.
	keys := make([]string, 0, len(f.collectionStore))
	for k := range f.collectionStore {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return &fakeCollectionCursor{
		keys:  keys,
		store: f.collectionStore,
		idx:   -1, // before first
	}
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

func (f *fakeSegment) newRoaringSetCursor() roaringset.SegmentCursor {
	if f.strategy != segmentindex.StrategyRoaringSet {
		panic("not a roaring set segment")
	}
	return newFakeRoaringSetCursor(f.roaringStore)
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
	if f.strategy != segmentindex.StrategyRoaringSet {
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

type fakeRoaringSetCursor struct {
	keys   [][]byte
	layer  []roaringset.BitmapLayer // 1:1 with keys
	pos    int
	closed bool
}

func newFakeRoaringSetCursor(store map[string]*sroar.Bitmap) *fakeRoaringSetCursor {
	type kv struct {
		k []byte
		v *sroar.Bitmap
	}
	pairs := make([]kv, 0, len(store))
	for k, v := range store {
		pairs = append(pairs, kv{k: []byte(k), v: v})
	}
	sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })

	c := &fakeRoaringSetCursor{
		keys:  make([][]byte, 0, len(pairs)),
		layer: make([]roaringset.BitmapLayer, 0, len(pairs)),
	}
	for _, p := range pairs {
		c.keys = append(c.keys, append([]byte(nil), p.k...)) // copy for safety
		// additions-only layer; clone so tests can safely Flatten/Or/AndNot
		c.layer = append(c.layer, roaringset.BitmapLayer{
			Additions: p.v.Clone(),
		})
	}
	c.pos = -1
	return c
}

func (c *fakeRoaringSetCursor) Close() { c.closed = true }

func (c *fakeRoaringSetCursor) First() ([]byte, roaringset.BitmapLayer, error) {
	if c.closed {
		return nil, roaringset.BitmapLayer{}, lsmkv.NotFound
	}
	if len(c.keys) == 0 {
		return nil, roaringset.BitmapLayer{}, lsmkv.NotFound
	}
	c.pos = 0
	return c.keys[c.pos], c.layer[c.pos], nil
}

func (c *fakeRoaringSetCursor) Next() ([]byte, roaringset.BitmapLayer, error) {
	if c.closed {
		return nil, roaringset.BitmapLayer{}, lsmkv.NotFound
	}
	c.pos++
	if c.pos < 0 || c.pos >= len(c.keys) {
		return nil, roaringset.BitmapLayer{}, lsmkv.NotFound
	}
	return c.keys[c.pos], c.layer[c.pos], nil
}

func (c *fakeRoaringSetCursor) Seek(seek []byte) ([]byte, roaringset.BitmapLayer, error) {
	panic("not implemented")
}

type fakeCollectionCursor struct {
	keys  []string
	store map[string][]value
	idx   int // -1 before first element
}

func (c *fakeCollectionCursor) first() ([]byte, []value, error) {
	if len(c.keys) == 0 {
		c.idx = -1
		return nil, nil, lsmkv.NotFound
	}
	c.idx = 0
	k := c.keys[c.idx]
	return []byte(k), c.store[k], nil
}

func (c *fakeCollectionCursor) next() ([]byte, []value, error) {
	// If iteration hasn't started, first() semantics.
	if c.idx < 0 {
		return c.first()
	}

	c.idx++
	if c.idx >= len(c.keys) {
		return nil, nil, lsmkv.NotFound
	}
	k := c.keys[c.idx]
	return []byte(k), c.store[k], nil
}

func (c *fakeCollectionCursor) seek(target []byte) ([]byte, []value, error) {
	panic("not implemented")
}
