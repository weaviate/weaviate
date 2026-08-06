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
	"unsafe"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// mapIndex is the memtable's ordered rowKey -> []MapPair index, implemented by
// both the red-black tree (locked reads) and the skip list (lock-free reads).
// insert returns the bytes of index backing newly allocated by the call, so the
// memtable can keep its flush-size budget honest about that overhead.
type mapIndex interface {
	insert(key []byte, pair MapPair) int
	get(key []byte) ([]MapPair, error)
	flattenInOrder() []*binarySearchNodeMap
}

// var, not const, so benchmarks can A/B it; captured per memtable at construction.
var useSkipListMemtable = true

// shallow size of one MapPair (two slice headers + bool), i.e. one value-log slot.
var mapPairSize = int(unsafe.Sizeof(MapPair{}))

func newMapIndex(lockFree bool) mapIndex {
	if lockFree {
		return newSkipListMap()
	}
	return &binarySearchTreeMap{}
}

// skipListMap reduces the generic log to Map/Inverted semantics via
// sortAndDedupValues (sort by MapPair.Key, keep the last per key).
type skipListMap struct {
	sl *skipList[MapPair]
}

func newSkipListMap() *skipListMap {
	return &skipListMap{sl: newSkipList[MapPair]()}
}

func (m *skipListMap) insert(key []byte, pair MapPair) int {
	return m.sl.insert(key, pair) * mapPairSize
}

func (m *skipListMap) get(key []byte) ([]MapPair, error) {
	raw, ok := m.sl.get(key)
	if !ok {
		return nil, lsmkv.NotFound
	}
	// raw is a fresh snapshot owned by this call and never aliased to the log, so
	// it can be sorted/deduped in place — no second copy.
	return sortAndDedupValuesInPlace(raw), nil
}

func (m *skipListMap) flattenInOrder() []*binarySearchNodeMap {
	var out []*binarySearchNodeMap
	m.sl.forEach(func(key []byte, values []MapPair) {
		out = append(out, &binarySearchNodeMap{
			key:    key,
			values: sortAndDedupValuesInPlace(values), // values is a fresh per-node snapshot
		})
	})
	return out
}
