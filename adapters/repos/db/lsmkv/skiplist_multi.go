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

// multiIndex is the memtable's ordered key -> []value index for the Set
// strategy, implemented by both the red-black tree (locked reads) and the skip
// list (lock-free reads). insert returns the bytes of index backing newly
// allocated by the call. Value order is insertion order — set decoding relies
// on later tombstones cancelling earlier values.
type multiIndex interface {
	insert(key []byte, values []value) int
	get(key []byte) ([]value, error)
	flattenInOrder() []*binarySearchNodeMulti
}

// shallow size of one value (slice header + bool), i.e. one value-log slot.
var multiValueSize = int(unsafe.Sizeof(value{}))

func newMultiIndex(lockFree bool) multiIndex {
	if lockFree {
		return newSkipListMulti()
	}
	return &binarySearchTreeMulti{}
}

// skipListMulti maps the generic log onto Set semantics directly: the log is
// the per-key value list, in insertion order.
type skipListMulti struct {
	sl *skipList[value]
}

func newSkipListMulti() *skipListMulti {
	return &skipListMulti{sl: newSkipList[value]()}
}

func (m *skipListMulti) insert(key []byte, values []value) int {
	return m.sl.insertMany(key, values) * multiValueSize
}

func (m *skipListMulti) get(key []byte) ([]value, error) {
	vs, ok := m.sl.get(key)
	if !ok {
		return nil, lsmkv.NotFound
	}
	return vs, nil
}

func (m *skipListMulti) flattenInOrder() []*binarySearchNodeMulti {
	var out []*binarySearchNodeMulti
	m.sl.forEach(func(key []byte, values []value) {
		if len(values) == 0 {
			values = nil // the red-black tree's flatten also nils empty values
		}
		out = append(out, &binarySearchNodeMulti{
			key:    key,
			values: values, // fresh per-node snapshot
		})
	})
	return out
}
