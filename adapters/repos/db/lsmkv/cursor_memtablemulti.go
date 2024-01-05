//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (c *CursorSet) first() ([]byte, []value, error) {
	k, v := c.First()
	value := newSetEncoder().Do(v)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, value, nil
}

func (c *CursorSet) seek(key []byte) ([]byte, []value, error) {
	k, v := c.Seek(key)
	value := newSetEncoder().Do(v)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, value, nil
}

func (c *CursorSet) next() ([]byte, []value, error) {
	k, v := c.Next()
	value := newSetEncoder().Do(v)
	return k, value, nil
}

func (c *CursorMap) first() ([]byte, []MapPair, error) {
	k, v := c.First()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	k, v := c.Seek(key)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorMap) next() ([]byte, []MapPair, error) {
	k, v := c.Next()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorReplace) first() ([]byte, []byte, error) {
	k, v := c.First()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorReplace) seek(key []byte) ([]byte, []byte, error) {
	k, v := c.Seek(key)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorReplace) next() ([]byte, []byte, error) {
	k, v := c.Next()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (m *MemtableMulti) newCollectionCursor() innerCursorCollection {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			innerCursorCollection: m.newCollectionCursor(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var cursors []innerCursorCollection
	for _, response := range results {
		cursors = append(cursors, response.innerCursorCollection)
	}
	set := &CursorSet{
		unlock: func() {
			// should be no-op, but there can be some unforeseen edge cases
		},
		innerCursors: cursors,
	}
	return set
}

func (m *MemtableMulti) newRoaringSetCursor() roaringset.InnerCursor {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			innerCursorRoaringSet: m.newRoaringSetCursor(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var cursors []roaringset.InnerCursor
	for _, response := range results {
		cursors = append(cursors, response.innerCursorRoaringSet)
	}
	set := roaringset.NewCombinedCursorLayer(cursors, false)
	return set
}

func (m *MemtableMulti) newMapCursor() innerCursorMap {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			innerCursorMap: m.newMapCursor(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var cursors []innerCursorMap
	for _, response := range results {
		cursors = append(cursors, response.innerCursorMap)
	}
	set := &CursorMap{
		unlock: func() {
			// should be no-op, but there can be some unforeseen edge cases
		},
		innerCursors: cursors,
	}
	return set
}

func (m *MemtableMulti) newCursor() innerCursorReplace {
	memtableOperation := func(id int, m *MemtableSingle) ThreadedMemtableResponse {
		return ThreadedMemtableResponse{
			innerCursorReplace: m.newCursor(),
		}
	}
	results := m.callAllWorkers(memtableOperation, true)
	var cursors []innerCursorReplace
	for _, response := range results {
		cursors = append(cursors, response.innerCursorReplace)
	}
	set := &CursorReplace{
		unlock: func() {
			// should be no-op, but there can be some unforeseen edge cases
		},
		innerCursors: cursors,
	}
	return set
}
