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

	"github.com/weaviate/weaviate/entities/lsmkv"
)

type memtableCursorCollection struct {
	data    []*binarySearchNodeMulti
	current int
	lock    func()
	unlock  func()
}

func (m *Memtable) newCollectionCursor() innerCursorCollection {
	// This cursor is a really primitive approach, it actually requires
	// flattening the entire memtable - even if the cursor were to point to the
	// very last element. However, given that the memtable will on average be
	// only half it's max capacity and even that is relatively small, we might
	// get away with the full-flattening and a linear search. Let's not optimize
	// prematurely.

	m.RLock()
	defer m.RUnlock()

	data := m.keyMulti.flattenInOrder()

	return &memtableCursorCollection{
		data:   data,
		lock:   m.RLock,
		unlock: m.RUnlock,
	}
}

func (c *memtableCursorCollection) first() ([]byte, []value, error) {
	c.lock()
	defer c.unlock()

	if len(c.data) == 0 {
		return nil, nil, lsmkv.NotFound
	}

	c.current = 0

	// there is no key-level tombstone, only individual values can have
	// tombstones
	return c.data[c.current].key, c.data[c.current].values, nil
}

func (c *memtableCursorCollection) seek(key []byte) ([]byte, []value, error) {
	c.lock()
	defer c.unlock()

	pos := c.posLargerThanEqual(key)
	if pos == -1 {
		return nil, nil, lsmkv.NotFound
	}

	c.current = pos
	// there is no key-level tombstone, only individual values can have
	// tombstones
	return c.data[pos].key, c.data[pos].values, nil
}

func (c *memtableCursorCollection) posLargerThanEqual(key []byte) int {
	for i, node := range c.data {
		if bytes.Compare(node.key, key) >= 0 {
			return i
		}
	}

	return -1
}

func (c *memtableCursorCollection) next() ([]byte, []value, error) {
	c.lock()
	defer c.unlock()

	c.current++
	if c.current >= len(c.data) {
		return nil, nil, lsmkv.NotFound
	}

	// there is no key-level tombstone, only individual values can have
	// tombstones
	return c.data[c.current].key, c.data[c.current].values, nil
}
