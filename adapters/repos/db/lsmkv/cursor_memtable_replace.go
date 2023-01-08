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
	"bytes"

	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/ent"
)

type memtableCursor struct {
	data    []*binarySearchNode
	current int
	lock    func()
	unlock  func()
}

func (l *Memtable) newCursor() innerCursorReplace {
	// This cursor is a really primitive approach, it actually requires
	// flattening the entire memtable - even if the cursor were to point to the
	// very last element. However, given that the memtable will on average be
	// only half it's max capacity and even that is relatively small, we might
	// get away with the full-flattening and a linear search. Let's not optimize
	// prematurely.

	l.RLock()
	defer l.RUnlock()

	data := l.key.flattenInOrder()

	return &memtableCursor{
		data:   data,
		lock:   l.RLock,
		unlock: l.RUnlock,
	}
}

func (c *memtableCursor) first() ([]byte, []byte, error) {
	c.lock()
	defer c.unlock()

	if len(c.data) == 0 {
		return nil, nil, ent.NotFound
	}

	c.current = 0

	if c.data[c.current].tombstone {
		return c.data[c.current].key, nil, ent.Deleted
	}
	return c.data[c.current].key, c.data[c.current].value, nil
}

func (c *memtableCursor) seek(key []byte) ([]byte, []byte, error) {
	c.lock()
	defer c.unlock()

	pos := c.posLargerThanEqual(key)
	if pos == -1 {
		return nil, nil, ent.NotFound
	}

	c.current = pos
	if c.data[c.current].tombstone {
		return c.data[c.current].key, nil, ent.Deleted
	}
	return c.data[pos].key, c.data[pos].value, nil
}

func (c *memtableCursor) posLargerThanEqual(key []byte) int {
	for i, node := range c.data {
		if bytes.Compare(node.key, key) >= 0 {
			return i
		}
	}

	return -1
}

func (c *memtableCursor) next() ([]byte, []byte, error) {
	c.lock()
	defer c.unlock()

	c.current++
	if c.current >= len(c.data) {
		return nil, nil, ent.NotFound
	}

	if c.data[c.current].tombstone {
		return c.data[c.current].key, nil, ent.Deleted
	}
	return c.data[c.current].key, c.data[c.current].value, nil
}
