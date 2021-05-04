package lsmkv

import (
	"bytes"
)

type memtableCursor struct {
	data    []*binarySearchNode
	current int
}

func (l *Memtable) newCursor() innerCursor {
	// This cursor is a really primitive approach, it actually requires
	// flattening the entire memtable - even if the cursor were to point to the
	// very last element. However, given that the memtable will on average be
	// only half it's max capacity and even that is relatively small, we might
	// get away with the full-flattening and a linear search. Let's not optimize
	// prematurely.

	l.RLock()
	defer l.RUnlock()

	// TODO: support strategies other than replace
	data := l.key.flattenInOrder()

	return &memtableCursor{
		data: data,
	}
}

func (c *memtableCursor) first() ([]byte, []byte, error) {
	if len(c.data) == 0 {
		return nil, nil, NotFound
	}

	c.current = 0

	if c.data[c.current].tombstone {
		return c.data[c.current].key, nil, Deleted
	}
	return c.data[c.current].key, c.data[c.current].value, nil
}

func (c *memtableCursor) seek(key []byte) ([]byte, []byte, error) {
	pos := c.posLargerThanEqual(key)
	if pos == -1 {
		return nil, nil, NotFound
	}

	c.current = pos
	if c.data[c.current].tombstone {
		return c.data[c.current].key, nil, Deleted
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
	c.current++
	if c.current >= len(c.data) {
		return nil, nil, NotFound
	}

	if c.data[c.current].tombstone {
		return c.data[c.current].key, nil, Deleted
	}
	return c.data[c.current].key, c.data[c.current].value, nil
}
