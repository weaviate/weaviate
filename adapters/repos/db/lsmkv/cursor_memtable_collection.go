//nolint // TODO
package lsmkv

import "bytes"

type memtableCursorCollection struct {
	data    []*binarySearchNodeMulti
	current int
}

func (l *Memtable) newCollectionCursor() innerCursorCollection {
	// This cursor is a really primitive approach, it actually requires
	// flattening the entire memtable - even if the cursor were to point to the
	// very last element. However, given that the memtable will on average be
	// only half it's max capacity and even that is relatively small, we might
	// get away with the full-flattening and a linear search. Let's not optimize
	// prematurely.

	l.RLock()
	defer l.RUnlock()

	data := l.keyMulti.flattenInOrder()

	return &memtableCursorCollection{
		data: data,
	}
}

func (c *memtableCursorCollection) first() ([]byte, []value, error) {
	if len(c.data) == 0 {
		return nil, nil, NotFound
	}

	c.current = 0

	// there is no key-level tombstone, only individual values can have
	// tombstones
	return c.data[c.current].key, c.data[c.current].values, nil
}

func (c *memtableCursorCollection) seek(key []byte) ([]byte, []value, error) {
	pos := c.posLargerThanEqual(key)
	if pos == -1 {
		return nil, nil, NotFound
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
	c.current++
	if c.current >= len(c.data) {
		return nil, nil, NotFound
	}

	// there is no key-level tombstone, only individual values can have
	// tombstones
	return c.data[c.current].key, c.data[c.current].values, nil
}
