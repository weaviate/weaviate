package lsmkv

import (
	"bytes"

	"github.com/pkg/errors"
)

type CursorSet struct {
	innerCursors []innerCursorCollection
	state        []cursorStateCollection
}

type innerCursorCollection interface {
	first() ([]byte, []value, error)
	next() ([]byte, []value, error)
	seek([]byte) ([]byte, []value, error)
}

type cursorStateCollection struct {
	key   []byte
	value []value
	err   error
}

func (b *Bucket) SetCursor() *CursorSet {
	return &CursorSet{
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: append(
			b.disk.newCollectionCursors(), b.active.newCollectionCursor()),
	}
}

func (c *CursorSet) Seek(key []byte) ([]byte, [][]byte) {
	c.seekAll(key)
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorSet) Next() ([]byte, [][]byte) {
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorSet) First() ([]byte, [][]byte) {
	c.firstAll()
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorSet) seekAll(target []byte) {
	state := make([]cursorStateCollection, len(c.innerCursors))
	for i, cur := range c.innerCursors {
		key, value, err := cur.seek(target)
		if err == NotFound {
			state[i].err = err
			continue
		}

		if err != nil {
			panic(errors.Wrap(err, "unexpected error in seek"))
		}

		state[i].key = key
		state[i].value = value
	}

	c.state = state
}

func (c *CursorSet) firstAll() {
	state := make([]cursorStateCollection, len(c.innerCursors))
	for i, cur := range c.innerCursors {
		key, value, err := cur.first()
		if err == NotFound {
			state[i].err = err
			continue
		}

		if err != nil {
			panic(errors.Wrap(err, "unexpected error in seek"))
		}

		state[i].key = key
		state[i].value = value
	}

	c.state = state
}

func (c *CursorSet) serveCurrentStateAndAdvance() ([]byte, [][]byte) {
	id, err := c.cursorWithLowestKey()
	if err != nil {
		if err == NotFound {
			return nil, nil
		}
	}

	// check if this is a duplicate key before checking for the remaining errors,
	// as cases such as 'Deleted' can be better handled inside
	// mergeDuplicatesInCurrentStateAndAdvance where we can be sure to act on
	// segments in the correct order
	if ids, ok := c.haveDuplicatesInState(id); ok {
		return c.mergeDuplicatesInCurrentStateAndAdvance(ids)
	} else {
		return c.mergeDuplicatesInCurrentStateAndAdvance([]int{id})
	}
}

func (c *CursorSet) cursorWithLowestKey() (int, error) {
	err := NotFound
	pos := -1
	var lowest []byte

	for i, res := range c.state {
		if res.err == NotFound {
			continue
		}

		if lowest == nil || bytes.Compare(res.key, lowest) <= 0 {
			pos = i
			err = res.err
			lowest = res.key
		}
	}

	if err != nil {
		return pos, err
	}

	return pos, nil
}

func (c *CursorSet) haveDuplicatesInState(idWithLowestKey int) ([]int, bool) {
	key := c.state[idWithLowestKey].key

	var idsFound []int

	for i, cur := range c.state {
		if i == idWithLowestKey {
			idsFound = append(idsFound, i)
			continue
		}

		if bytes.Equal(key, cur.key) {
			idsFound = append(idsFound, i)
		}
	}

	return idsFound, len(idsFound) > 1
}

// if there are no duplicates present it will still work as returning the
// latest result is the same as returning the only result
func (c *CursorSet) mergeDuplicatesInCurrentStateAndAdvance(ids []int) ([]byte, [][]byte) {
	res := c.state[ids[len(ids)-1]]

	for _, id := range ids {
		c.advanceInner(id)
	}

	// if res.err == Deleted {
	// 	// element was deleted, proceed with next round
	// 	return c.Next()
	// }

	// TODO, this is temporary nonsense logic
	values := make([][]byte, len(res.value))
	for i, value := range res.value {
		values[i] = value.value
	}

	return res.key, values
}

func (c *CursorSet) advanceInner(id int) {
	k, v, err := c.innerCursors[id].next()
	if err == NotFound {
		c.state[id].err = err
		c.state[id].key = nil
		c.state[id].value = nil
		return
	}

	if err == Deleted {
		c.state[id].err = err
		c.state[id].key = k
		c.state[id].value = nil
		return
	}

	if err != nil {
		panic(errors.Wrap(err, "unexpected error in advance"))
	}

	c.state[id].key = k
	c.state[id].value = v
	c.state[id].err = nil
}
