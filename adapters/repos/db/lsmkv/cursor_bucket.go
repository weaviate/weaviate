package lsmkv

import (
	"bytes"

	"github.com/pkg/errors"
)

type Cursor struct {
	innerCursors []innerCursor
	state        []cursorState
}

type innerCursor interface {
	first() ([]byte, []byte, error)
	next() ([]byte, []byte, error)
	seek([]byte) ([]byte, []byte, error)
}

type cursorState struct {
	key   []byte
	value []byte
	err   error
}

func (b *Bucket) Cursor() *Cursor {
	return &Cursor{
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: append(b.disk.newCursors(), b.active.newCursor()),
	}
}

func (c *Cursor) seekAll(target []byte) {
	state := make([]cursorState, len(c.innerCursors))
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

func (c *Cursor) Seek(key []byte) ([]byte, []byte) {
	c.seekAll(key)

	id, err := c.cursorWithLowestKey()
	if err != nil {
		if err == NotFound {
			return nil, nil
		}

		panic(errors.Wrap(err, "unexpected error in seek"))
	}
	res := c.state[id]

	// only advance the one that delivered a result
	c.advanceInner(id)
	return res.key, res.value
}

func (c *Cursor) cursorWithLowestKey() (int, error) {
	// TODO: what about duplicates? (updates, deletes)
	err := NotFound
	pos := -1
	var lowest []byte

	for i, res := range c.state {
		if res.err == NotFound {
			continue
		}

		if lowest == nil || bytes.Compare(res.key, lowest) < 0 {
			pos = i
			err = nil
			lowest = res.key
		}
	}

	if err != nil {
		return -1, err
	}

	return pos, nil
}

func (c *Cursor) advanceInner(id int) {
	k, v, err := c.innerCursors[id].next()
	if err == NotFound {
		c.state[id].err = err
		c.state[id].key = nil
		c.state[id].value = nil
		return
	}

	if err != nil {
		panic(errors.Wrap(err, "unexpected error in advance"))
	}

	c.state[id].key = k
	c.state[id].value = v
}

func (c *Cursor) Next() ([]byte, []byte) {
	id, err := c.cursorWithLowestKey()
	if err != nil {
		if err == NotFound {
			return nil, nil
		}

		panic(errors.Wrap(err, "unexpected error in next"))
	}
	res := c.state[id]

	// only advance the one that delivered a result
	c.advanceInner(id)
	return res.key, res.value
}

func (c *Cursor) firstAll() {
	state := make([]cursorState, len(c.innerCursors))
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

func (c *Cursor) First() ([]byte, []byte) {
	c.firstAll()

	id, err := c.cursorWithLowestKey()
	if err != nil {
		if err == NotFound {
			return nil, nil
		}

		panic(errors.Wrap(err, "unexpected error in seek"))
	}
	res := c.state[id]

	// only advance the one that delivered a result
	c.advanceInner(id)
	return res.key, res.value
}
