//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"bytes"

	"github.com/pkg/errors"
)

type CursorSet struct {
	innerCursors []innerCursorCollection
	state        []cursorStateCollection
	unlock       func()
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

// SetCursor holds a RLock for the flushing state. It needs to be closed using the
// .Close() methods or otherwise the lock will never be relased
func (b *Bucket) SetCursor() *CursorSet {
	b.flushLock.RLock()

	if b.strategy != StrategySetCollection {
		panic("SetCursor() called on strategy other than 'set'")
	}

	innerCursors := b.disk.newCollectionCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newCollectionCursor())
	}

	innerCursors = append(innerCursors, b.active.newCollectionCursor())

	return &CursorSet{
		unlock: b.flushLock.RUnlock,
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: innerCursors,
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

func (c *CursorSet) Close() {
	c.unlock()
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
	// take the key from any of the results, we have the guarantee that they're
	// all the same
	key := c.state[ids[0]].key

	var raw []value
	for _, id := range ids {
		raw = append(raw, c.state[id].value...)
		c.advanceInner(id)
	}

	values := newSetDecoder().Do(raw)
	return key, values
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
