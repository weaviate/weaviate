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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv/entities"
)

type CursorReplace struct {
	innerCursors []innerCursorReplace
	state        []cursorStateReplace
	unlock       func()
	serveCache   cursorStateReplace
}

type innerCursorReplace interface {
	first() ([]byte, []byte, error)
	next() ([]byte, []byte, error)
	seek([]byte) ([]byte, []byte, error)
}

type cursorStateReplace struct {
	key   []byte
	value []byte
	err   error
}

// Cursor holds a RLock for the flushing state. It needs to be closed using the
// .Close() methods or otherwise the lock will never be relased
func (b *Bucket) Cursor() *CursorReplace {
	b.flushLock.RLock()

	if b.strategy != StrategyReplace {
		panic("Cursor() called on strategy other than 'replace'")
	}

	innerCursors, unlockSegmentGroup := b.disk.newCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newCursor())
	}

	innerCursors = append(innerCursors, b.active.newCursor())

	return &CursorReplace{
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: innerCursors,
		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
	}
}

func (c *CursorReplace) Close() {
	c.unlock()
}

func (c *CursorReplace) seekAll(target []byte) {
	state := make([]cursorStateReplace, len(c.innerCursors))
	for i, cur := range c.innerCursors {
		key, value, err := cur.seek(target)
		if err == entities.NotFound {
			state[i].err = err
			continue
		}

		if err == entities.Deleted {
			state[i].err = err
			state[i].key = key
			continue
		}

		if err != nil {
			panic(errors.Wrap(err, "unexpected error in seek (cursor type 'replace')"))
		}

		state[i].key = key
		state[i].value = value
	}

	c.state = state
}

func (c *CursorReplace) serveCurrentStateAndAdvance() ([]byte, []byte) {
	id, err := c.cursorWithLowestKey()
	if err != nil {
		if err == entities.NotFound {
			return nil, nil
		}
	}

	// check if this is a duplicate key before checking for the remaining errors,
	// as cases such as 'entities.Deleted' can be better handled inside
	// mergeDuplicatesInCurrentStateAndAdvance where we can be sure to act on
	// segments in the correct order
	if ids, ok := c.haveDuplicatesInState(id); ok {
		return c.mergeDuplicatesInCurrentStateAndAdvance(ids)
	} else {
		return c.mergeDuplicatesInCurrentStateAndAdvance([]int{id})
	}
}

func (c *CursorReplace) haveDuplicatesInState(idWithLowestKey int) ([]int, bool) {
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
func (c *CursorReplace) mergeDuplicatesInCurrentStateAndAdvance(ids []int) ([]byte, []byte) {
	c.copyStateIntoServeCache(ids[len(ids)-1])

	// with a replace strategy only the highest will be returned, but still all
	// need to be advanced - or we would just encounter them again in the next
	// round
	for _, id := range ids {
		c.advanceInner(id)
	}

	if c.serveCache.err == entities.Deleted {
		// element was deleted, proceed with next round
		return c.Next()
	}

	return c.serveCache.key, c.serveCache.value
}

func (c *CursorReplace) copyStateIntoServeCache(pos int) {
	resMut := c.state[pos]
	if len(resMut.key) > cap(c.serveCache.key) {
		c.serveCache.key = make([]byte, len(resMut.key))
	} else {
		c.serveCache.key = c.serveCache.key[:len(resMut.key)]
	}

	if len(resMut.value) > cap(c.serveCache.value) {
		c.serveCache.value = make([]byte, len(resMut.value))
	} else {
		c.serveCache.value = c.serveCache.value[:len(resMut.value)]
	}

	copy(c.serveCache.key, resMut.key)
	copy(c.serveCache.value, resMut.value)
	c.serveCache.err = resMut.err
}

func (c *CursorReplace) Seek(key []byte) ([]byte, []byte) {
	c.seekAll(key)
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorReplace) cursorWithLowestKey() (int, error) {
	err := entities.NotFound
	pos := -1
	var lowest []byte

	for i, res := range c.state {
		if res.err == entities.NotFound {
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

func (c *CursorReplace) advanceInner(id int) {
	k, v, err := c.innerCursors[id].next()
	if err == entities.NotFound {
		c.state[id].err = err
		c.state[id].key = nil
		c.state[id].value = nil
		return
	}

	if err == entities.Deleted {
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

func (c *CursorReplace) Next() ([]byte, []byte) {
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorReplace) firstAll() {
	state := make([]cursorStateReplace, len(c.innerCursors))
	for i, cur := range c.innerCursors {
		key, value, err := cur.first()
		if err == entities.NotFound {
			state[i].err = err
			continue
		}
		if err == entities.Deleted {
			state[i].err = err
			state[i].key = key
			continue
		}

		if err != nil {
			panic(errors.Wrap(err, "unexpected error in first (cursor type 'replace')"))
		}

		state[i].key = key
		state[i].value = value
	}

	c.state = state
}

func (c *CursorReplace) First() ([]byte, []byte) {
	c.firstAll()
	return c.serveCurrentStateAndAdvance()
}
