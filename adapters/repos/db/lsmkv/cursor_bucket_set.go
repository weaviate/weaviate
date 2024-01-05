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
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

type CursorSet struct {
	innerCursors []innerCursorCollection
	state        []cursorStateCollection
	unlock       func()
	keyOnly      bool
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
// .Close() methods or otherwise the lock will never be released
func (b *Bucket) SetCursor() *CursorSet {
	b.flushLock.RLock()

	if b.strategy != StrategySetCollection {
		panic("SetCursor() called on strategy other than 'set'")
	}

	innerCursors, unlockSegmentGroup := b.disk.newCollectionCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newCollectionCursor())
	}

	innerCursors = append(innerCursors, b.active.newCollectionCursor())

	return &CursorSet{
		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: innerCursors,
	}
}

// SetCursorKeyOnly returns nil for all values. It has no control over the
// underlying "inner" cursors which may still retrieve a value which is then
// discarded. It does however, omit any handling of values, such as decoding,
// making this considerably more efficient if only keys are required.
//
// The same locking rules as for SetCursor apply.
func (b *Bucket) SetCursorKeyOnly() *CursorSet {
	c := b.SetCursor()
	c.keyOnly = true
	return c
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
		if errors.Is(err, lsmkv.NotFound) {
			state[i].err = err
			continue
		}

		if err != nil {
			panic(fmt.Errorf("unexpected error in seek: %w", err))
		}

		state[i].key = key
		if !c.keyOnly {
			state[i].value = value
		}
	}

	c.state = state
}

func (c *CursorSet) firstAll() {
	state := make([]cursorStateCollection, len(c.innerCursors))
	for i, cur := range c.innerCursors {
		key, value, err := cur.first()
		if errors.Is(err, lsmkv.NotFound) {
			state[i].err = err
			continue
		}

		if err != nil {
			panic(fmt.Errorf("unexpected error in seek: %w", err))
		}

		state[i].key = key
		if !c.keyOnly {
			state[i].value = value
		}
	}

	c.state = state
}

func (c *CursorSet) serveCurrentStateAndAdvance() ([]byte, [][]byte) {
	id, err := c.cursorWithLowestKey()
	if err != nil {
		if errors.Is(err, lsmkv.NotFound) {
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

func (c *CursorSet) cursorWithLowestKey() (int, error) {
	err := lsmkv.NotFound
	pos := -1
	var lowest []byte

	for i, res := range c.state {
		if errors.Is(res.err, lsmkv.NotFound) {
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
	if len(values) == 0 {
		// all values deleted, skip key
		return c.Next()
	}

	// TODO remove keyOnly option, not used anyway
	if !c.keyOnly {
		return key, values
	} else {
		return key, nil
	}
}

func (c *CursorSet) advanceInner(id int) {
	k, v, err := c.innerCursors[id].next()
	if errors.Is(err, lsmkv.NotFound) {
		c.state[id].err = err
		c.state[id].key = nil
		if !c.keyOnly {
			c.state[id].value = nil
		}
		return
	}

	if errors.Is(err, lsmkv.Deleted) {
		c.state[id].err = err
		c.state[id].key = k
		c.state[id].value = nil
		return
	}

	if err != nil {
		panic(fmt.Errorf("unexpected error in advance: %w", err))
	}

	c.state[id].key = k
	if !c.keyOnly {
		c.state[id].value = v
	}
	c.state[id].err = nil
}
