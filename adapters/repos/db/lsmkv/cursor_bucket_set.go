//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

// SetCursor behaves like [Cursor], but for the RoaringSet strategy. It
// needs to be closed using .Close() to free references to the underlying
// segments.
func (b *Bucket) SetCursor() *CursorSet {
	MustBeExpectedStrategy(b.strategy, StrategySetCollection)
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	innerCursors, unlockSegmentGroup := b.disk.newCollectionCursors()

	// we hold a flush-lock during initialzation, but we release it before
	// returning to the caller. However, `*memtable.newCollectionCursor` creates
	// a deep copy of the entire content, so this cursor will remain valid even
	// after we release the lock
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newCollectionCursor())
	}

	innerCursors = append(innerCursors, b.active.newCollectionCursor())

	return &CursorSet{
		unlock: func() {
			unlockSegmentGroup()
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
	for {
		id, err := c.cursorWithLowestKey()
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				return nil, nil
			}
		}

		ids, _ := c.haveDuplicatesInState(id)

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
			// all values deleted, proceed
			continue
		}

		// TODO remove keyOnly option, not used anyway
		if !c.keyOnly {
			return key, values
		}
		return key, nil
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
