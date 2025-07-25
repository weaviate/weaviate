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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type CursorReplace struct {
	innerCursors []innerCursorReplace
	state        []cursorStateReplace
	unlock       func()
	serveCache   cursorStateReplace

	reusableIDList []int
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
// .Close() methods or otherwise the lock will never be released
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

// CursorInMemWith returns a cursor which scan over the primary key of entries
// not yet persisted on disk.
// Segment creation and compaction will be blocked until the cursor is closed
func (b *Bucket) CursorInMem() *CursorReplace {
	b.flushLock.RLock()

	if b.strategy != StrategyReplace {
		panic("CursorInMemWith() called on strategy other than 'replace'")
	}

	var innerCursors []innerCursorReplace

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	var flushingMemtableCursor innerCursorReplace
	var releaseFlushingMemtable func()

	if b.flushing != nil {
		flushingMemtableCursor, releaseFlushingMemtable = b.flushing.newBlockingCursor()
		innerCursors = append(innerCursors, flushingMemtableCursor)
	}

	activeMemtableCursor, releaseActiveMemtable := b.active.newBlockingCursor()
	innerCursors = append(innerCursors, activeMemtableCursor)

	return &CursorReplace{
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: innerCursors,
		unlock: func() {
			if b.flushing != nil {
				releaseFlushingMemtable()
			}
			releaseActiveMemtable()
			b.flushLock.RUnlock()
		},
	}
}

// CursorOnDiskWith returns a cursor which scan over the primary key of entries
// already persisted on disk.
// New segments can still be created but compaction will be prevented
// while any cursor remains active
func (b *Bucket) CursorOnDisk() *CursorReplace {
	if b.strategy != StrategyReplace {
		panic("CursorWith(desiredSecondaryIndexCount) called on strategy other than 'replace'")
	}

	innerCursors, unlockSegmentGroup := b.disk.newCursorsWithFlushingSupport()

	return &CursorReplace{
		innerCursors: innerCursors,
		unlock: func() {
			unlockSegmentGroup()
		},
	}
}

// CursorWithSecondaryIndex holds a RLock for the flushing state. It needs to be closed using the
// .Close() methods or otherwise the lock will never be released
func (b *Bucket) CursorWithSecondaryIndex(pos int) *CursorReplace {
	b.flushLock.RLock()

	if b.strategy != StrategyReplace {
		panic("CursorWithSecondaryIndex() called on strategy other than 'replace'")
	}

	if b.secondaryIndices <= uint16(pos) {
		panic("CursorWithSecondaryIndex() called on a bucket without enough secondary indexes")
	}

	innerCursors, unlockSegmentGroup := b.disk.newCursorsWithSecondaryIndex(pos)

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newCursorWithSecondaryIndex(pos))
	}

	innerCursors = append(innerCursors, b.active.newCursorWithSecondaryIndex(pos))

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
		if errors.Is(err, lsmkv.NotFound) {
			state[i].err = err
			continue
		}

		if errors.Is(err, lsmkv.Deleted) {
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
	for {
		id, err := c.cursorWithLowestKey()
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) {
				return nil, nil
			}
		}

		ids, _ := c.haveDuplicatesInState(id)

		c.copyStateIntoServeCache(ids[len(ids)-1])

		// with a replace strategy only the highest will be returned, but still all
		// need to be advanced - or we would just encounter them again in the next
		// round
		for _, id := range ids {
			c.advanceInner(id)
		}

		if errors.Is(c.serveCache.err, lsmkv.Deleted) {
			// element was deleted, proceed with next round
			continue
		}

		return c.serveCache.key, c.serveCache.value
	}
}

func (c *CursorReplace) haveDuplicatesInState(idWithLowestKey int) ([]int, bool) {
	key := c.state[idWithLowestKey].key

	c.reusableIDList = c.reusableIDList[:0]

	for i, cur := range c.state {
		if i == idWithLowestKey {
			c.reusableIDList = append(c.reusableIDList, i)
			continue
		}

		if bytes.Equal(key, cur.key) {
			c.reusableIDList = append(c.reusableIDList, i)
		}
	}

	return c.reusableIDList, len(c.reusableIDList) > 1
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

func (c *CursorReplace) advanceInner(id int) {
	k, v, err := c.innerCursors[id].next()
	if errors.Is(err, lsmkv.NotFound) {
		c.state[id].err = err
		c.state[id].key = nil
		c.state[id].value = nil
		return
	}

	if errors.Is(err, lsmkv.Deleted) {
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
		if errors.Is(err, lsmkv.NotFound) {
			state[i].err = err
			continue
		}
		if errors.Is(err, lsmkv.Deleted) {
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
