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
	"sort"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

type CursorMap struct {
	innerCursors []innerCursorMap
	state        []cursorStateMap
	unlock       func()
	listCfg      MapListOptionConfig
	keyOnly      bool
}

type cursorStateMap struct {
	key   []byte
	value []MapPair
	err   error
}

type innerCursorMap interface {
	first() ([]byte, []MapPair, error)
	next() ([]byte, []MapPair, error)
	seek([]byte) ([]byte, []MapPair, error)
}

func (b *Bucket) MapCursor(cfgs ...MapListOption) *CursorMap {
	b.flushLock.RLock()

	c := MapListOptionConfig{}
	for _, cfg := range cfgs {
		cfg(&c)
	}

	innerCursors, unlockSegmentGroup := b.disk.newMapCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newMapCursor())
	}

	innerCursors = append(innerCursors, b.active.newMapCursor())

	return &CursorMap{
		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: innerCursors,
		listCfg:      c,
	}
}

func (b *Bucket) MapCursorKeyOnly(cfgs ...MapListOption) *CursorMap {
	c := b.MapCursor(cfgs...)
	c.keyOnly = true
	return c
}

func (c *CursorMap) Seek(key []byte) ([]byte, []MapPair) {
	c.seekAll(key)
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorMap) Next() ([]byte, []MapPair) {
	// before := time.Now()
	// defer func() {
	// 	fmt.Printf("-- total next took %s\n", time.Since(before))
	// }()
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorMap) First() ([]byte, []MapPair) {
	c.firstAll()
	return c.serveCurrentStateAndAdvance()
}

func (c *CursorMap) Close() {
	c.unlock()
}

func (c *CursorMap) seekAll(target []byte) {
	state := make([]cursorStateMap, len(c.innerCursors))
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

func (c *CursorMap) firstAll() {
	state := make([]cursorStateMap, len(c.innerCursors))
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

func (c *CursorMap) serveCurrentStateAndAdvance() ([]byte, []MapPair) {
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

func (c *CursorMap) cursorWithLowestKey() (int, error) {
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

func (c *CursorMap) haveDuplicatesInState(idWithLowestKey int) ([]int, bool) {
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
func (c *CursorMap) mergeDuplicatesInCurrentStateAndAdvance(ids []int) ([]byte, []MapPair) {
	// take the key from any of the results, we have the guarantee that they're
	// all the same
	key := c.state[ids[0]].key

	// appending := time.Duration(0)
	// advancing := time.Duration(0)

	var perSegmentResults [][]MapPair

	for _, id := range ids {
		candidates := c.state[id].value
		perSegmentResults = append(perSegmentResults, candidates)

		// before = time.Now()
		c.advanceInner(id)
		// advancing += time.Since(before)
	}
	// fmt.Printf("--- extract values [appending] took %s\n", appending)
	// fmt.Printf("--- extract values [advancing] took %s\n", advancing)

	if c.listCfg.legacyRequireManualSorting {
		for i := range perSegmentResults {
			sort.Slice(perSegmentResults[i], func(a, b int) bool {
				return bytes.Compare(perSegmentResults[i][a].Key,
					perSegmentResults[i][b].Key) == -1
			})
		}
	}

	merged, err := newSortedMapMerger().do(perSegmentResults)
	if err != nil {
		panic(fmt.Errorf("unexpected error decoding map values: %w", err))
	}
	if len(merged) == 0 {
		// all values deleted, skip key
		return c.Next()
	}

	// TODO remove keyOnly option, not used anyway
	if !c.keyOnly {
		return key, merged
	} else {
		return key, nil
	}
}

func (c *CursorMap) advanceInner(id int) {
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
		panic(fmt.Errorf("unexpected error in advance: %w", err))
	}

	c.state[id].key = k
	if !c.keyOnly {
		c.state[id].value = v
	}
	c.state[id].err = nil
}
