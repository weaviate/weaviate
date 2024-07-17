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
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type LinearCursorReplace struct {
	innerCursors []innerCursorReplace

	seekKey       []byte
	currentCursor int

	deletedKeys map[string]struct{}

	unlock func()
}

// LinearCursorReplace holds a RLock for the flushing state. It needs to be closed using the
// .Close() methods or otherwise the lock will never be released
func (b *Bucket) LinearCursor() *LinearCursorReplace {
	b.flushLock.RLock()

	if b.strategy != StrategyReplace {
		panic("LinearCursorReplace() called on strategy other than 'replace'")
	}

	innerCursors, unlockSegmentGroup := b.disk.newCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newCursor())
	}

	innerCursors = append(innerCursors, b.active.newCursor())

	return &LinearCursorReplace{
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: innerCursors,

		deletedKeys: make(map[string]struct{}),

		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
	}
}

func (c *LinearCursorReplace) Close() {
	c.unlock()
}

func (c *LinearCursorReplace) Seek(seekKey []byte) ([]byte, []byte) {
	c.seekKey = seekKey

	for c.currentCursor = 0; c.currentCursor < len(c.innerCursors); c.currentCursor++ {
		cursor := c.innerCursors[c.currentCursor]

		key, value, err := cursor.seek(seekKey)

		for {
			// read from cursor until a non-deleted key is found

			if errors.Is(err, lsmkv.NotFound) {
				// this cursor is skipped
				break
			}
			if err != nil && !errors.Is(err, lsmkv.Deleted) {
				panic(errors.Wrap(err, "unexpected error in seek (cursor type 'replace')"))
			}

			if errors.Is(err, lsmkv.Deleted) {
				k := make([]byte, len(key))
				copy(k, key)

				c.deletedKeys[string(key)] = struct{}{}
			} else {
				_, deleted := c.deletedKeys[string(key)]
				if !deleted {
					k := make([]byte, len(key))
					copy(k, key)

					v := make([]byte, len(value))
					copy(v, value)

					return k, v
				}
			}

			key, value, err = cursor.next()
		}
	}

	return nil, nil
}

func (c *LinearCursorReplace) Next() ([]byte, []byte) {
	seekRequired := false

	for ; c.currentCursor < len(c.innerCursors); c.currentCursor++ {
		cursor := c.innerCursors[c.currentCursor]

		var key []byte
		var value []byte
		var err error

		if seekRequired {
			key, value, err = cursor.seek(c.seekKey)
			seekRequired = false
		} else {
			key, value, err = cursor.next()
		}

		for {
			// read from cursor until a non-deleted key is found
			if errors.Is(err, lsmkv.NotFound) {
				seekRequired = len(c.seekKey) > 0
				break
			}
			if err != nil && !errors.Is(err, lsmkv.Deleted) {
				panic(errors.Wrap(err, "unexpected error in next (cursor type 'replace')"))
			}

			if errors.Is(err, lsmkv.Deleted) {
				k := make([]byte, len(key))
				copy(k, key)

				c.deletedKeys[string(key)] = struct{}{}
			} else {
				_, deleted := c.deletedKeys[string(key)]
				if !deleted {
					k := make([]byte, len(key))
					copy(k, key)

					v := make([]byte, len(value))
					copy(v, value)

					return k, v
				}
			}

			key, value, err = cursor.next()
		}

	}

	return nil, nil
}

func (c *LinearCursorReplace) First() ([]byte, []byte) {
	c.seekKey = nil
	c.currentCursor = 0

	for c.currentCursor = 0; c.currentCursor < len(c.innerCursors); c.currentCursor++ {
		cursor := c.innerCursors[c.currentCursor]

		key, value, err := cursor.first()

		for {
			// read from cursor until a non-deleted key is found
			if errors.Is(err, lsmkv.NotFound) {
				break
			}
			if err != nil && !errors.Is(err, lsmkv.Deleted) {
				panic(errors.Wrap(err, "unexpected error in first (cursor type 'replace')"))
			}

			if errors.Is(err, lsmkv.Deleted) {
				k := make([]byte, len(key))
				copy(k, key)

				c.deletedKeys[string(key)] = struct{}{}
			} else {
				_, deleted := c.deletedKeys[string(key)]
				if !deleted {
					k := make([]byte, len(key))
					copy(k, key)

					v := make([]byte, len(value))
					copy(v, value)

					return k, v
				}
			}

			key, value, err = cursor.next()
		}
	}

	return nil, nil
}
