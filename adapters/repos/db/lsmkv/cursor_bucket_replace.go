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
	"context"
)

type CursorReplace struct {
	realCursor *StreamingCursor
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


	return &CursorReplace{
		realCursor: NewStreamingCursor(context.Background(), b.dir, false),
	}
}

// CursorInMemWith returns a cursor which scan over the primary key of entries
// not yet persisted on disk.
// Segment creation and compaction will be blocked until the cursor is closed
func (b *Bucket) CursorInMem() *CursorReplace {
	return nil
}

// CursorOnDiskWith returns a cursor which scan over the primary key of entries
// already persisted on disk.
// New segments can still be created but compaction will be prevented
// while any cursor remains active
func (b *Bucket) CursorOnDisk() *CursorReplace {
	if b.strategy != StrategyReplace {
		panic("CursorWith(desiredSecondaryIndexCount) called on strategy other than 'replace'")
	}

	return &CursorReplace{
		realCursor: NewStreamingCursor(context.Background(), b.dir, false),
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



	return &CursorReplace{
		// cursor are in order from oldest to newest, with the memtable cursor
		realCursor: NewStreamingCursor(context.Background(), b.dir, false),

	}
}

func (c *CursorReplace) Close() {
	c.realCursor.Close()
}


func (c *CursorReplace) Seek(key []byte) ([]byte, []byte) {
	return c.realCursor.Seek(key)
}



func (c *CursorReplace) Next() ([]byte, []byte) {
	return c.realCursor.Next()
}


func (c *CursorReplace) First() ([]byte, []byte) {
	return c.realCursor.First()
}
