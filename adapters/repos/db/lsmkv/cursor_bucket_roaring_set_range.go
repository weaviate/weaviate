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
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

type CursorRoaringSetRange interface {
	First() (uint8, *sroar.Bitmap, bool)
	Next() (uint8, *sroar.Bitmap, bool)
	Close()
}

type cursorRoaringSetRange struct {
	combinedCursor *roaringsetrange.CombinedCursor
	unlock         func()
}

func (c *cursorRoaringSetRange) First() (uint8, *sroar.Bitmap, bool) {
	return c.combinedCursor.First()
}

func (c *cursorRoaringSetRange) Next() (uint8, *sroar.Bitmap, bool) {
	return c.combinedCursor.Next()
}

func (c *cursorRoaringSetRange) Close() {
	c.combinedCursor.Close()
	c.unlock()
}

func (b *Bucket) CursorRoaringSetRange() CursorRoaringSetRange {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSetRange)

	b.flushLock.RLock()

	innerCursors, unlockSegmentGroup := b.disk.newRoaringSetRangeCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newRoaringSetRangeCursor())
	}
	innerCursors = append(innerCursors, b.active.newRoaringSetRangeCursor())

	// cursors are in order from oldest to newest, with the memtable cursor
	// being at the very top
	return &cursorRoaringSetRange{
		combinedCursor: roaringsetrange.NewCombinedCursor(innerCursors, b.logger),
		unlock: func() {
			unlockSegmentGroup()
			b.flushLock.RUnlock()
		},
	}
}
