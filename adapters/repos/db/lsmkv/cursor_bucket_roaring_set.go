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
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type CursorRoaringSet interface {
	First() ([]byte, *sroar.Bitmap)
	Next() ([]byte, *sroar.Bitmap)
	Seek([]byte) ([]byte, *sroar.Bitmap)
	Close()
}

type cursorRoaringSet struct {
	combinedCursor *roaringset.CombinedCursor
	unlock         func()
}

func (c *cursorRoaringSet) First() ([]byte, *sroar.Bitmap) {
	return c.combinedCursor.First()
}

func (c *cursorRoaringSet) Next() ([]byte, *sroar.Bitmap) {
	return c.combinedCursor.Next()
}

func (c *cursorRoaringSet) Seek(key []byte) ([]byte, *sroar.Bitmap) {
	return c.combinedCursor.Seek(key)
}

func (c *cursorRoaringSet) Close() {
	c.unlock()
}

// CursorRoaringSet behaves like [Cursor], but for the RoaringSet strategy. It
// needs to be closed using .Close() to free references to the underlying
// segments.
func (b *Bucket) CursorRoaringSet() CursorRoaringSet {
	return b.cursorRoaringSet(false)
}

// CursorRoaringSetKey is the equivalent of [CursorRoaringSet], but only
// returns keys. See [Cursor] for details on snapshot isolation. Needs to be
// closed using .Close() to free references to the underlying disk segments.
func (b *Bucket) CursorRoaringSetKeyOnly() CursorRoaringSet {
	return b.cursorRoaringSet(true)
}

func (b *Bucket) cursorRoaringSet(keyOnly bool) CursorRoaringSet {
	MustBeExpectedStrategy(b.strategy, StrategyRoaringSet)

	cursorOpenedAt := time.Now()
	b.metrics.IncBucketOpenedCursorsByStrategy(b.strategy)
	b.metrics.IncBucketOpenCursorsByStrategy(b.strategy)

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	innerCursors, unlockSegmentGroup := b.disk.newRoaringSetCursors()

	// we hold a flush-lock during initialzation, but we release it before
	// returning to the caller. However, `*memtable.newCursor` creates a deep
	// copy of the entire content, so this cursor will remain valid even after we
	// release the lock
	if b.flushing != nil {
		innerCursors = append(innerCursors, b.flushing.newRoaringSetCursor())
	}
	innerCursors = append(innerCursors, b.active.newRoaringSetCursor())

	// cursors are in order from oldest to newest, with the memtable cursor
	// being at the very top
	return &cursorRoaringSet{
		combinedCursor: roaringset.NewCombinedCursor(innerCursors, keyOnly),
		unlock: func() {
			unlockSegmentGroup()

			b.metrics.DecBucketOpenCursorsByStrategy(b.strategy)
			b.metrics.ObserveBucketCursorDurationByStrategy(b.strategy, time.Since(cursorOpenedAt))
		},
	}
}
