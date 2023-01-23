package lsmkv

import (
	"fmt"

	"github.com/dgraph-io/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
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

func (b *Bucket) CursorRoaringSet() CursorRoaringSet {
	return b.cursorRoaringSet(false)
}

func (b *Bucket) CursorRoaringSetKeyOnly() CursorRoaringSet {
	return b.cursorRoaringSet(true)
}

func (b *Bucket) cursorRoaringSet(keyOnly bool) CursorRoaringSet {
	b.flushLock.RLock()

	// TODO move to helper func
	if err := checkStrategyRoaringSet(b.strategy); err != nil {
		panic(fmt.Sprintf("CursorRoaringSet() called on strategy other than '%s'", StrategyRoaringSet))
	}

	innerCursors, unlockSegmentGroup := b.disk.newRoaringSetCursors()

	// we have a flush-RLock, so we have the guarantee that the flushing state
	// will not change for the lifetime of the cursor, thus there can only be two
	// states: either a flushing memtable currently exists - or it doesn't
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
			b.flushLock.RUnlock()
		},
	}
}
