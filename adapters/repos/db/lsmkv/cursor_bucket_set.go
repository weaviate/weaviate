//nolint // TODO
package lsmkv

type CursorSet struct {
	innerCursors []innerCursorCollection
	state        []cursorStateCollection
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

func (b *Bucket) SetCursor() *CursorSet {
	return &CursorSet{
		// cursor are in order from oldest to newest, with the memtable cursor
		// being at the very top
		innerCursors: append(
			b.disk.newCollectionCursors(), b.active.newCollectionCursor()),
	}
}
