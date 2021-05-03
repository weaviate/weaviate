package lsmkv

type Cursor struct {
	memtable *memtableCursor
}

func (b *Bucket) Cursor() *Cursor {
	return &Cursor{
		memtable: b.active.newCursor(),
	}
}

func (c *Cursor) Seek(key []byte) ([]byte, []byte) {
	return c.memtable.seek(key)
}

func (c *Cursor) Next() ([]byte, []byte) {
	return c.memtable.next()
}

func (c *Cursor) First() ([]byte, []byte) {
	return c.memtable.first()
}
