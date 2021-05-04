package lsmkv

type Cursor struct {
	memtable       *memtableCursor
	segmentCursors []*segmentCursor
}

func (b *Bucket) Cursor() *Cursor {
	return &Cursor{
		memtable:       b.active.newCursor(),
		segmentCursors: b.disk.newCursors(),
	}
}

func (c *Cursor) Seek(key []byte) ([]byte, []byte) {
	// temp logic
	k, v := c.memtable.seek(key)

	// hacky workaround to switch to disk only
	if k == nil {
		k, v, err := c.segmentCursors[0].seek(key)
		if err != nil {
			panic(err)
		}

		return k, v
	}

	return k, v
}

func (c *Cursor) Next() ([]byte, []byte) {
	// temp logic
	k, v := c.memtable.next()
	// hacky workaround to switch to disk only
	if k == nil && len(c.segmentCursors) > 0 {
		k, v, err := c.segmentCursors[0].next()
		if err == NotFound {
			return nil, nil
		}

		if err != nil {
			panic(err)
		}

		return k, v
	}

	return k, v
}

func (c *Cursor) First() ([]byte, []byte) {
	return c.memtable.first()
}
