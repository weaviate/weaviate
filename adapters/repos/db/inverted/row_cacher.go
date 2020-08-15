package inverted

import (
	"bytes"
	"sync"
	"sync/atomic"
)

type rowCacher struct {
	maxSize     uint64
	rowStore    *sync.Map
	currentSize uint64
}

func newRowCacher(maxSize uint64) *rowCacher {
	c := &rowCacher{
		maxSize:  maxSize,
		rowStore: &sync.Map{},
	}

	return c
}

func (rc *rowCacher) Store(id []byte, row *docPointers) {
	size := uint64(row.count * 4)
	if size > rc.maxSize {
		return
	}

	if atomic.LoadUint64(&rc.currentSize)+size > rc.maxSize {
		rc.deleteExistingEntries(size)
	}
	rc.rowStore.Store(string(id), row)
	atomic.AddUint64(&rc.currentSize, uint64(row.count*4))
}

func (rc *rowCacher) deleteExistingEntries(sizeToDelete uint64) {
	var deleted uint64
	rc.rowStore.Range(func(key, value interface{}) bool {
		parsed := value.(*docPointers)
		size := uint64(parsed.count * 4)
		rc.rowStore.Delete(key)
		deleted += size
		atomic.AddUint64(&rc.currentSize, -size)
		if deleted >= sizeToDelete {
			return false
		}
		return true
	})
}

func (rc *rowCacher) Load(id []byte,
	expectedChecksum []byte) (*docPointers, bool) {
	retrieved, ok := rc.rowStore.Load(string(id))
	if !ok {
		return nil, false
	}

	parsed := retrieved.(*docPointers)
	if !bytes.Equal(parsed.checksum, expectedChecksum) {
		rc.rowStore.Delete(string(id))
		return nil, false
	}

	return parsed, true
}
