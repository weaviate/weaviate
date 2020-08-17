//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"bytes"
	"sync"
	"sync/atomic"
)

type RowCacher struct {
	maxSize     uint64
	rowStore    *sync.Map
	currentSize uint64
}

func NewRowCacher(maxSize uint64) *RowCacher {
	c := &RowCacher{
		maxSize:  maxSize,
		rowStore: &sync.Map{},
	}

	return c
}

func (rc *RowCacher) Store(id []byte, row *docPointers) {
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

func (rc *RowCacher) deleteExistingEntries(sizeToDelete uint64) {
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

func (rc *RowCacher) Load(id []byte,
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
