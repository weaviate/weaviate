//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"sync"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

type CacheEntry struct {
	Hash      []byte
	AllowList helpers.AllowList
}

func (ce *CacheEntry) Size() uint64 {
	return ce.AllowList.Size()
}

func (rc *RowCacher) Store(id []byte, row *CacheEntry) {
	size := row.Size()
	if size > rc.maxSize {
		return
	}

	if atomic.LoadUint64(&rc.currentSize)+size > rc.maxSize {
		rc.deleteExistingEntries(size)
	}
	rc.rowStore.Store(string(id), row)
	atomic.AddUint64(&rc.currentSize, size)
}

func (rc *RowCacher) deleteExistingEntries(sizeToDelete uint64) {
	var deleted uint64
	rc.rowStore.Range(func(key, value interface{}) bool {
		parsed := value.(*CacheEntry)
		size := parsed.Size()
		rc.rowStore.Delete(key)
		deleted += size
		atomic.AddUint64(&rc.currentSize, -size)
		return deleted <= sizeToDelete
	})
}

func (rc *RowCacher) Size() uint64 {
	return atomic.LoadUint64(&rc.currentSize)
}

func (rc *RowCacher) Load(id []byte) (*CacheEntry, bool) {
	retrieved, ok := rc.rowStore.Load(string(id))
	if !ok {
		return nil, false
	}

	parsed := retrieved.(*CacheEntry)

	return parsed, true
}
