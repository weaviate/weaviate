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

package lsmkv

import (
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

type memtableCursorRoaringSet struct {
	bstCursor *roaringset.BinarySearchTreeCursor
	lock      func()
	unlock    func()
}

func (m *Memtable) newRoaringSetCursor() roaringset.InnerCursor {
	m.RLock()
	defer m.RUnlock()

	return &memtableCursorRoaringSet{
		bstCursor: roaringset.NewBinarySearchTreeCursor(m.roaringSet),
		lock:      m.RLock,
		unlock:    m.RUnlock,
	}
}

func (c *memtableCursorRoaringSet) First() ([]byte, roaringset.BitmapLayer, error) {
	c.lock()
	defer c.unlock()
	return c.bstCursor.First()
}

func (c *memtableCursorRoaringSet) Next() ([]byte, roaringset.BitmapLayer, error) {
	c.lock()
	defer c.unlock()
	return c.bstCursor.Next()
}

func (c *memtableCursorRoaringSet) Seek(key []byte) ([]byte, roaringset.BitmapLayer, error) {
	c.lock()
	defer c.unlock()
	return c.bstCursor.Seek(key)
}
