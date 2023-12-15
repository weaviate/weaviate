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
	"github.com/weaviate/weaviate/entities/lsmkv"
)

func (c *CursorSet) first() ([]byte, []value, error) {
	k, v := c.First()
	value := newSetEncoder().Do(v)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, value, nil
}

func (c *CursorSet) seek(key []byte) ([]byte, []value, error) {
	k, v := c.Seek(key)
	value := newSetEncoder().Do(v)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, value, nil
}

func (c *CursorSet) next() ([]byte, []value, error) {
	k, v := c.Next()
	value := newSetEncoder().Do(v)
	return k, value, nil
}

func (c *CursorMap) first() ([]byte, []MapPair, error) {
	k, v := c.First()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	k, v := c.Seek(key)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorMap) next() ([]byte, []MapPair, error) {
	k, v := c.Next()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorReplace) first() ([]byte, []byte, error) {
	k, v := c.First()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorReplace) seek(key []byte) ([]byte, []byte, error) {
	k, v := c.Seek(key)
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (c *CursorReplace) next() ([]byte, []byte, error) {
	k, v := c.Next()
	if k == nil && v == nil {
		return nil, nil, lsmkv.NotFound
	}
	return k, v, nil
}

func (m *MemtableThreaded) newCollectionCursor() innerCursorCollection {
	output := m.threadedOperation(ThreadedMemtableRequest{
		operation: ThreadedNewCollectionCursor,
	}, true, "NewCollectionCursor")
	return output.cursorSet
}

func (m *MemtableThreaded) newRoaringSetCursor() roaringset.InnerCursor {
	output := m.threadedOperation(ThreadedMemtableRequest{
		operation: ThreadedNewRoaringSetCursor,
	}, true, "NewRoaringSetCursor")
	return output.cursorRoaringSet
}

func (m *MemtableThreaded) newMapCursor() innerCursorMap {
	output := m.threadedOperation(ThreadedMemtableRequest{
		operation: ThreadedNewMapCursor,
	}, true, "NewMapCursor")
	return output.cursorMap
}

func (m *MemtableThreaded) newCursor() innerCursorReplace {
	output := m.threadedOperation(ThreadedMemtableRequest{
		operation: ThreadedNewCursor,
	}, true, "NewCursor")
	return output.cursorReplace
}
