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

package roaringsetrange

import (
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

type CombinedCursor struct {
	cursors []InnerCursor

	inited    bool
	states    []innerCursorState
	deletions []*sroar.Bitmap
}

type InnerCursor interface {
	First() (uint8, roaringset.BitmapLayer, bool)
	Next() (uint8, roaringset.BitmapLayer, bool)
}

type innerCursorState struct {
	key       uint8
	additions *sroar.Bitmap
	ok        bool
}

func NewCombinedCursor(innerCursors []InnerCursor) *CombinedCursor {
	return &CombinedCursor{
		cursors:   innerCursors,
		inited:    false,
		states:    make([]innerCursorState, len(innerCursors)),
		deletions: make([]*sroar.Bitmap, len(innerCursors)),
	}
}

func (c *CombinedCursor) First() (uint8, *sroar.Bitmap, bool) {
	c.inited = true
	for i, cursor := range c.cursors {
		key, layer, ok := cursor.First()
		if ok && key == 0 {
			c.deletions[i] = layer.Deletions
		} else {
			c.deletions[i] = sroar.NewBitmap()
		}

		c.states[i] = innerCursorState{
			key:       key,
			additions: layer.Additions,
			ok:        ok,
		}
	}

	return c.combine()
}

func (c *CombinedCursor) Next() (uint8, *sroar.Bitmap, bool) {
	if !c.inited {
		return c.First()
	}
	return c.combine()
}

func (c *CombinedCursor) combine() (uint8, *sroar.Bitmap, bool) {
	key, ids := c.getCursorIdsWithLowestKey()
	if len(ids) == 0 {
		return 0, nil, false
	}

	layers := roaringset.BitmapLayers{}
	for i := range c.cursors {
		var additions *sroar.Bitmap
		if _, ok := ids[i]; ok {
			additions = c.states[i].additions

			// move forward used cursors
			key, layer, ok := c.cursors[i].Next()
			c.states[i] = innerCursorState{
				key:       key,
				additions: layer.Additions,
				ok:        ok,
			}
		} else {
			additions = sroar.NewBitmap()
		}

		layers = append(layers, roaringset.BitmapLayer{
			Additions: additions,
			Deletions: c.deletions[i],
		})
	}

	return key, layers.Flatten(), true
}

func (c *CombinedCursor) getCursorIdsWithLowestKey() (uint8, map[int]struct{}) {
	var lowestKey uint8
	ids := map[int]struct{}{}

	for id, state := range c.states {
		if !state.ok {
			continue
		}

		if len(ids) == 0 {
			lowestKey = state.key
			ids[id] = struct{}{}
		} else if lowestKey == state.key {
			ids[id] = struct{}{}
		} else if lowestKey > state.key {
			lowestKey = state.key
			ids = map[int]struct{}{id: {}}
		}
	}

	return lowestKey, ids
}
