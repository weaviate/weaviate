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
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/errors"
)

type CombinedCursor struct {
	cursors []InnerCursor
	logger  logrus.FieldLogger

	inited     bool
	states     []innerCursorState
	deletions  []*sroar.Bitmap
	nextKey    uint8
	nextLayers roaringset.BitmapLayers
	nextCh     chan struct{}
	doneCh     chan struct{}
	closeCh    chan struct{}
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

func NewCombinedCursor(innerCursors []InnerCursor, logger logrus.FieldLogger) *CombinedCursor {
	c := &CombinedCursor{
		cursors: innerCursors,
		logger:  logger,

		inited:     false,
		states:     make([]innerCursorState, len(innerCursors)),
		deletions:  make([]*sroar.Bitmap, len(innerCursors)),
		nextKey:    0,
		nextLayers: make(roaringset.BitmapLayers, len(innerCursors)),
		nextCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		closeCh:    make(chan struct{}, 1),
	}

	errors.GoWrapper(func() {
		for {
			select {
			case <-c.nextCh:
				c.nextKey, c.nextLayers = c.createNext()
				c.doneCh <- struct{}{}
			case <-c.closeCh:
				return
			}
		}
	}, logger)

	return c
}

func (c *CombinedCursor) First() (uint8, *sroar.Bitmap, bool) {
	c.inited = true
	for id, cursor := range c.cursors {
		key, layer, ok := cursor.First()
		if ok && key == 0 {
			c.deletions[id] = layer.Deletions
		} else {
			c.deletions[id] = sroar.NewBitmap()
		}

		c.states[id] = innerCursorState{
			key:       key,
			additions: layer.Additions,
			ok:        ok,
		}
	}

	// init next layers
	c.nextCh <- struct{}{}
	<-c.doneCh

	return c.next()
}

func (c *CombinedCursor) Next() (uint8, *sroar.Bitmap, bool) {
	if !c.inited {
		return c.First()
	}

	return c.next()
}

func (c *CombinedCursor) Close() {
	c.closeCh <- struct{}{}
}

func (c *CombinedCursor) next() (uint8, *sroar.Bitmap, bool) {
	if c.nextLayers == nil {
		return 0, nil, false
	}

	key := c.nextKey
	layers := c.nextLayers
	c.nextCh <- struct{}{}
	flat := layers.FlattenMutate()
	<-c.doneCh

	return key, flat, true
}

func (c *CombinedCursor) createNext() (uint8, roaringset.BitmapLayers) {
	key, ids := c.getCursorIdsWithLowestKey()
	if len(ids) == 0 {
		return 0, nil
	}

	layers := make(roaringset.BitmapLayers, len(c.cursors))
	empty := sroar.NewBitmap()

	for id := range c.cursors {
		additions := empty
		deletions := c.deletions[id]

		if _, ok := ids[id]; ok {
			additions = c.states[id].additions

			// move forward used cursors
			key, layer, ok := c.cursors[id].Next()
			c.states[id] = innerCursorState{
				key:       key,
				additions: layer.Additions,
				ok:        ok,
			}
		}

		// 1st addition and non 1st deletion bitmaps are mutated while flattening
		// therefore they are cloned upfront in goroutine
		if id == 0 {
			additions = additions.Clone()
		} else {
			deletions = deletions.Clone()
		}

		layers[id] = roaringset.BitmapLayer{
			Additions: additions,
			Deletions: deletions,
		}
	}

	return key, layers
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
