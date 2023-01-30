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

package roaringset

import (
	"bytes"

	"github.com/dgraph-io/sroar"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/entities"
)

type CombinedCursor struct {
	cursors []InnerCursor
	states  []innerCursorState
	keyOnly bool
}

type InnerCursor interface {
	First() ([]byte, BitmapLayer, error)
	Next() ([]byte, BitmapLayer, error)
	Seek(key []byte) ([]byte, BitmapLayer, error)
}

type innerCursorState struct {
	key   []byte
	layer BitmapLayer
	err   error
}

// When keyOnly flag is set, only keys are returned by First/Next/Seek access methods,
// 2nd value returned is expected to be nil
// When keyOnly is not set, 2nd value is always bitmap. Returned bitmap can be empty (e.g. for Next call after last element was already returned)
func NewCombinedCursor(innerCursors []InnerCursor, keyOnly bool) *CombinedCursor {
	return &CombinedCursor{cursors: innerCursors, keyOnly: keyOnly}
}

func (c *CombinedCursor) First() ([]byte, *sroar.Bitmap) {
	states := c.runAll(func(ic InnerCursor) ([]byte, BitmapLayer, error) {
		return ic.First()
	})
	return c.getResultFromStates(states)
}

func (c *CombinedCursor) Next() ([]byte, *sroar.Bitmap) {
	// fallback to First if no previous calls of First or Seek
	if c.states == nil {
		return c.First()
	}
	return c.getResultFromStates(c.states)
}

func (c *CombinedCursor) Seek(key []byte) ([]byte, *sroar.Bitmap) {
	states := c.runAll(func(ic InnerCursor) ([]byte, BitmapLayer, error) {
		return ic.Seek(key)
	})
	return c.getResultFromStates(states)
}

type cursorRun func(ic InnerCursor) ([]byte, BitmapLayer, error)

func (c *CombinedCursor) runAll(cursorRun cursorRun) []innerCursorState {
	states := make([]innerCursorState, len(c.cursors))
	for id, ic := range c.cursors {
		states[id] = c.createState(cursorRun(ic))
	}
	return states
}

func (c *CombinedCursor) createState(key []byte, layer BitmapLayer, err error) innerCursorState {
	if err == entities.NotFound {
		return innerCursorState{err: err}
	}
	if err != nil {
		panic(errors.Wrap(err, "unexpected error")) // TODO necessary?
	}
	state := innerCursorState{key: key}
	if !c.keyOnly {
		state.layer = layer
	}
	return state
}

func (c *CombinedCursor) getResultFromStates(states []innerCursorState) ([]byte, *sroar.Bitmap) {
	// NotFound is returned only by Seek call.
	// If all cursors returned NotFound, combined Seek has no result, therefore inner cursors' states
	// should not be updated to allow combined cursor to proceed with following Next calls

	key, ids, allNotFound := c.getCursorIdsWithLowestKey(states)
	if !allNotFound {
		c.states = states
	}
	layers := BitmapLayers{}
	for _, id := range ids {
		if !c.keyOnly {
			layers = append(layers, c.states[id].layer)
		}
		// forward cursors used in final result
		c.states[id] = c.createState(c.cursors[id].Next())
	}
	if !c.keyOnly {
		return key, layers.Flatten()
	}
	return key, nil
}

func (c *CombinedCursor) getCursorIdsWithLowestKey(states []innerCursorState) ([]byte, []int, bool) {
	var lowestKey []byte
	ids := []int{}
	allNotFound := true

	for id, state := range states {
		if state.err == entities.NotFound {
			continue
		}
		allNotFound = false
		if state.key == nil {
			continue
		}
		if lowestKey == nil {
			lowestKey = state.key
			ids = []int{id}
		} else if cmp := bytes.Compare(lowestKey, state.key); cmp > 0 {
			lowestKey = state.key
			ids = []int{id}
		} else if cmp == 0 {
			ids = append(ids, id)
		}
	}

	return lowestKey, ids, allNotFound
}
