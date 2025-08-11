//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// Previous implementation of cursor called recursively Next() when empty entry occurred,
// which could lead to stack overflow. This test prevents a regression.
func TestMapCursor_StackOverflow(t *testing.T) {
	cursor := &CursorMap{
		unlock:       func() {},
		innerCursors: []innerCursorMap{&emptyInnerCursorMap{}},
		listCfg:      MapListOptionConfig{},
		keyOnly:      false,
	}

	k, mp := cursor.First(context.Background())
	assert.Nil(t, k)
	assert.Nil(t, mp)
}

type emptyInnerCursorMap struct {
	key uint64
}

func (c *emptyInnerCursorMap) first() ([]byte, []MapPair, error) {
	c.key = 0
	return c.bytes(), []MapPair{}, nil
}

func (c *emptyInnerCursorMap) next() ([]byte, []MapPair, error) {
	if c.key >= 1<<20 {
		return nil, nil, lsmkv.NotFound
	}
	c.key++
	return c.bytes(), []MapPair{}, nil
}

func (c *emptyInnerCursorMap) seek(key []byte) ([]byte, []MapPair, error) {
	return c.first()
}

func (c *emptyInnerCursorMap) bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, c.key)
	return b
}
