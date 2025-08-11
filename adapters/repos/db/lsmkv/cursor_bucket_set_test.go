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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// Previous implementation of cursor called recursively Next() when empty entry occurred,
// which could lead to stack overflow. This test prevents a regression.
func TestSetCursor_StackOverflow(t *testing.T) {
	cursor := &CursorSet{
		unlock:       func() {},
		innerCursors: []innerCursorCollection{&emptyInnerCursorSet{}},
		keyOnly:      false,
	}

	k, vals := cursor.First()
	assert.Nil(t, k)
	assert.Nil(t, vals)
}

type emptyInnerCursorSet struct {
	key uint64
}

func (c *emptyInnerCursorSet) first() ([]byte, []value, error) {
	c.key = 0
	return c.bytes(), []value{}, nil
}

func (c *emptyInnerCursorSet) next() ([]byte, []value, error) {
	if c.key >= 1<<22 {
		return nil, nil, lsmkv.NotFound
	}
	c.key++
	return c.bytes(), []value{}, nil
}

func (c *emptyInnerCursorSet) seek(key []byte) ([]byte, []value, error) {
	return c.first()
}

func (c *emptyInnerCursorSet) bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, c.key)
	return b
}
