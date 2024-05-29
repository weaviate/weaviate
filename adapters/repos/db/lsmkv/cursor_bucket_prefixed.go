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

package lsmkv

import (
	"strings"

	"github.com/weaviate/sroar"
)

type Cursor[T any] interface {
	First() ([]byte, T)
	Next() ([]byte, T)
	Seek([]byte) ([]byte, T)
	Close()
}

func newCursorPrefixedRoaringSet(cursor CursorRoaringSet, prefix []byte) CursorRoaringSet {
	return &cursorPrefixed[*sroar.Bitmap]{
		cursor:          cursor,
		prefix:          prefix,
		started:         false,
		finished:        false,
		lastMatchingKey: nil,
	}
}

func newCursorPrefixedSet(cursor CursorSet, prefix []byte) CursorSet {
	return &cursorPrefixed[[][]byte]{
		cursor:          cursor,
		prefix:          prefix,
		started:         false,
		finished:        false,
		lastMatchingKey: nil,
	}
}

func newCursorPrefixedMap(cursor CursorMap, prefix []byte) CursorMap {
	return &cursorPrefixed[[]MapPair]{
		cursor:          cursor,
		prefix:          prefix,
		started:         false,
		finished:        false,
		lastMatchingKey: nil,
	}
}

func newCursorPrefixedReplace(cursor CursorReplace, prefix []byte) CursorReplace {
	return &cursorPrefixed[[]byte]{
		cursor:          cursor,
		prefix:          prefix,
		started:         false,
		finished:        false,
		lastMatchingKey: nil,
	}
}

type cursorPrefixed[T any] struct {
	cursor Cursor[T]
	prefix []byte
	// indicates whether internal cursor was already used
	// if not 1st call to Next() should fallback to First()
	started bool
	// indicates whether internal cursor passed prefixed keys or reached its end
	// if so, further calls to Next() can skip calling internal cursor
	finished bool
	// stores last matching prefixed key got from internal cursor
	// key can be used to rewind internal cursor to previous position in case of
	// call to Seek returns no result
	// (on unsuccessful Seek, current cursor should not advance even if internal one advanced,
	// therefore internal cursor has to be moved to previous position)
	lastMatchingKey []byte
}

func (c *cursorPrefixed[T]) First() ([]byte, T) {
	c.started = true

	if foundPrefixedKey, bm := c.cursor.Seek(c.prefix); foundPrefixedKey != nil {
		// something found, remove prefix if matches
		if key, removed := _removePrefix(c.prefix, foundPrefixedKey); removed {
			c.lastMatchingKey = foundPrefixedKey
			c.finished = false
			return key, bm
		}
	}

	var t0 T
	c.lastMatchingKey = nil
	c.finished = true
	return nil, t0
}

func (c *cursorPrefixed[T]) Next() ([]byte, T) {
	// fallback to First if not used before
	if !c.started {
		return c.First()
	}
	var t0 T
	if c.finished {
		return nil, t0
	}

	if foundPrefixedKey, bm := c.cursor.Next(); foundPrefixedKey != nil {
		// something found, remove prefix if matches
		if key, removed := _removePrefix(c.prefix, foundPrefixedKey); removed {
			c.lastMatchingKey = foundPrefixedKey
			c.finished = false
			return key, bm
		}
	}

	c.lastMatchingKey = nil
	c.finished = true
	return nil, t0
}

func (c *cursorPrefixed[T]) Seek(key []byte) ([]byte, T) {
	c.started = true

	if foundPrefixedKey, bm := c.cursor.Seek(_addPrefix(c.prefix, key)); foundPrefixedKey != nil {
		// something found, remove prefix if matches
		if key, removed := _removePrefix(c.prefix, foundPrefixedKey); removed {
			c.lastMatchingKey = foundPrefixedKey
			c.finished = false
			return key, bm
		}
	}

	var t0 T
	// move internal cursor back to previous position
	if c.lastMatchingKey != nil {
		c.cursor.Seek(c.lastMatchingKey)
	}
	return nil, t0
}

func (c *cursorPrefixed[T]) Close() {
	c.cursor.Close()
}

func _addPrefix(prefix, key []byte) []byte {
	pk := make([]byte, 0, len(prefix)+len(key))
	pk = append(pk, prefix...)
	pk = append(pk, key...)
	return pk
}

func _removePrefix(prefix, prefixedKey []byte) ([]byte, bool) {
	if _matchesPrefix(prefix, prefixedKey) {
		return prefixedKey[len(prefix):], true
	}
	return prefixedKey, false
}

func _matchesPrefix(prefix, prefixedKey []byte) bool {
	return strings.HasPrefix(string(prefixedKey), string(prefix))
}
