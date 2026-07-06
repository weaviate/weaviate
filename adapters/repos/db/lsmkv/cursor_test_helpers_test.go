//go:build integrationTest

//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The cursor constructors return (cursor, error) since they may refuse a new
// consistent view while the bucket is shutting down. These helpers wrap the
// "open, assert no error, iterate, copy out" boilerplate that would otherwise
// be repeated at every call site. Everything returned is deep-copied, so the
// cursor is closed before the helper returns and the result stays valid.

// mustReplaceCursor opens a Replace cursor, asserting the bucket is not shutting
// down. The caller owns Close.
func mustReplaceCursor(t *testing.T, b *Bucket) *CursorReplace {
	t.Helper()
	c, err := b.Cursor()
	require.NoError(t, err)
	return c
}

// collectCursorReplace iterates a Replace cursor and returns the keys/values it
// yields. It starts at First() when seek is nil, otherwise at Seek(seek), and
// stops after limit entries (limit <= 0 collects everything).
func collectCursorReplace(t *testing.T, b *Bucket, seek []byte, limit int) (keys, values [][]byte) {
	t.Helper()
	c := mustReplaceCursor(t, b)
	defer c.Close()

	var k, v []byte
	if seek == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(seek)
	}
	for ; k != nil && (limit <= 0 || len(keys) < limit); k, v = c.Next() {
		keys = copyAndAppend(keys, k)
		values = copyAndAppend(values, v)
	}
	return keys, values
}

// collectCursorSet is the Set-strategy equivalent of collectCursorReplace. Each
// value is a list of values for that key, deep-copied out of the cursor.
func collectCursorSet(t *testing.T, b *Bucket, seek []byte, limit int) (keys [][]byte, values [][][]byte) {
	t.Helper()
	c, err := b.SetCursor()
	require.NoError(t, err)
	defer c.Close()

	var k []byte
	var v [][]byte
	if seek == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(seek)
	}
	for ; k != nil && (limit <= 0 || len(keys) < limit); k, v = c.Next() {
		keys = append(keys, copyByteSlice(k))
		values = append(values, copyByteSliceList(v))
	}
	return keys, values
}

// collectSetKVs is the Set-strategy equivalent of collectReplaceKVs.
func collectSetKVs[T any](t *testing.T, b *Bucket, build func(k []byte, v [][]byte) T) []T {
	t.Helper()
	c, err := b.SetCursor()
	require.NoError(t, err)
	defer c.Close()

	var out []T
	for k, v := c.First(); k != nil; k, v = c.Next() {
		out = append(out, build(copyByteSlice(k), copyByteSliceList(v)))
	}
	return out
}

// collectRoaringSetKVs is the RoaringSet-strategy equivalent of
// collectReplaceKVs. The bitmap is materialized via ToArray() inside the loop,
// which allocates, so the result stays valid after the cursor closes.
func collectRoaringSetKVs[T any](t *testing.T, b *Bucket, build func(k []byte, values []uint64) T) []T {
	t.Helper()
	c, err := b.CursorRoaringSet()
	require.NoError(t, err)
	defer c.Close()

	var out []T
	for k, v := c.First(); k != nil; k, v = c.Next() {
		out = append(out, build(copyByteSlice(k), v.ToArray()))
	}
	return out
}

// copyByteSliceList deep-copies a list of byte slices.
func copyByteSliceList(src [][]byte) [][]byte {
	if src == nil {
		return nil
	}
	dst := make([][]byte, len(src))
	for i := range src {
		dst[i] = copyByteSlice(src[i])
	}
	return dst
}
