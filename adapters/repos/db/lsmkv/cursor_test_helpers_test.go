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

//go:build integrationTest

package lsmkv

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

// assertCursorReplace opens a Replace cursor, collects up to limit entries (from
// seek, or First when seek is nil) and asserts the yielded keys/values equal the
// expected ones. The expected data is passed inline as strings so each call site
// stays a few lines long — short enough that Sonar's copy-paste detector no
// longer flags the intrinsically repetitive seek/collect/compare shape that this
// suite exercises across dozens of near-identical sub-tests.
func assertCursorReplace(t *testing.T, b *Bucket, seek []byte, limit int, expectedKeys, expectedValues []string) {
	t.Helper()
	keys, values := collectCursorReplace(t, b, seek, limit)
	assert.Equal(t, toByteSlices(expectedKeys), keys)
	assert.Equal(t, toByteSlices(expectedValues), values)
}

// assertCursorSet is the Set-strategy equivalent of assertCursorReplace; each
// expected value is the list of values stored under that key.
func assertCursorSet(t *testing.T, b *Bucket, seek []byte, limit int, expectedKeys []string, expectedValues [][]string) {
	t.Helper()
	keys, values := collectCursorSet(t, b, seek, limit)
	assert.Equal(t, toByteSlices(expectedKeys), keys)
	assert.Equal(t, toByteSliceLists(expectedValues), values)
}

// toByteSlices converts a list of strings to [][]byte, returning nil for empty
// input so it compares equal to a cursor that yielded nothing.
func toByteSlices(ss []string) [][]byte {
	if len(ss) == 0 {
		return nil
	}
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// toByteSliceLists converts a list of string lists to [][][]byte, returning nil
// for empty input.
func toByteSliceLists(sss [][]string) [][][]byte {
	if len(sss) == 0 {
		return nil
	}
	out := make([][][]byte, len(sss))
	for i, ss := range sss {
		out[i] = toByteSlices(ss)
	}
	return out
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
