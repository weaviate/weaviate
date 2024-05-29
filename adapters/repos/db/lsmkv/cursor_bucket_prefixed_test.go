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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func TestCursorPrefixed_RoaringSet(t *testing.T) {
	type testCase struct {
		prefix string
		id     uint64
	}

	testCases := []testCase{
		{
			prefix: "a",
			id:     1,
		},
		{
			prefix: "b",
			id:     2,
		},
		{
			prefix: "c",
			id:     3,
		},
	}

	for _, tc := range testCases {
		var cursor CursorRoaringSet = newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte(tc.prefix))

		t.Run("first", func(t *testing.T) {
			key, bm := cursor.First()
			assertEntryRoaringSet(t, key, bm, []byte("201"), tc.id, 201)
		})

		t.Run("nexts", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntryRoaringSet(t, key1, value1, []byte("202"), tc.id, 202)
			key2, value2 := cursor.Next()
			assertEntryRoaringSet(t, key2, value2, []byte("203"), tc.id, 203)
			key3, value3 := cursor.Next()
			assertEntryRoaringSet(t, key3, value3, []byte("401"), tc.id, 401)
			key4, value4 := cursor.Next()
			assertEntryRoaringSet(t, key4, value4, []byte("402"), tc.id, 402)
			key5, value5 := cursor.Next()
			assertEntryRoaringSet(t, key5, value5, []byte("403"), tc.id, 403)
			key6, value6 := cursor.Next()
			assertEntryRoaringSetNil(t, key6, value6)
			key7, value7 := cursor.Next()
			assertEntryRoaringSetNil(t, key7, value7)
		})

		t.Run("seeks", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("402"))
			assertEntryRoaringSet(t, key1, value1, []byte("402"), tc.id, 402)
			key2, value2 := cursor.Seek([]byte("333"))
			assertEntryRoaringSet(t, key2, value2, []byte("401"), tc.id, 401)
			key3, value3 := cursor.Seek([]byte("203"))
			assertEntryRoaringSet(t, key3, value3, []byte("203"), tc.id, 203)
			key4, value4 := cursor.Seek([]byte("200"))
			assertEntryRoaringSet(t, key4, value4, []byte("201"), tc.id, 201)
			key5, value5 := cursor.Seek([]byte("404"))
			assertEntryRoaringSetNil(t, key5, value5)
			key6, value6 := cursor.Seek([]byte("101"))
			assertEntryRoaringSet(t, key6, value6, []byte("201"), tc.id, 201)
		})

		t.Run("mix", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("401"))
			assertEntryRoaringSet(t, key1, value1, []byte("401"), tc.id, 401)
			key2, value2 := cursor.First()
			assertEntryRoaringSet(t, key2, value2, []byte("201"), tc.id, 201)
			key3, value3 := cursor.Seek([]byte("666"))
			assertEntryRoaringSetNil(t, key3, value3)
			key4, value4 := cursor.Next()
			assertEntryRoaringSet(t, key4, value4, []byte("202"), tc.id, 202)
			key5, value5 := cursor.Next()
			assertEntryRoaringSet(t, key5, value5, []byte("203"), tc.id, 203)
			key6, value6 := cursor.First()
			assertEntryRoaringSet(t, key6, value6, []byte("201"), tc.id, 201)
			key7, value7 := cursor.Seek([]byte("402"))
			assertEntryRoaringSet(t, key7, value7, []byte("402"), tc.id, 402)
			key8, value8 := cursor.Next()
			assertEntryRoaringSet(t, key8, value8, []byte("403"), tc.id, 403)
			key9, value9 := cursor.Next()
			assertEntryRoaringSetNil(t, key9, value9)
			key10, value10 := cursor.Next()
			assertEntryRoaringSetNil(t, key10, value10)
			key11, value11 := cursor.First()
			assertEntryRoaringSet(t, key11, value11, []byte("201"), tc.id, 201)
			key12, value12 := cursor.Seek([]byte("201"))
			assertEntryRoaringSet(t, key12, value12, []byte("201"), tc.id, 201)
			key13, value13 := cursor.Seek([]byte("403"))
			assertEntryRoaringSet(t, key13, value13, []byte("403"), tc.id, 403)
			key14, value14 := cursor.Next()
			assertEntryRoaringSetNil(t, key14, value14)
			key15, value15 := cursor.Seek([]byte("403"))
			assertEntryRoaringSet(t, key15, value15, []byte("403"), tc.id, 403)
		})
	}

	for _, tc := range testCases {
		var cursor CursorRoaringSet = newCursorPrefixedRoaringSet(newFakeCursorRoaringSet(), []byte(tc.prefix))

		t.Run("next fallbacks to first", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntryRoaringSet(t, key1, value1, []byte("201"), tc.id, 201)
			key2, value2 := cursor.Next()
			assertEntryRoaringSet(t, key2, value2, []byte("202"), tc.id, 202)
		})
	}
}

func newFakeCursorRoaringSet() CursorRoaringSet {
	entries := []*entry[*sroar.Bitmap]{
		{key: []byte("a201"), value: roaringset.NewBitmap(1, 201)},
		{key: []byte("a202"), value: roaringset.NewBitmap(1, 202)},
		{key: []byte("a203"), value: roaringset.NewBitmap(1, 203)},
		{key: []byte("a401"), value: roaringset.NewBitmap(1, 401)},
		{key: []byte("a402"), value: roaringset.NewBitmap(1, 402)},
		{key: []byte("a403"), value: roaringset.NewBitmap(1, 403)},
		{key: []byte("b201"), value: roaringset.NewBitmap(2, 201)},
		{key: []byte("b202"), value: roaringset.NewBitmap(2, 202)},
		{key: []byte("b203"), value: roaringset.NewBitmap(2, 203)},
		{key: []byte("b401"), value: roaringset.NewBitmap(2, 401)},
		{key: []byte("b402"), value: roaringset.NewBitmap(2, 402)},
		{key: []byte("b403"), value: roaringset.NewBitmap(2, 403)},
		{key: []byte("c201"), value: roaringset.NewBitmap(3, 201)},
		{key: []byte("c202"), value: roaringset.NewBitmap(3, 202)},
		{key: []byte("c203"), value: roaringset.NewBitmap(3, 203)},
		{key: []byte("c401"), value: roaringset.NewBitmap(3, 401)},
		{key: []byte("c402"), value: roaringset.NewBitmap(3, 402)},
		{key: []byte("c403"), value: roaringset.NewBitmap(3, 403)},
	}
	return &fakeCursor[*sroar.Bitmap]{pos: 0, entries: entries}
}

func assertEntryRoaringSet(t *testing.T, key []byte, bm *sroar.Bitmap, expectedKey []byte, expectedValues ...uint64) {
	require.Equal(t, expectedKey, key)
	require.ElementsMatch(t, expectedValues, bm.ToArray())
}

func assertEntryRoaringSetNil(t *testing.T, key []byte, bm *sroar.Bitmap) {
	require.Nil(t, key)
	require.Nil(t, bm)
}

func TestCursorPrefixed_Set(t *testing.T) {
	type testCase struct {
		prefix string
		id     uint64
	}

	testCases := []testCase{
		{
			prefix: "a",
			id:     1,
		},
		{
			prefix: "b",
			id:     2,
		},
		{
			prefix: "c",
			id:     3,
		},
	}

	for _, tc := range testCases {
		var cursor CursorSet = newCursorPrefixedSet(newFakeCursorSet(), []byte(tc.prefix))

		t.Run("first", func(t *testing.T) {
			key, value := cursor.First()
			assertEntrySet(t, key, value, []byte("201"), tc.id, 201)
		})

		t.Run("nexts", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntrySet(t, key1, value1, []byte("202"), tc.id, 202)
			key2, value2 := cursor.Next()
			assertEntrySet(t, key2, value2, []byte("203"), tc.id, 203)
			key3, value3 := cursor.Next()
			assertEntrySet(t, key3, value3, []byte("401"), tc.id, 401)
			key4, value4 := cursor.Next()
			assertEntrySet(t, key4, value4, []byte("402"), tc.id, 402)
			key5, value5 := cursor.Next()
			assertEntrySet(t, key5, value5, []byte("403"), tc.id, 403)
			key6, value6 := cursor.Next()
			assertEntrySetNil(t, key6, value6)
			key7, value7 := cursor.Next()
			assertEntrySetNil(t, key7, value7)
		})

		t.Run("seeks", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("402"))
			assertEntrySet(t, key1, value1, []byte("402"), tc.id, 402)
			key2, value2 := cursor.Seek([]byte("333"))
			assertEntrySet(t, key2, value2, []byte("401"), tc.id, 401)
			key3, value3 := cursor.Seek([]byte("203"))
			assertEntrySet(t, key3, value3, []byte("203"), tc.id, 203)
			key4, value4 := cursor.Seek([]byte("200"))
			assertEntrySet(t, key4, value4, []byte("201"), tc.id, 201)
			key5, value5 := cursor.Seek([]byte("404"))
			assertEntrySetNil(t, key5, value5)
			key6, value6 := cursor.Seek([]byte("101"))
			assertEntrySet(t, key6, value6, []byte("201"), tc.id, 201)
		})

		t.Run("mix", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("401"))
			assertEntrySet(t, key1, value1, []byte("401"), tc.id, 401)
			key2, value2 := cursor.First()
			assertEntrySet(t, key2, value2, []byte("201"), tc.id, 201)
			key3, value3 := cursor.Seek([]byte("666"))
			assertEntrySetNil(t, key3, value3)
			key4, value4 := cursor.Next()
			assertEntrySet(t, key4, value4, []byte("202"), tc.id, 202)
			key5, value5 := cursor.Next()
			assertEntrySet(t, key5, value5, []byte("203"), tc.id, 203)
			key6, value6 := cursor.First()
			assertEntrySet(t, key6, value6, []byte("201"), tc.id, 201)
			key7, value7 := cursor.Seek([]byte("402"))
			assertEntrySet(t, key7, value7, []byte("402"), tc.id, 402)
			key8, value8 := cursor.Next()
			assertEntrySet(t, key8, value8, []byte("403"), tc.id, 403)
			key9, value9 := cursor.Next()
			assertEntrySetNil(t, key9, value9)
			key10, value10 := cursor.Next()
			assertEntrySetNil(t, key10, value10)
			key11, value11 := cursor.First()
			assertEntrySet(t, key11, value11, []byte("201"), tc.id, 201)
			key12, value12 := cursor.Seek([]byte("201"))
			assertEntrySet(t, key12, value12, []byte("201"), tc.id, 201)
			key13, value13 := cursor.Seek([]byte("403"))
			assertEntrySet(t, key13, value13, []byte("403"), tc.id, 403)
			key14, value14 := cursor.Next()
			assertEntrySetNil(t, key14, value14)
			key15, value15 := cursor.Seek([]byte("403"))
			assertEntrySet(t, key15, value15, []byte("403"), tc.id, 403)
		})
	}

	for _, tc := range testCases {
		var cursor CursorSet = newCursorPrefixedSet(newFakeCursorSet(), []byte(tc.prefix))

		t.Run("next fallbacks to first", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntrySet(t, key1, value1, []byte("201"), tc.id, 201)
			key2, value2 := cursor.Next()
			assertEntrySet(t, key2, value2, []byte("202"), tc.id, 202)
		})
	}
}

func newFakeCursorSet() CursorSet {
	entries := []*entry[[][]byte]{
		{key: []byte("a201"), value: asBytes2D(1, 201)},
		{key: []byte("a202"), value: asBytes2D(1, 202)},
		{key: []byte("a203"), value: asBytes2D(1, 203)},
		{key: []byte("a401"), value: asBytes2D(1, 401)},
		{key: []byte("a402"), value: asBytes2D(1, 402)},
		{key: []byte("a403"), value: asBytes2D(1, 403)},
		{key: []byte("b201"), value: asBytes2D(2, 201)},
		{key: []byte("b202"), value: asBytes2D(2, 202)},
		{key: []byte("b203"), value: asBytes2D(2, 203)},
		{key: []byte("b401"), value: asBytes2D(2, 401)},
		{key: []byte("b402"), value: asBytes2D(2, 402)},
		{key: []byte("b403"), value: asBytes2D(2, 403)},
		{key: []byte("c201"), value: asBytes2D(3, 201)},
		{key: []byte("c202"), value: asBytes2D(3, 202)},
		{key: []byte("c203"), value: asBytes2D(3, 203)},
		{key: []byte("c401"), value: asBytes2D(3, 401)},
		{key: []byte("c402"), value: asBytes2D(3, 402)},
		{key: []byte("c403"), value: asBytes2D(3, 403)},
	}
	return &fakeCursor[[][]byte]{pos: 0, entries: entries}
}

func assertEntrySet(t *testing.T, key []byte, set [][]byte, expectedKey []byte, expectedValues ...uint64) {
	require.Equal(t, expectedKey, key)
	require.ElementsMatch(t, asBytes2D(expectedValues...), set)
}

func assertEntrySetNil(t *testing.T, key []byte, set [][]byte) {
	require.Nil(t, key)
	require.Nil(t, set)
}

func asBytes2D(values ...uint64) [][]byte {
	bytes := make([][]byte, len(values))
	for i := range values {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, values[i])
		bytes[i] = b
	}
	return bytes
}

func TestCursorPrefixed_Map(t *testing.T) {
	type testCase struct {
		prefix string
		id     uint64
	}

	testCases := []testCase{
		{
			prefix: "a",
			id:     1,
		},
		{
			prefix: "b",
			id:     2,
		},
		{
			prefix: "c",
			id:     3,
		},
	}

	for _, tc := range testCases {
		var cursor CursorMap = newCursorPrefixedMap(newFakeCursorMap(), []byte(tc.prefix))

		t.Run("first", func(t *testing.T) {
			key, value := cursor.First()
			assertEntryMap(t, key, value, []byte("201"), tc.id, 201)
		})

		t.Run("nexts", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntryMap(t, key1, value1, []byte("202"), tc.id, 202)
			key2, value2 := cursor.Next()
			assertEntryMap(t, key2, value2, []byte("203"), tc.id, 203)
			key3, value3 := cursor.Next()
			assertEntryMap(t, key3, value3, []byte("401"), tc.id, 401)
			key4, value4 := cursor.Next()
			assertEntryMap(t, key4, value4, []byte("402"), tc.id, 402)
			key5, value5 := cursor.Next()
			assertEntryMap(t, key5, value5, []byte("403"), tc.id, 403)
			key6, value6 := cursor.Next()
			assertEntryMapNil(t, key6, value6)
			key7, value7 := cursor.Next()
			assertEntryMapNil(t, key7, value7)
		})

		t.Run("seeks", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("402"))
			assertEntryMap(t, key1, value1, []byte("402"), tc.id, 402)
			key2, value2 := cursor.Seek([]byte("333"))
			assertEntryMap(t, key2, value2, []byte("401"), tc.id, 401)
			key3, value3 := cursor.Seek([]byte("203"))
			assertEntryMap(t, key3, value3, []byte("203"), tc.id, 203)
			key4, value4 := cursor.Seek([]byte("200"))
			assertEntryMap(t, key4, value4, []byte("201"), tc.id, 201)
			key5, value5 := cursor.Seek([]byte("404"))
			assertEntryMapNil(t, key5, value5)
			key6, value6 := cursor.Seek([]byte("101"))
			assertEntryMap(t, key6, value6, []byte("201"), tc.id, 201)
		})

		t.Run("mix", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("401"))
			assertEntryMap(t, key1, value1, []byte("401"), tc.id, 401)
			key2, value2 := cursor.First()
			assertEntryMap(t, key2, value2, []byte("201"), tc.id, 201)
			key3, value3 := cursor.Seek([]byte("666"))
			assertEntryMapNil(t, key3, value3)
			key4, value4 := cursor.Next()
			assertEntryMap(t, key4, value4, []byte("202"), tc.id, 202)
			key5, value5 := cursor.Next()
			assertEntryMap(t, key5, value5, []byte("203"), tc.id, 203)
			key6, value6 := cursor.First()
			assertEntryMap(t, key6, value6, []byte("201"), tc.id, 201)
			key7, value7 := cursor.Seek([]byte("402"))
			assertEntryMap(t, key7, value7, []byte("402"), tc.id, 402)
			key8, value8 := cursor.Next()
			assertEntryMap(t, key8, value8, []byte("403"), tc.id, 403)
			key9, value9 := cursor.Next()
			assertEntryMapNil(t, key9, value9)
			key10, value10 := cursor.Next()
			assertEntryMapNil(t, key10, value10)
			key11, value11 := cursor.First()
			assertEntryMap(t, key11, value11, []byte("201"), tc.id, 201)
			key12, value12 := cursor.Seek([]byte("201"))
			assertEntryMap(t, key12, value12, []byte("201"), tc.id, 201)
			key13, value13 := cursor.Seek([]byte("403"))
			assertEntryMap(t, key13, value13, []byte("403"), tc.id, 403)
			key14, value14 := cursor.Next()
			assertEntryMapNil(t, key14, value14)
			key15, value15 := cursor.Seek([]byte("403"))
			assertEntryMap(t, key15, value15, []byte("403"), tc.id, 403)
		})
	}

	for _, tc := range testCases {
		var cursor CursorMap = newCursorPrefixedMap(newFakeCursorMap(), []byte(tc.prefix))

		t.Run("next fallbacks to first", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntryMap(t, key1, value1, []byte("201"), tc.id, 201)
			key2, value2 := cursor.Next()
			assertEntryMap(t, key2, value2, []byte("202"), tc.id, 202)
		})
	}
}

func newFakeCursorMap() CursorMap {
	entries := []*entry[[]MapPair]{
		{key: []byte("a201"), value: asMapPairs(1, 201)},
		{key: []byte("a202"), value: asMapPairs(1, 202)},
		{key: []byte("a203"), value: asMapPairs(1, 203)},
		{key: []byte("a401"), value: asMapPairs(1, 401)},
		{key: []byte("a402"), value: asMapPairs(1, 402)},
		{key: []byte("a403"), value: asMapPairs(1, 403)},
		{key: []byte("b201"), value: asMapPairs(2, 201)},
		{key: []byte("b202"), value: asMapPairs(2, 202)},
		{key: []byte("b203"), value: asMapPairs(2, 203)},
		{key: []byte("b401"), value: asMapPairs(2, 401)},
		{key: []byte("b402"), value: asMapPairs(2, 402)},
		{key: []byte("b403"), value: asMapPairs(2, 403)},
		{key: []byte("c201"), value: asMapPairs(3, 201)},
		{key: []byte("c202"), value: asMapPairs(3, 202)},
		{key: []byte("c203"), value: asMapPairs(3, 203)},
		{key: []byte("c401"), value: asMapPairs(3, 401)},
		{key: []byte("c402"), value: asMapPairs(3, 402)},
		{key: []byte("c403"), value: asMapPairs(3, 403)},
	}
	return &fakeCursor[[]MapPair]{pos: 0, entries: entries}
}

func assertEntryMap(t *testing.T, key []byte, mp []MapPair, expectedKey []byte, expectedValues ...uint64) {
	require.Equal(t, expectedKey, key)
	require.ElementsMatch(t, asMapPairs(expectedValues...), mp)
}

func assertEntryMapNil(t *testing.T, key []byte, mp []MapPair) {
	require.Nil(t, key)
	require.Nil(t, mp)
}

func asMapPairs(values ...uint64) []MapPair {
	mp := MapPair{Tombstone: false}
	for i := range values {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, values[i])
		if i == 0 {
			mp.Key = b
		} else {
			mp.Value = append(mp.Value, b...)
		}
	}
	return []MapPair{mp}
}

func TestCursorPrefixed_Replace(t *testing.T) {
	type testCase struct {
		prefix string
		id     uint64
	}

	testCases := []testCase{
		{
			prefix: "a",
			id:     1,
		},
		{
			prefix: "b",
			id:     2,
		},
		{
			prefix: "c",
			id:     3,
		},
	}

	for _, tc := range testCases {
		var cursor CursorReplace = newCursorPrefixedReplace(newFakeCursorReplace(), []byte(tc.prefix))

		t.Run("first", func(t *testing.T) {
			key, value := cursor.First()
			assertEntryReplace(t, key, value, []byte("201"), tc.id, 201)
		})

		t.Run("nexts", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntryReplace(t, key1, value1, []byte("202"), tc.id, 202)
			key2, value2 := cursor.Next()
			assertEntryReplace(t, key2, value2, []byte("203"), tc.id, 203)
			key3, value3 := cursor.Next()
			assertEntryReplace(t, key3, value3, []byte("401"), tc.id, 401)
			key4, value4 := cursor.Next()
			assertEntryReplace(t, key4, value4, []byte("402"), tc.id, 402)
			key5, value5 := cursor.Next()
			assertEntryReplace(t, key5, value5, []byte("403"), tc.id, 403)
			key6, value6 := cursor.Next()
			assertEntryReplaceNil(t, key6, value6)
			key7, value7 := cursor.Next()
			assertEntryReplaceNil(t, key7, value7)
		})

		t.Run("seeks", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("402"))
			assertEntryReplace(t, key1, value1, []byte("402"), tc.id, 402)
			key2, value2 := cursor.Seek([]byte("333"))
			assertEntryReplace(t, key2, value2, []byte("401"), tc.id, 401)
			key3, value3 := cursor.Seek([]byte("203"))
			assertEntryReplace(t, key3, value3, []byte("203"), tc.id, 203)
			key4, value4 := cursor.Seek([]byte("200"))
			assertEntryReplace(t, key4, value4, []byte("201"), tc.id, 201)
			key5, value5 := cursor.Seek([]byte("404"))
			assertEntryReplaceNil(t, key5, value5)
			key6, value6 := cursor.Seek([]byte("101"))
			assertEntryReplace(t, key6, value6, []byte("201"), tc.id, 201)
		})

		t.Run("mix", func(t *testing.T) {
			key1, value1 := cursor.Seek([]byte("401"))
			assertEntryReplace(t, key1, value1, []byte("401"), tc.id, 401)
			key2, value2 := cursor.First()
			assertEntryReplace(t, key2, value2, []byte("201"), tc.id, 201)
			key3, value3 := cursor.Seek([]byte("666"))
			assertEntryReplaceNil(t, key3, value3)
			key4, value4 := cursor.Next()
			assertEntryReplace(t, key4, value4, []byte("202"), tc.id, 202)
			key5, value5 := cursor.Next()
			assertEntryReplace(t, key5, value5, []byte("203"), tc.id, 203)
			key6, value6 := cursor.First()
			assertEntryReplace(t, key6, value6, []byte("201"), tc.id, 201)
			key7, value7 := cursor.Seek([]byte("402"))
			assertEntryReplace(t, key7, value7, []byte("402"), tc.id, 402)
			key8, value8 := cursor.Next()
			assertEntryReplace(t, key8, value8, []byte("403"), tc.id, 403)
			key9, value9 := cursor.Next()
			assertEntryReplaceNil(t, key9, value9)
			key10, value10 := cursor.Next()
			assertEntryReplaceNil(t, key10, value10)
			key11, value11 := cursor.First()
			assertEntryReplace(t, key11, value11, []byte("201"), tc.id, 201)
			key12, value12 := cursor.Seek([]byte("201"))
			assertEntryReplace(t, key12, value12, []byte("201"), tc.id, 201)
			key13, value13 := cursor.Seek([]byte("403"))
			assertEntryReplace(t, key13, value13, []byte("403"), tc.id, 403)
			key14, value14 := cursor.Next()
			assertEntryReplaceNil(t, key14, value14)
			key15, value15 := cursor.Seek([]byte("403"))
			assertEntryReplace(t, key15, value15, []byte("403"), tc.id, 403)
		})
	}

	for _, tc := range testCases {
		var cursor CursorReplace = newCursorPrefixedReplace(newFakeCursorReplace(), []byte(tc.prefix))

		t.Run("next fallbacks to first", func(t *testing.T) {
			key1, value1 := cursor.Next()
			assertEntryReplace(t, key1, value1, []byte("201"), tc.id, 201)
			key2, value2 := cursor.Next()
			assertEntryReplace(t, key2, value2, []byte("202"), tc.id, 202)
		})
	}
}

func newFakeCursorReplace() CursorReplace {
	entries := []*entry[[]byte]{
		{key: []byte("a201"), value: asBytes(1, 201)},
		{key: []byte("a202"), value: asBytes(1, 202)},
		{key: []byte("a203"), value: asBytes(1, 203)},
		{key: []byte("a401"), value: asBytes(1, 401)},
		{key: []byte("a402"), value: asBytes(1, 402)},
		{key: []byte("a403"), value: asBytes(1, 403)},
		{key: []byte("b201"), value: asBytes(2, 201)},
		{key: []byte("b202"), value: asBytes(2, 202)},
		{key: []byte("b203"), value: asBytes(2, 203)},
		{key: []byte("b401"), value: asBytes(2, 401)},
		{key: []byte("b402"), value: asBytes(2, 402)},
		{key: []byte("b403"), value: asBytes(2, 403)},
		{key: []byte("c201"), value: asBytes(3, 201)},
		{key: []byte("c202"), value: asBytes(3, 202)},
		{key: []byte("c203"), value: asBytes(3, 203)},
		{key: []byte("c401"), value: asBytes(3, 401)},
		{key: []byte("c402"), value: asBytes(3, 402)},
		{key: []byte("c403"), value: asBytes(3, 403)},
	}
	return &fakeCursor[[]byte]{pos: 0, entries: entries}
}

func assertEntryReplace(t *testing.T, key []byte, val []byte, expectedKey []byte, expectedValues ...uint64) {
	require.Equal(t, expectedKey, key)
	require.ElementsMatch(t, asBytes(expectedValues...), val)
}

func assertEntryReplaceNil(t *testing.T, key []byte, val []byte) {
	require.Nil(t, key)
	require.Nil(t, val)
}

func asBytes(values ...uint64) []byte {
	bytes := make([]byte, 8*len(values))
	for i := range values {
		binary.LittleEndian.PutUint64(bytes[i*8:(i+1)*8], values[i])
	}
	return bytes
}

type entry[T any] struct {
	key   []byte
	value T
}

type fakeCursor[T any] struct {
	pos     int
	entries []*entry[T]
}

func (c *fakeCursor[T]) First() ([]byte, T) {
	e := c.entries[0]
	c.pos++
	return e.key, e.value
}

func (c *fakeCursor[T]) Next() ([]byte, T) {
	if c.pos < len(c.entries) {
		e := c.entries[c.pos]
		c.pos++
		return e.key, e.value
	}
	var t0 T
	return nil, t0
}

func (c *fakeCursor[T]) Seek(key []byte) ([]byte, T) {
	for i, e := range c.entries {
		if string(e.key) >= string(key) {
			c.pos = i
			c.pos++
			return e.key, e.value
		}
	}
	var t0 T
	return nil, t0
}

func (c *fakeCursor[T]) Close() {}
