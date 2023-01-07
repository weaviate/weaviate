//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoaringSetStrategy_InsertAndSetAdd(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
			WithStrategy(StrategyRoaringSet))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := []uint64{1, 2}
			orig2 := []uint64{3, 4}
			orig3 := []uint64{5, 6}

			err = b.RoaringSetAddList(key1, orig1)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key2, orig2)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key3, orig3)
			require.Nil(t, err)

			res, err := b.RoaringSetGet(key1)
			require.Nil(t, err)
			for _, testVal := range orig1 {
				assert.True(t, res.Contains(testVal))
			}

			res, err = b.RoaringSetGet(key2)
			require.Nil(t, err)
			for _, testVal := range orig2 {
				assert.True(t, res.Contains(testVal))
			}

			res, err = b.RoaringSetGet(key3)
			require.Nil(t, err)
			for _, testVal := range orig3 {
				assert.True(t, res.Contains(testVal))
			}
		})

		t.Run("extend some, delete some, keep some", func(t *testing.T) {
			additions2 := []uint64{5}
			removal3 := uint64(5)

			err = b.RoaringSetAddList(key2, additions2)
			require.Nil(t, err)
			err = b.RoaringSetRemoveOne(key3, removal3)
			require.Nil(t, err)

			res, err := b.RoaringSetGet(key1)
			require.Nil(t, err)
			for _, testVal := range []uint64{1, 2} { // unchanged values
				assert.True(t, res.Contains(testVal))
			}

			res, err = b.RoaringSetGet(key2)
			require.Nil(t, err)
			for _, testVal := range []uint64{3, 4, 5} { // ectended with 5
				assert.True(t, res.Contains(testVal))
			}

			res, err = b.RoaringSetGet(key3)
			require.Nil(t, err)
			for _, testVal := range []uint64{6} { // fewer remain
				assert.True(t, res.Contains(testVal))
			}
			for _, testVal := range []uint64{5} { // no longer contained
				assert.False(t, res.Contains(testVal))
			}
		})
	})

	t.Run("with a single flush in between updates", func(t *testing.T) {
		b, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
			WithStrategy(StrategyRoaringSet))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := []uint64{1, 2}
			orig2 := []uint64{3, 4}
			orig3 := []uint64{5, 6}

			err = b.RoaringSetAddList(key1, orig1)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key2, orig2)
			require.Nil(t, err)
			err = b.RoaringSetAddList(key3, orig3)
			require.Nil(t, err)

			res, err := b.RoaringSetGet(key1)
			require.Nil(t, err)
			for _, testVal := range orig1 {
				assert.True(t, res.Contains(testVal))
			}

			res, err = b.RoaringSetGet(key2)
			require.Nil(t, err)
			for _, testVal := range orig2 {
				assert.True(t, res.Contains(testVal))
			}

			res, err = b.RoaringSetGet(key3)
			require.Nil(t, err)
			for _, testVal := range orig3 {
				assert.True(t, res.Contains(testVal))
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		// TODO: enable when reading supports disk segments
		// t.Run("extend some, delete some, keep some", func(t *testing.T) {
		// 	additions2 := []uint64{5}
		// 	removal3 := uint64(5)

		// 	err = b.RoaringSetAddList(key2, additions2)
		// 	require.Nil(t, err)
		// 	err = b.RoaringSetRemoveOne(key3, removal3)
		// 	require.Nil(t, err)

		// 	res, err := b.RoaringSetGet(key1)
		// 	require.Nil(t, err)
		// 	for _, testVal := range []uint64{1, 2} { // unchanged values
		// 		assert.True(t, res.Contains(testVal))
		// 	}

		// 	res, err = b.RoaringSetGet(key2)
		// 	require.Nil(t, err)
		// 	for _, testVal := range []uint64{3, 4, 5} { // ectended with 5
		// 		assert.True(t, res.Contains(testVal))
		// 	}

		// 	res, err = b.RoaringSetGet(key3)
		// 	require.Nil(t, err)
		// 	for _, testVal := range []uint64{6} { // fewer remain
		// 		assert.True(t, res.Contains(testVal))
		// 	}
		// 	for _, testVal := range []uint64{5} { // no longer contained
		// 		assert.False(t, res.Contains(testVal))
		// 	}
		// })
	})
}
