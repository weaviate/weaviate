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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestMapCollectionStrategy(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "mapInsertAndAppend",
			f:    mapInsertAndAppend,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
			},
		},
		{
			name: "mapInsertAndDelete",
			f:    mapInsertAndDelete,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
			},
		},
		{
			name: "mapCursors",
			f:    mapCursors,
			opts: []BucketOption{
				WithStrategy(StrategyMapCollection),
			},
		},
	}
	tests.run(ctx, t)
}

func mapInsertAndAppend(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})

	t.Run("with a single flush between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test2-key-1")
		rowKey2 := []byte("test2-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})

	t.Run("with flushes after initial and update", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test3-key-1")
		rowKey2 := []byte("test3-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)

			// Flush again!
			require.Nil(t, b.FlushAndSwitch())

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})

	t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test4-key-1")
		rowKey2 := []byte("test4-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("replace an existing map key", func(t *testing.T) {
			err = b.MapSet(rowKey1, MapPair{
				Key:   []byte("row1-key1"),        // existing key
				Value: []byte("row1-key1-value2"), // updated value
			})
			require.Nil(t, err)
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.Nil(t, err)

			row1Updated := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value2"), // <--- updated, rest unchanged
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Unchanged := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			res, err := b2.MapList(rowKey1)
			require.Nil(t, err)
			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b2.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Unchanged)
		})
	})
}

func mapInsertAndDelete(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("delete some keys, re-add one of them", func(t *testing.T) {
			err := b.MapDeleteKey(rowKey1, []byte("row1-key1"))
			require.Nil(t, err)
			err = b.MapDeleteKey(rowKey2, []byte("row2-key2"))
			require.Nil(t, err)
			err = b.MapSet(rowKey2, MapPair{
				Key:   []byte("row2-key2"),
				Value: []byte("row2-key2-reinserted"),
			})
			require.Nil(t, err)
		})

		t.Run("validate the results", func(t *testing.T) {
			row1Updated := []MapPair{
				// key 1 was deleted
				{
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Updated := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-reinserted"),
				},
			}

			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.ElementsMatch(t, row2Updated, res)
		})
	})

	t.Run("with flushes between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete some keys, re-add one of them", func(t *testing.T) {
			err := b.MapDeleteKey(rowKey1, []byte("row1-key1"))
			require.Nil(t, err)
			err = b.MapDeleteKey(rowKey2, []byte("row2-key2"))
			require.Nil(t, err)
			err = b.MapSet(rowKey2, MapPair{
				Key:   []byte("row2-key2"),
				Value: []byte("row2-key2-reinserted"),
			})
			require.Nil(t, err)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("validate the results", func(t *testing.T) {
			row1Updated := []MapPair{
				// key 1 was deleted
				{
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Updated := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-reinserted"),
				},
			}

			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.ElementsMatch(t, row2Updated, res)
		})
	})

	t.Run("with memtable only, then an orderly shutdown and restart", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		rowKey1 := []byte("test1-key-1")
		rowKey2 := []byte("test1-key-2")

		t.Run("set original values and verify", func(t *testing.T) {
			row1Map := []MapPair{
				{
					Key:   []byte("row1-key1"),
					Value: []byte("row1-key1-value1"),
				}, {
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Map := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-value1"),
				},
			}

			for _, pair := range row1Map {
				err = b.MapSet(rowKey1, pair)
				require.Nil(t, err)
			}

			for _, pair := range row2Map {
				err = b.MapSet(rowKey2, pair)
				require.Nil(t, err)
			}

			res, err := b.MapList(rowKey1)
			require.Nil(t, err)
			assert.Equal(t, row1Map, res)
			res, err = b.MapList(rowKey2)
			require.Nil(t, err)
			assert.Equal(t, res, row2Map)
		})

		t.Run("delete some keys, re-add one of them", func(t *testing.T) {
			err := b.MapDeleteKey(rowKey1, []byte("row1-key1"))
			require.Nil(t, err)
			err = b.MapDeleteKey(rowKey2, []byte("row2-key2"))
			require.Nil(t, err)
			err = b.MapSet(rowKey2, MapPair{
				Key:   []byte("row2-key2"),
				Value: []byte("row2-key2-reinserted"),
			})
			require.Nil(t, err)
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.Nil(t, err)

			row1Updated := []MapPair{
				// key 1 was deleted
				{
					Key:   []byte("row1-key2"),
					Value: []byte("row1-key2-value1"),
				},
			}

			row2Updated := []MapPair{
				{
					Key:   []byte("row2-key1"),
					Value: []byte("row2-key1-value1"),
				}, {
					Key:   []byte("row2-key2"),
					Value: []byte("row2-key2-reinserted"),
				},
			}

			// NOTE: We are accepting that the order is changed here. Given the name
			// "MapCollection" there should be no expectations regarding the order,
			// but we have yet to validate if this fits with all of the intended use
			// cases.
			res, err := b2.MapList(rowKey1)
			require.Nil(t, err)
			assert.ElementsMatch(t, row1Updated, res)
			res, err = b2.MapList(rowKey2)
			require.Nil(t, err)
			assert.ElementsMatch(t, row2Updated, res)
		})
	})
}

func mapCursors(ctx context.Context, t *testing.T, opts []BucketOption) {
	t.Run("memtable-only", func(t *testing.T) {
		r := getRandomSeed()
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("set original values", func(t *testing.T) {
			pairs := 20
			valuesPerPair := 3
			keys := make([][]byte, pairs)
			values := make([][]MapPair, pairs)

			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("row-%03d", i))
				values[i] = make([]MapPair, valuesPerPair)
				for j := range values[i] {
					values[i][j] = MapPair{
						Key:   []byte(fmt.Sprintf("row-%03d-key-%d", i, j)),
						Value: []byte(fmt.Sprintf("row-%03d-value-%d", i, j)),
					}
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				mapPairs := values[i]
				for j := range mapPairs {
					err = b.MapSet(keys[i], mapPairs[j])
					require.Nil(t, err)
				}
			}
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-016"),
				[]byte("row-017"),
				[]byte("row-018"),
				[]byte("row-019"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-016-key-0"), Value: []byte("row-016-value-0")},
					{Key: []byte("row-016-key-1"), Value: []byte("row-016-value-1")},
					{Key: []byte("row-016-key-2"), Value: []byte("row-016-value-2")},
				},
				{
					{Key: []byte("row-017-key-0"), Value: []byte("row-017-value-0")},
					{Key: []byte("row-017-key-1"), Value: []byte("row-017-value-1")},
					{Key: []byte("row-017-key-2"), Value: []byte("row-017-value-2")},
				},
				{
					{Key: []byte("row-018-key-0"), Value: []byte("row-018-value-0")},
					{Key: []byte("row-018-key-1"), Value: []byte("row-018-value-1")},
					{Key: []byte("row-018-key-2"), Value: []byte("row-018-value-2")},
				},
				{
					{Key: []byte("row-019-key-0"), Value: []byte("row-019-value-0")},
					{Key: []byte("row-019-key-1"), Value: []byte("row-019-value-1")},
					{Key: []byte("row-019-key-2"), Value: []byte("row-019-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			for k, v := c.Seek([]byte("row-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})

		t.Run("start from beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-000"),
				[]byte("row-001"),
				[]byte("row-002"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-000-key-0"), Value: []byte("row-000-value-0")},
					{Key: []byte("row-000-key-1"), Value: []byte("row-000-value-1")},
					{Key: []byte("row-000-key-2"), Value: []byte("row-000-value-2")},
				},
				{
					{Key: []byte("row-001-key-0"), Value: []byte("row-001-value-0")},
					{Key: []byte("row-001-key-1"), Value: []byte("row-001-value-1")},
					{Key: []byte("row-001-key-2"), Value: []byte("row-001-value-2")},
				},
				{
					{Key: []byte("row-002-key-0"), Value: []byte("row-002-value-0")},
					{Key: []byte("row-002-key-1"), Value: []byte("row-002-value-1")},
					{Key: []byte("row-002-key-2"), Value: []byte("row-002-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})

		t.Run("delete/replace an existing map key/value pair", func(t *testing.T) {
			row := []byte("row-002")
			pair := MapPair{
				Key:   []byte("row-002-key-1"),           // existing key
				Value: []byte("row-002-value-1-updated"), // updated value
			}

			require.Nil(t, b.MapSet(row, pair))

			row = []byte("row-001")
			key := []byte("row-001-key-1")

			require.Nil(t, b.MapDeleteKey(row, key))
		})

		t.Run("verify update is contained", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-001"),
				[]byte("row-002"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-001-key-0"), Value: []byte("row-001-value-0")},
					// key-1 was deleted
					{Key: []byte("row-001-key-2"), Value: []byte("row-001-value-2")},
				},
				{
					{Key: []byte("row-002-key-0"), Value: []byte("row-002-value-0")},
					{Key: []byte("row-002-key-1"), Value: []byte("row-002-value-1-updated")},
					{Key: []byte("row-002-key-2"), Value: []byte("row-002-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("row-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})
	})

	t.Run("with flushes", func(t *testing.T) {
		r := getRandomSeed()
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		t.Run("first third (%3==0)", func(t *testing.T) {
			pairs := 20
			valuesPerPair := 3
			var keys [][]byte
			var values [][]MapPair

			for i := 0; i < pairs; i++ {
				if i%3 != 0 {
					continue
				}

				keys = append(keys, []byte(fmt.Sprintf("row-%03d", i)))
				curValues := make([]MapPair, valuesPerPair)
				for j := range curValues {
					curValues[j] = MapPair{
						Key:   []byte(fmt.Sprintf("row-%03d-key-%d", i, j)),
						Value: []byte(fmt.Sprintf("row-%03d-value-%d", i, j)),
					}
				}

				values = append(values, curValues)
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				mapPairs := values[i]
				for j := range mapPairs {
					err = b.MapSet(keys[i], mapPairs[j])
					require.Nil(t, err)
				}
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("second third (%3==1)", func(t *testing.T) {
			pairs := 20
			valuesPerPair := 3
			var keys [][]byte
			var values [][]MapPair

			for i := 0; i < pairs; i++ {
				if i%3 != 1 {
					continue
				}

				keys = append(keys, []byte(fmt.Sprintf("row-%03d", i)))
				curValues := make([]MapPair, valuesPerPair)
				for j := range curValues {
					curValues[j] = MapPair{
						Key:   []byte(fmt.Sprintf("row-%03d-key-%d", i, j)),
						Value: []byte(fmt.Sprintf("row-%03d-value-%d", i, j)),
					}
				}

				values = append(values, curValues)
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				mapPairs := values[i]
				for j := range mapPairs {
					err = b.MapSet(keys[i], mapPairs[j])
					require.Nil(t, err)
				}
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("third third (%3==2) memtable only", func(t *testing.T) {
			pairs := 20
			valuesPerPair := 3
			var keys [][]byte
			var values [][]MapPair

			for i := 0; i < pairs; i++ {
				if i%3 != 2 {
					continue
				}

				keys = append(keys, []byte(fmt.Sprintf("row-%03d", i)))
				curValues := make([]MapPair, valuesPerPair)
				for j := range curValues {
					curValues[j] = MapPair{
						Key:   []byte(fmt.Sprintf("row-%03d-key-%d", i, j)),
						Value: []byte(fmt.Sprintf("row-%03d-value-%d", i, j)),
					}
				}

				values = append(values, curValues)
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				mapPairs := values[i]
				for j := range mapPairs {
					err = b.MapSet(keys[i], mapPairs[j])
					require.Nil(t, err)
				}
			}

			// no flush for this one, so this segment stays in the memtable
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-016"),
				[]byte("row-017"),
				[]byte("row-018"),
				[]byte("row-019"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-016-key-0"), Value: []byte("row-016-value-0")},
					{Key: []byte("row-016-key-1"), Value: []byte("row-016-value-1")},
					{Key: []byte("row-016-key-2"), Value: []byte("row-016-value-2")},
				},
				{
					{Key: []byte("row-017-key-0"), Value: []byte("row-017-value-0")},
					{Key: []byte("row-017-key-1"), Value: []byte("row-017-value-1")},
					{Key: []byte("row-017-key-2"), Value: []byte("row-017-value-2")},
				},
				{
					{Key: []byte("row-018-key-0"), Value: []byte("row-018-value-0")},
					{Key: []byte("row-018-key-1"), Value: []byte("row-018-value-1")},
					{Key: []byte("row-018-key-2"), Value: []byte("row-018-value-2")},
				},
				{
					{Key: []byte("row-019-key-0"), Value: []byte("row-019-value-0")},
					{Key: []byte("row-019-key-1"), Value: []byte("row-019-value-1")},
					{Key: []byte("row-019-key-2"), Value: []byte("row-019-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			for k, v := c.Seek([]byte("row-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})

		t.Run("start from beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-000"),
				[]byte("row-001"),
				[]byte("row-002"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-000-key-0"), Value: []byte("row-000-value-0")},
					{Key: []byte("row-000-key-1"), Value: []byte("row-000-value-1")},
					{Key: []byte("row-000-key-2"), Value: []byte("row-000-value-2")},
				},
				{
					{Key: []byte("row-001-key-0"), Value: []byte("row-001-value-0")},
					{Key: []byte("row-001-key-1"), Value: []byte("row-001-value-1")},
					{Key: []byte("row-001-key-2"), Value: []byte("row-001-value-2")},
				},
				{
					{Key: []byte("row-002-key-0"), Value: []byte("row-002-value-0")},
					{Key: []byte("row-002-key-1"), Value: []byte("row-002-value-1")},
					{Key: []byte("row-002-key-2"), Value: []byte("row-002-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})

		t.Run("delete/replace an existing map key/value pair", func(t *testing.T) {
			row := []byte("row-002")
			pair := MapPair{
				Key:   []byte("row-002-key-1"),           // existing key
				Value: []byte("row-002-value-1-updated"), // updated value
			}

			require.Nil(t, b.MapSet(row, pair))

			row = []byte("row-001")
			key := []byte("row-001-key-1")

			require.Nil(t, b.MapDeleteKey(row, key))
		})

		t.Run("verify update is contained", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-001"),
				[]byte("row-002"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-001-key-0"), Value: []byte("row-001-value-0")},
					// key-1 was deleted
					{Key: []byte("row-001-key-2"), Value: []byte("row-001-value-2")},
				},
				{
					{Key: []byte("row-002-key-0"), Value: []byte("row-002-value-0")},
					{Key: []byte("row-002-key-1"), Value: []byte("row-002-value-1-updated")},
					{Key: []byte("row-002-key-2"), Value: []byte("row-002-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("row-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})

		t.Run("one final flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("verify update is contained - after flushing the update", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("row-001"),
				[]byte("row-002"),
			}
			expectedValues := [][]MapPair{
				{
					{Key: []byte("row-001-key-0"), Value: []byte("row-001-value-0")},
					// key-1 was deleted
					{Key: []byte("row-001-key-2"), Value: []byte("row-001-value-2")},
				},
				{
					{Key: []byte("row-002-key-0"), Value: []byte("row-002-value-0")},
					{Key: []byte("row-002-key-1"), Value: []byte("row-002-value-1-updated")},
					{Key: []byte("row-002-key-2"), Value: []byte("row-002-value-2")},
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][]MapPair
			c := b.MapCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("row-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)

			require.Equal(t, len(expectedValues), len(retrievedValues))
			for i := range expectedValues {
				assert.ElementsMatch(t, expectedValues[i], retrievedValues[i])
			}
		})
	})
}
