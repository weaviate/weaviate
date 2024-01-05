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

func TestSetCollectionStrategy(t *testing.T) {
	ctx := testCtx()
	tests := bucketIntegrationTests{
		{
			name: "collectionInsertAndSetAdd",
			f:    collectionInsertAndSetAdd,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
			},
		},
		{
			name: "collectionInsertAndSetAddInsertAndDelete",
			f:    collectionInsertAndSetAddInsertAndDelete,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
			},
		},
		{
			name: "collectionCursors",
			f:    collectionCursors,
			opts: []BucketOption{
				WithStrategy(StrategySetCollection),
			},
		},
	}
	tests.run(ctx, t)
}

func collectionInsertAndSetAdd(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
			append2 := [][]byte{[]byte("value 2.3")}
			append3 := [][]byte{[]byte("value 3.3")}

			err = b.SetAdd(key2, append2)
			require.Nil(t, err)
			err = b.SetAdd(key3, append3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, append(orig2, append2...), res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, append(orig3, append3...), res)
		})
	})

	t.Run("with a single flush between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test2-key-1")
		key2 := []byte("test2-key-2")
		key3 := []byte("test2-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
			append2 := [][]byte{[]byte("value 2.3")}
			append3 := [][]byte{[]byte("value 3.3")}

			err = b.SetAdd(key2, append2)
			require.Nil(t, err)
			err = b.SetAdd(key3, append3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, append(orig2, append2...), res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, append(orig3, append3...), res)
		})
	})

	t.Run("with flushes after initial and update", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)
		key1 := []byte("test-3-key-1")
		key2 := []byte("test-3-key-2")
		key3 := []byte("test-3-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
			append2 := [][]byte{[]byte("value 2.3")}
			append3 := [][]byte{[]byte("value 3.3")}

			err = b.SetAdd(key2, append2)
			require.Nil(t, err)
			err = b.SetAdd(key3, append3)
			require.Nil(t, err)

			// Flush again!
			require.Nil(t, b.FlushAndSwitch())

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, append(orig2, append2...), res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, append(orig3, append3...), res)
		})
	})

	t.Run("update in memtable, then do an orderly shutdown, and re-init", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test4-key-1")
		key2 := []byte("test4-key-2")
		key3 := []byte("test4-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("replace some, keep one", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
			append2 := [][]byte{[]byte("value 2.3")}
			append3 := [][]byte{[]byte("value 3.3")}

			err = b.SetAdd(key2, append2)
			require.Nil(t, err)
			err = b.SetAdd(key3, append3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, append(orig2, append2...), res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, append(orig3, append3...), res)
		})

		t.Run("orderly shutdown", func(t *testing.T) {
			b.Shutdown(context.Background())
		})

		t.Run("init another bucket on the same files", func(t *testing.T) {
			b2, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.Nil(t, err)

			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}
			append2 := [][]byte{[]byte("value 2.3")}
			append3 := [][]byte{[]byte("value 3.3")}

			res, err := b2.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b2.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, append(orig2, append2...), res)
			res, err = b2.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, append(orig3, append3...), res)
		})
	})
}

func collectionInsertAndSetAddInsertAndDelete(ctx context.Context, t *testing.T, opts []BucketOption) {
	dirName := t.TempDir()

	t.Run("memtable-only", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test1-key-1")
		key2 := []byte("test1-key-2")
		key3 := []byte("test1-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, orig1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, orig2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, orig3, res)
		})

		t.Run("delete individual keys", func(t *testing.T) {
			delete2 := []byte("value 2.1")
			delete3 := []byte("value 3.2")

			err = b.SetDeleteSingle(key2, delete2)
			require.Nil(t, err)
			err = b.SetDeleteSingle(key3, delete3)
			require.Nil(t, err)
		})

		t.Run("validate the results", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{[]byte("value 2.2")}                      // value1 deleted
			expected3 := [][]byte{[]byte("value 3.1")}                      // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})

		t.Run("re-add keys which were previously deleted and new ones", func(t *testing.T) {
			readd2 := [][]byte{[]byte("value 2.1"), []byte("value 2.3")}
			readd3 := [][]byte{[]byte("value 3.2"), []byte("value 3.3")}

			err = b.SetAdd(key2, readd2)
			require.Nil(t, err)
			err = b.SetAdd(key3, readd3)
			require.Nil(t, err)
		})

		t.Run("validate the results again", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{
				[]byte("value 2.2"), // from original import
				[]byte("value 2.1"), // added again after initial deletion
				[]byte("value 2.3"), // newly added
			}
			expected3 := [][]byte{
				[]byte("value 3.1"), // form original import
				[]byte("value 3.2"), // added again after initial deletion
				[]byte("value 3.3"), // newly added
			} // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})
	})

	t.Run("with a single flush between updates", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test2-key-1")
		key2 := []byte("test2-key-2")
		key3 := []byte("test2-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete individual keys", func(t *testing.T) {
			delete2 := []byte("value 2.1")
			delete3 := []byte("value 3.2")

			err = b.SetDeleteSingle(key2, delete2)
			require.Nil(t, err)
			err = b.SetDeleteSingle(key3, delete3)
			require.Nil(t, err)
		})

		t.Run("validate the results", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{[]byte("value 2.2")}                      // value1 deleted
			expected3 := [][]byte{[]byte("value 3.1")}                      // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})

		t.Run("re-add keys which were previously deleted and new ones", func(t *testing.T) {
			readd2 := [][]byte{[]byte("value 2.1"), []byte("value 2.3")}
			readd3 := [][]byte{[]byte("value 3.2"), []byte("value 3.3")}

			err = b.SetAdd(key2, readd2)
			require.Nil(t, err)
			err = b.SetAdd(key3, readd3)
			require.Nil(t, err)
		})

		t.Run("validate the results again", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{
				[]byte("value 2.2"), // from original import
				[]byte("value 2.1"), // added again after initial deletion
				[]byte("value 2.3"), // newly added
			}
			expected3 := [][]byte{
				[]byte("value 3.1"), // form original import
				[]byte("value 3.2"), // added again after initial deletion
				[]byte("value 3.3"), // newly added
			} // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})
	})

	t.Run("with flushes in between and after the update", func(t *testing.T) {
		b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		key1 := []byte("test3-key-1")
		key2 := []byte("test3-key-2")
		key3 := []byte("test3-key-3")

		t.Run("set original values and verify", func(t *testing.T) {
			orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
			orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
			orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

			err = b.SetAdd(key1, orig1)
			require.Nil(t, err)
			err = b.SetAdd(key2, orig2)
			require.Nil(t, err)
			err = b.SetAdd(key3, orig3)
			require.Nil(t, err)

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, res, orig1)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, res, orig2)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, res, orig3)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("delete individual keys", func(t *testing.T) {
			delete2 := []byte("value 2.1")
			delete3 := []byte("value 3.2")

			err = b.SetDeleteSingle(key2, delete2)
			require.Nil(t, err)
			err = b.SetDeleteSingle(key3, delete3)
			require.Nil(t, err)
		})

		t.Run("flush to disk - again!", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("validate", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{[]byte("value 2.2")}                      // value1 deleted
			expected3 := [][]byte{[]byte("value 3.1")}                      // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})

		t.Run("re-add keys which were previously deleted and new ones", func(t *testing.T) {
			readd2 := [][]byte{[]byte("value 2.1"), []byte("value 2.3")}
			readd3 := [][]byte{[]byte("value 3.2"), []byte("value 3.3")}

			err = b.SetAdd(key2, readd2)
			require.Nil(t, err)
			err = b.SetAdd(key3, readd3)
			require.Nil(t, err)
		})

		t.Run("flush to disk - yet again!", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("validate the results again", func(t *testing.T) {
			expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
			expected2 := [][]byte{
				[]byte("value 2.2"), // from original import
				[]byte("value 2.1"), // added again after initial deletion
				[]byte("value 2.3"), // newly added
			}
			expected3 := [][]byte{
				[]byte("value 3.1"), // form original import
				[]byte("value 3.2"), // added again after initial deletion
				[]byte("value 3.3"), // newly added
			} // value2 deleted

			res, err := b.SetList(key1)
			require.Nil(t, err)
			assert.Equal(t, expected1, res)
			res, err = b.SetList(key2)
			require.Nil(t, err)
			assert.Equal(t, expected2, res)
			res, err = b.SetList(key3)
			require.Nil(t, err)
			assert.Equal(t, expected3, res)
		})
	})

	t.Run("update in memtable, make orderly shutdown, then create a new bucket from disk",
		func(t *testing.T) {
			b, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
			require.Nil(t, err)

			// so big it effectively never triggers as part of this test
			b.SetMemtableThreshold(1e9)

			key1 := []byte("test4-key-1")
			key2 := []byte("test4-key-2")
			key3 := []byte("test4-key-3")

			t.Run("set original values and verify", func(t *testing.T) {
				orig1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")}
				orig2 := [][]byte{[]byte("value 2.1"), []byte("value 2.2")}
				orig3 := [][]byte{[]byte("value 3.1"), []byte("value 3.2")}

				err = b.SetAdd(key1, orig1)
				require.Nil(t, err)
				err = b.SetAdd(key2, orig2)
				require.Nil(t, err)
				err = b.SetAdd(key3, orig3)
				require.Nil(t, err)

				res, err := b.SetList(key1)
				require.Nil(t, err)
				assert.Equal(t, res, orig1)
				res, err = b.SetList(key2)
				require.Nil(t, err)
				assert.Equal(t, res, orig2)
				res, err = b.SetList(key3)
				require.Nil(t, err)
				assert.Equal(t, res, orig3)
			})

			t.Run("delete individual keys", func(t *testing.T) {
				delete2 := []byte("value 2.1")
				delete3 := []byte("value 3.2")

				err = b.SetDeleteSingle(key2, delete2)
				require.Nil(t, err)
				err = b.SetDeleteSingle(key3, delete3)
				require.Nil(t, err)
			})

			t.Run("orderly shutdown", func(t *testing.T) {
				b.Shutdown(context.Background())
			})

			t.Run("init another bucket on the same files", func(t *testing.T) {
				b2, err := NewBucket(ctx, dirName, "", nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				expected1 := [][]byte{[]byte("value 1.1"), []byte("value 1.2")} // unchanged
				expected2 := [][]byte{[]byte("value 2.2")}                      // value1 deleted
				expected3 := [][]byte{[]byte("value 3.1")}                      // value2 deleted

				res, err := b2.SetList(key1)
				require.Nil(t, err)
				assert.Equal(t, expected1, res)
				res, err = b2.SetList(key2)
				require.Nil(t, err)
				assert.Equal(t, expected2, res)
				res, err = b2.SetList(key3)
				require.Nil(t, err)
				assert.Equal(t, expected3, res)
			})
		})
}

func collectionCursors(ctx context.Context, t *testing.T, opts []BucketOption) {
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
			values := make([][][]byte, pairs)

			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
				values[i] = make([][]byte, valuesPerPair)
				for j := range values[i] {
					values[i][j] = []byte(fmt.Sprintf("value-%03d.%d", i, j))
				}
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.SetAdd(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][][]byte{
				{[]byte("value-016.0"), []byte("value-016.1"), []byte("value-016.2")},
				{[]byte("value-017.0"), []byte("value-017.1"), []byte("value-017.2")},
				{[]byte("value-018.0"), []byte("value-018.1"), []byte("value-018.2")},
				{[]byte("value-019.0"), []byte("value-019.1"), []byte("value-019.2")},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][][]byte{
				{[]byte("value-000.0"), []byte("value-000.1"), []byte("value-000.2")},
				{[]byte("value-001.0"), []byte("value-001.1"), []byte("value-001.2")},
				{[]byte("value-002.0"), []byte("value-002.1"), []byte("value-002.2")},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("extend an existing key", func(t *testing.T) {
			key := []byte("key-002")
			extend := [][]byte{[]byte("value-002.3")}

			require.Nil(t, b.SetAdd(key, extend))
		})

		t.Run("verify the extension is contained", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][][]byte{
				{[]byte("value-001.0"), []byte("value-001.1"), []byte("value-001.2")},
				{
					[]byte("value-002.0"), []byte("value-002.1"), []byte("value-002.2"),
					[]byte("value-002.3"),
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("key-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
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
			var values [][][]byte

			for i := 0; i < pairs; i++ {
				if i%3 != 0 {
					continue
				}
				keys = append(keys, []byte(fmt.Sprintf("key-%03d", i)))
				curValues := make([][]byte, valuesPerPair)
				for j := range curValues {
					curValues[j] = []byte(fmt.Sprintf("value-%03d.%d", i, j))
				}
				values = append(values, curValues)
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.SetAdd(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("second third (%3==1)", func(t *testing.T) {
			pairs := 20
			valuesPerPair := 3
			var keys [][]byte
			var values [][][]byte

			for i := 0; i < pairs; i++ {
				if i%3 != 1 {
					continue
				}
				keys = append(keys, []byte(fmt.Sprintf("key-%03d", i)))
				curValues := make([][]byte, valuesPerPair)
				for j := range curValues {
					curValues[j] = []byte(fmt.Sprintf("value-%03d.%d", i, j))
				}
				values = append(values, curValues)
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.SetAdd(keys[i], values[i])
				require.Nil(t, err)
			}
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("third (%3==2) memtable-only", func(t *testing.T) {
			pairs := 20
			valuesPerPair := 3
			var keys [][]byte
			var values [][][]byte

			for i := 0; i < pairs; i++ {
				if i%3 != 2 {
					continue
				}
				keys = append(keys, []byte(fmt.Sprintf("key-%03d", i)))
				curValues := make([][]byte, valuesPerPair)
				for j := range curValues {
					curValues[j] = []byte(fmt.Sprintf("value-%03d.%d", i, j))
				}
				values = append(values, curValues)
			}

			// shuffle to make sure the BST isn't accidentally in order
			r.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			})

			for i := range keys {
				err = b.SetAdd(keys[i], values[i])
				require.Nil(t, err)
			}

			// no flush for this one, so this segment stays in the memtable
		})

		t.Run("seek from somewhere in the middle", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-016"),
				[]byte("key-017"),
				[]byte("key-018"),
				[]byte("key-019"),
			}
			expectedValues := [][][]byte{
				{[]byte("value-016.0"), []byte("value-016.1"), []byte("value-016.2")},
				{[]byte("value-017.0"), []byte("value-017.1"), []byte("value-017.2")},
				{[]byte("value-018.0"), []byte("value-018.1"), []byte("value-018.2")},
				{[]byte("value-019.0"), []byte("value-019.1"), []byte("value-019.2")},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			for k, v := c.Seek([]byte("key-016")); k != nil; k, v = c.Next() {
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("start from the beginning", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-000"),
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][][]byte{
				{[]byte("value-000.0"), []byte("value-000.1"), []byte("value-000.2")},
				{[]byte("value-001.0"), []byte("value-001.1"), []byte("value-001.2")},
				{[]byte("value-002.0"), []byte("value-002.1"), []byte("value-002.2")},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.First(); k != nil && retrieved < 3; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("delete & extend an existing key", func(t *testing.T) {
			key := []byte("key-002")
			extend := [][]byte{[]byte("value-002.3")}

			require.Nil(t, b.SetAdd(key, extend))

			key = []byte("key-001")
			deleteValue := []byte("value-001.1")
			require.Nil(t, b.SetDeleteSingle(key, deleteValue))
		})

		t.Run("verify the extension is contained", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][][]byte{
				{
					[]byte("value-001.0"),
					// "value-001.1" deleted
					[]byte("value-001.2"),
				},
				{
					[]byte("value-002.0"), []byte("value-002.1"), []byte("value-002.2"),
					[]byte("value-002.3"),
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("key-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})

		t.Run("flush to disk", func(t *testing.T) {
			require.Nil(t, b.FlushAndSwitch())
		})

		t.Run("verify again after flush", func(t *testing.T) {
			expectedKeys := [][]byte{
				[]byte("key-001"),
				[]byte("key-002"),
			}
			expectedValues := [][][]byte{
				{
					[]byte("value-001.0"),
					// "value-001.1" deleted
					[]byte("value-001.2"),
				},
				{
					[]byte("value-002.0"), []byte("value-002.1"), []byte("value-002.2"),
					[]byte("value-002.3"),
				},
			}

			var retrievedKeys [][]byte
			var retrievedValues [][][]byte
			c := b.SetCursor()
			defer c.Close()
			retrieved := 0
			for k, v := c.Seek([]byte("key-001")); k != nil && retrieved < 2; k, v = c.Next() {
				retrieved++
				retrievedKeys = append(retrievedKeys, k)
				retrievedValues = append(retrievedValues, v)
			}

			assert.Equal(t, expectedKeys, retrievedKeys)
			assert.Equal(t, expectedValues, retrievedValues)
		})
	})
}
