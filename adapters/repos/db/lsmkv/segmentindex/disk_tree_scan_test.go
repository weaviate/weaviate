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

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// variedKeys returns n sorted keys of varying length, so node sizes in the
// serialized tree differ and range splits land on uneven boundaries.
func variedKeys(n int) []Key {
	keys := make([]Key, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%06d%s", i, strings.Repeat("x", i%9))
		keys[i] = Key{Key: []byte(k), ValueStart: i * 10, ValueEnd: i*10 + 5}
	}
	return keys
}

func marshalTree(t *testing.T, keys []Key) []byte {
	t.Helper()
	var buf bytes.Buffer
	_, err := MarshalSortedKeysFromKeys(&buf, keys)
	require.NoError(t, err)
	return buf.Bytes()
}

// visited maps key -> [start, end, count].
func walkRanges(t *testing.T, dt *DiskTree, ranges [][2]int) map[string][3]uint64 {
	t.Helper()
	visited := map[string][3]uint64{}
	for _, r := range ranges {
		require.NoError(t, dt.ForEachNodeInRange(r[0], r[1], func(key []byte, start, end uint64) error {
			prev := visited[string(key)]
			visited[string(key)] = [3]uint64{start, end, prev[2] + 1}
			return nil
		}))
	}
	return visited
}

func requireAllNodesOnce(t *testing.T, keys []Key, visited map[string][3]uint64) {
	t.Helper()
	require.Len(t, visited, len(keys))
	for _, k := range keys {
		got, ok := visited[string(k.Key)]
		require.True(t, ok, "key %q not visited", k.Key)
		require.Equal(t, [3]uint64{uint64(k.ValueStart), uint64(k.ValueEnd), 1}, got,
			"key %q: wrong start/end or visited more than once", k.Key)
	}
}

func TestDiskTreeForEachNodeInRange(t *testing.T) {
	keys := variedKeys(100)
	data := marshalTree(t, keys)
	dt := NewDiskTree(data)

	t.Run("full range visits every node exactly once", func(t *testing.T) {
		requireAllNodesOnce(t, keys, walkRanges(t, dt, [][2]int{{0, len(data)}}))
	})

	t.Run("empty tree and empty range", func(t *testing.T) {
		require.NoError(t, NewDiskTree(nil).ForEachNodeInRange(0, 0, func([]byte, uint64, uint64) error {
			t.Fatal("callback must not run")
			return nil
		}))
		require.NoError(t, dt.ForEachNodeInRange(7, 7, func([]byte, uint64, uint64) error {
			t.Fatal("callback must not run")
			return nil
		}))
	})

	t.Run("out-of-bounds ranges error", func(t *testing.T) {
		noop := func([]byte, uint64, uint64) error { return nil }
		require.Error(t, dt.ForEachNodeInRange(-1, len(data), noop))
		require.Error(t, dt.ForEachNodeInRange(0, len(data)+1, noop))
		require.Error(t, dt.ForEachNodeInRange(5, 2, noop))
	})

	t.Run("fn error aborts and propagates", func(t *testing.T) {
		sentinel := errors.New("stop here")
		calls := 0
		err := dt.ForEachNodeInRange(0, len(data), func([]byte, uint64, uint64) error {
			calls++
			if calls == 3 {
				return sentinel
			}
			return nil
		})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 3, calls)
	})

	t.Run("misaligned range end errors", func(t *testing.T) {
		err := dt.ForEachNodeInRange(0, len(data)-1, func([]byte, uint64, uint64) error { return nil })
		require.Error(t, err)
	})

	t.Run("corrupt keyLen errors instead of stopping silently", func(t *testing.T) {
		corrupt := bytes.Clone(data)
		binary.LittleEndian.PutUint32(corrupt[0:4], 0xFFFFFFFF)
		err := NewDiskTree(corrupt).ForEachNodeInRange(0, len(corrupt), func([]byte, uint64, uint64) error {
			t.Fatal("callback must not run on a corrupt first node")
			return nil
		})
		require.ErrorContains(t, err, "key len")
	})
}

func TestDiskTreeSplitNodeRanges(t *testing.T) {
	for _, numKeys := range []int{0, 1, 2, 5, 100} {
		for _, parts := range []int{0, 1, 2, 3, 4, 7, 16, 1000} {
			t.Run(fmt.Sprintf("keys=%d parts=%d", numKeys, parts), func(t *testing.T) {
				keys := variedKeys(numKeys)
				data := marshalTree(t, keys)
				dt := NewDiskTree(data)

				ranges := dt.SplitNodeRanges(parts)
				if numKeys == 0 {
					require.Nil(t, ranges)
					return
				}
				if parts <= 1 {
					require.Equal(t, [][2]int{{0, len(data)}}, ranges)
				}
				require.LessOrEqual(t, len(ranges), max(parts, 1))
				require.LessOrEqual(t, len(ranges), numKeys)

				// contiguous, non-empty, covering [0, len(data)) exactly
				require.Equal(t, 0, ranges[0][0])
				require.Equal(t, len(data), ranges[len(ranges)-1][1])
				for i, r := range ranges {
					require.Less(t, r[0], r[1], "empty range at %d", i)
					if i > 0 {
						require.Equal(t, ranges[i-1][1], r[0], "gap/overlap at %d", i)
					}
				}

				requireAllNodesOnce(t, keys, walkRanges(t, dt, ranges))
			})
		}
	}

	t.Run("corrupt node mid-buffer leaves the tail to the last range", func(t *testing.T) {
		keys := variedKeys(10)
		data := marshalTree(t, keys)
		// corrupt the second serialized node's keyLen
		firstNodeSize := int(binary.LittleEndian.Uint32(data[0:4])) + TREE_KEY_STORE_OVERHEAD
		corrupt := bytes.Clone(data)
		binary.LittleEndian.PutUint32(corrupt[firstNodeSize:], 0xFFFFFFFF)
		dt := NewDiskTree(corrupt)

		ranges := dt.SplitNodeRanges(4)
		require.NotEmpty(t, ranges)
		require.Equal(t, 0, ranges[0][0])
		require.Equal(t, len(corrupt), ranges[len(ranges)-1][1])
		for i := 1; i < len(ranges); i++ {
			require.Equal(t, ranges[i-1][1], ranges[i][0])
		}

		// scanning the ranges must surface the corruption rather than skip nodes
		var err error
		for _, r := range ranges {
			if err = dt.ForEachNodeInRange(r[0], r[1], func([]byte, uint64, uint64) error {
				return nil
			}); err != nil {
				break
			}
		}
		require.ErrorContains(t, err, "key len")
	})
}

func TestDiskTreeContains(t *testing.T) {
	keys := variedKeys(50)
	data := marshalTree(t, keys)
	dt := NewDiskTree(data)

	t.Run("present and absent keys", func(t *testing.T) {
		for _, k := range keys {
			has, err := dt.Contains(k.Key)
			require.NoError(t, err)
			require.True(t, has, "key %q", k.Key)
		}
		for _, absent := range [][]byte{nil, []byte(""), []byte("00000"), []byte("000001"), []byte("999999")} {
			has, err := dt.Contains(absent)
			require.NoError(t, err)
			require.False(t, has, "key %q", absent)
		}
	})

	t.Run("empty tree", func(t *testing.T) {
		has, err := NewDiskTree(nil).Contains([]byte("anything"))
		require.NoError(t, err)
		require.False(t, has)
	})

	// Contains shares Get's descent, so on any blob the two must agree: a Get
	// match is a Contains hit, NotFound is a miss, and corruption errors both.
	t.Run("error parity with Get on corrupt and truncated blobs", func(t *testing.T) {
		corruptKeyLen := bytes.Clone(data)
		binary.LittleEndian.PutUint32(corruptKeyLen[0:4], 0xFFFFFFFF)
		blobs := [][]byte{corruptKeyLen}
		for trunc := 0; trunc <= len(data); trunc += 7 {
			blobs = append(blobs, data[:trunc])
		}
		queries := [][]byte{[]byte("000000"), keys[0].Key, keys[25].Key, keys[49].Key, []byte("zzz")}
		for _, blob := range blobs {
			blobTree := NewDiskTree(blob)
			for _, q := range queries {
				_, getErr := blobTree.Get(q)
				has, containsErr := blobTree.Contains(q)
				switch {
				case getErr == nil:
					assert.NoError(t, containsErr)
					assert.True(t, has)
				case errors.Is(getErr, lsmkv.NotFound):
					assert.NoError(t, containsErr)
					assert.False(t, has)
				default:
					require.Error(t, containsErr)
					assert.Equal(t, getErr.Error(), containsErr.Error())
				}
			}
		}
	})
}
