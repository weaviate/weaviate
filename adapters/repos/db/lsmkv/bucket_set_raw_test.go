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
	"context"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func newSetRawTestBucket(t *testing.T) *Bucket {
	t.Helper()

	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(context.Background(), t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, b.Shutdown(context.Background()))
	})

	return b
}

func TestSetRawListWithStats(t *testing.T) {
	key := []byte("posting-1")

	t.Run("missing key", func(t *testing.T) {
		b := newSetRawTestBucket(t)

		values, stats, err := b.SetRawListWithStats(key)
		require.NoError(t, err)
		assert.Empty(t, values)
		assert.Equal(t, SetRawListStats{}, stats)
	})

	t.Run("active memtable only", func(t *testing.T) {
		b := newSetRawTestBucket(t)
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1"), []byte("longer-v2")}))

		values, stats, err := b.SetRawListWithStats(key)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("v1"), []byte("longer-v2")}, values)
		assert.Equal(t, 0, stats.SegmentsHit)
		assert.True(t, stats.MemtableHit)
		assert.False(t, stats.FlushingHit)
		assert.Equal(t, len("v1")+len("longer-v2"), stats.Bytes)
	})

	t.Run("single flushed segment", func(t *testing.T) {
		b := newSetRawTestBucket(t)
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1")}))
		require.NoError(t, b.FlushMemtable())

		values, stats, err := b.SetRawListWithStats(key)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("v1")}, values)
		assert.Equal(t, 1, stats.SegmentsHit)
		assert.False(t, stats.MemtableHit)
		assert.Equal(t, 2, stats.Bytes)
	})

	t.Run("fragmented across segments and memtable", func(t *testing.T) {
		b := newSetRawTestBucket(t)
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1")}))
		require.NoError(t, b.FlushMemtable())
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v2")}))
		require.NoError(t, b.FlushMemtable())
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v3")}))

		values, stats, err := b.SetRawListWithStats(key)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}, values)
		assert.Equal(t, 2, stats.SegmentsHit)
		assert.True(t, stats.MemtableHit)
		assert.False(t, stats.FlushingHit)
		assert.Equal(t, 6, stats.Bytes)

		// results and stats must agree with the plain read path
		plain, err := b.SetRawList(key)
		require.NoError(t, err)
		assert.Equal(t, plain, values)
	})

	t.Run("segments without the key are not counted", func(t *testing.T) {
		b := newSetRawTestBucket(t)
		require.NoError(t, b.SetAdd([]byte("other"), [][]byte{[]byte("x")}))
		require.NoError(t, b.FlushMemtable())
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1")}))
		require.NoError(t, b.FlushMemtable())

		values, stats, err := b.SetRawListWithStats(key)
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("v1")}, values)
		assert.Equal(t, 1, stats.SegmentsHit)
	})
}

// TestSetRawListWithStatsFromView asserts the zero-copy, view-based read
// returns exactly the same values and stats as the copying SetRawListWithStats
// across the same data layouts.
func TestSetRawListWithStatsFromView(t *testing.T) {
	key := []byte("posting-1")

	cases := map[string]func(t *testing.T) *Bucket{
		"missing key": func(t *testing.T) *Bucket {
			return newSetRawTestBucket(t)
		},
		"active memtable only": func(t *testing.T) *Bucket {
			b := newSetRawTestBucket(t)
			require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1"), []byte("longer-v2")}))
			return b
		},
		"single flushed segment": func(t *testing.T) *Bucket {
			b := newSetRawTestBucket(t)
			require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1")}))
			require.NoError(t, b.FlushMemtable())
			return b
		},
		"fragmented across segments and memtable": func(t *testing.T) *Bucket {
			b := newSetRawTestBucket(t)
			require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v1")}))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v2")}))
			require.NoError(t, b.FlushMemtable())
			require.NoError(t, b.SetAdd(key, [][]byte{[]byte("v3")}))
			return b
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			b := build(t)

			wantValues, wantStats, err := b.SetRawListWithStats(key)
			require.NoError(t, err)

			view := b.GetConsistentView()
			defer view.ReleaseView()

			gotValues, gotStats, err := b.SetRawListWithStatsFromView(view, key)
			require.NoError(t, err)

			assert.Equal(t, wantValues, gotValues)
			assert.Equal(t, wantStats, gotStats)
		})
	}

	t.Run("flushed-segment values alias segment memory", func(t *testing.T) {
		b := newSetRawTestBucket(t)
		require.NoError(t, b.SetAdd(key, [][]byte{[]byte("aliased-value")}))
		require.NoError(t, b.FlushMemtable())

		view := b.GetConsistentView()
		defer view.ReleaseView()

		require.Len(t, view.Disk, 1)
		seg, ok := view.Disk[0].(*segment)
		if !ok {
			t.Skipf("disk segment is %T, not *segment; cannot check aliasing", view.Disk[0])
		}
		require.True(t, seg.readFromMemory,
			"small test segment expected to serve reads from memory (zero-copy path)")

		noCopy, _, err := b.SetRawListWithStatsFromView(view, key)
		require.NoError(t, err)
		require.Len(t, noCopy, 1)

		copied, _, err := b.SetRawListWithStats(key)
		require.NoError(t, err)
		require.Equal(t, copied, noCopy, "same bytes regardless of copy")

		// The zero-copy value must point INTO the segment's contents.
		lo := uintptr(unsafe.Pointer(&seg.contents[0]))
		hi := lo + uintptr(len(seg.contents))
		aliasedPtr := uintptr(unsafe.Pointer(&noCopy[0][0]))
		require.GreaterOrEqual(t, aliasedPtr, lo, "no-copy value must alias segment contents")
		require.Less(t, aliasedPtr, hi, "no-copy value must alias segment contents")

		// The copying read must NOT alias the segment's contents.
		copiedPtr := uintptr(unsafe.Pointer(&copied[0][0]))
		require.False(t, copiedPtr >= lo && copiedPtr < hi,
			"copying read must not alias segment contents")
	})
}
