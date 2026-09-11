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
	"errors"
	"fmt"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

func TestMemtableRoaringSet(t *testing.T) {
	logger, _ := test.NewNullLogger()
	memPath := func() string {
		return path.Join(t.TempDir(), "fake")
	}

	t.Run("inserting individual entries", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddOne(key1, 1))
		assert.Nil(t, m.roaringSetAddOne(key1, 2))
		assert.Nil(t, m.roaringSetAddOne(key2, 3))
		assert.Nil(t, m.roaringSetAddOne(key2, 4))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.False(t, setKey1.Additions.Contains(3))
		assert.False(t, setKey1.Additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(1))
		assert.False(t, setKey2.Additions.Contains(2))
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("inserting lists", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddList(key1, []uint64{1, 2}))
		assert.Nil(t, m.roaringSetAddList(key2, []uint64{3, 4}))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.False(t, setKey1.Additions.Contains(3))
		assert.False(t, setKey1.Additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(1))
		assert.False(t, setKey2.Additions.Contains(2))
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("inserting bitmaps", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		bm1 := roaringset.NewBitmap(1, 2)
		assert.Nil(t, m.roaringSetAddBitmap(key1, bm1))
		bm2 := roaringset.NewBitmap(3, 4)
		assert.Nil(t, m.roaringSetAddBitmap(key2, bm2))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.False(t, setKey1.Additions.Contains(3))
		assert.False(t, setKey1.Additions.Contains(4))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(1))
		assert.False(t, setKey2.Additions.Contains(2))
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("removing individual entries", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveOne(key1, 7))
		assert.Nil(t, m.roaringSetRemoveOne(key2, 8))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.False(t, setKey1.Additions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(7))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.False(t, setKey2.Additions.Contains(8))
		assert.True(t, setKey2.Deletions.Contains(8))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("removing lists", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveList(key1, []uint64{7, 8}))
		assert.Nil(t, m.roaringSetRemoveList(key2, []uint64{9, 10}))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey1.Additions.GetCardinality())
		assert.Equal(t, 2, setKey1.Deletions.GetCardinality())
		assert.True(t, setKey1.Deletions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(8))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey2.Additions.GetCardinality())
		assert.Equal(t, 2, setKey2.Deletions.GetCardinality())
		assert.True(t, setKey2.Deletions.Contains(9))
		assert.True(t, setKey2.Deletions.Contains(10))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("removing bitmaps", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetRemoveBitmap(key1, roaringset.NewBitmap(7, 8)))
		assert.Nil(t, m.roaringSetRemoveBitmap(key2, roaringset.NewBitmap(9, 10)))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey1.Additions.GetCardinality())
		assert.Equal(t, 2, setKey1.Deletions.GetCardinality())
		assert.True(t, setKey1.Deletions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(8))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.Equal(t, 0, setKey2.Additions.GetCardinality())
		assert.Equal(t, 2, setKey2.Deletions.GetCardinality())
		assert.True(t, setKey2.Deletions.Contains(9))
		assert.True(t, setKey2.Deletions.Contains(10))

		require.Nil(t, m.commitlog.close())
	})

	t.Run("adding/removing slices", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSet, 0)
		require.NoError(t, err)

		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     memPath(),
			strategy: StrategyRoaringSet,
		})
		require.Nil(t, err)

		key1, key2 := []byte("key1"), []byte("key2")

		assert.Nil(t, m.roaringSetAddRemoveSlices(key1,
			[]uint64{1, 2}, []uint64{7, 8}))
		assert.Nil(t, m.roaringSetAddRemoveSlices(key2,
			[]uint64{3, 4}, []uint64{9, 10}))
		assert.Greater(t, m.Size(), uint64(0))

		setKey1, err := m.roaringSetGet(key1)
		require.Nil(t, err)
		assert.Equal(t, 2, setKey1.Additions.GetCardinality())
		assert.True(t, setKey1.Additions.Contains(1))
		assert.True(t, setKey1.Additions.Contains(2))
		assert.Equal(t, 2, setKey1.Deletions.GetCardinality())
		assert.True(t, setKey1.Deletions.Contains(7))
		assert.True(t, setKey1.Deletions.Contains(8))

		setKey2, err := m.roaringSetGet(key2)
		require.Nil(t, err)
		assert.Equal(t, 2, setKey2.Additions.GetCardinality())
		assert.True(t, setKey2.Additions.Contains(3))
		assert.True(t, setKey2.Additions.Contains(4))
		assert.Equal(t, 2, setKey2.Deletions.GetCardinality())
		assert.True(t, setKey2.Deletions.Contains(9))
		assert.True(t, setKey2.Deletions.Contains(10))

		require.Nil(t, m.commitlog.close())
	})
}

// TestMemtableRoaringSetSize pins Size() as the emptiness predicate
// atomicallySwitchMemtable and Memtable.flush read it as.
func TestMemtableRoaringSetSize(t *testing.T) {
	logger, _ := test.NewNullLogger()

	newRoaringSetMemtable := func(t *testing.T, cl memtableCommitLogger) *Memtable {
		m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
			path:     path.Join(t.TempDir(), "fake"),
			strategy: StrategyRoaringSet,
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, m.commitlog.close()) })
		return m
	}
	newRoaringSetCommitLog := func(t *testing.T) memtableCommitLogger {
		cl, err := newCommitLogger(path.Join(t.TempDir(), "fake"), StrategyRoaringSet, 0)
		require.NoError(t, err)
		return cl
	}

	key := []byte("key1")

	t.Run("a write carrying no doc IDs leaves the memtable empty", func(t *testing.T) {
		writes := []struct {
			name  string
			write func(m *Memtable) error
		}{
			{"add list", func(m *Memtable) error { return m.roaringSetAddList(key, nil) }},
			{"add bitmap", func(m *Memtable) error { return m.roaringSetAddBitmap(key, sroar.NewBitmap()) }},
			{"remove list", func(m *Memtable) error { return m.roaringSetRemoveList(key, nil) }},
			{"remove bitmap", func(m *Memtable) error { return m.roaringSetRemoveBitmap(key, sroar.NewBitmap()) }},
			{"add and remove slices", func(m *Memtable) error {
				return m.roaringSetAddRemoveSlices(key, nil, nil)
			}},
			{"add batch", func(m *Memtable) error {
				return m.roaringSetAddBatch([]RoaringSetBatchEntry{{Key: key}})
			}},
			{"remove batch", func(m *Memtable) error {
				return m.roaringSetRemoveBatch([]RoaringSetBatchEntry{{Key: key}})
			}},
		}

		for _, w := range writes {
			t.Run(w.name, func(t *testing.T) {
				m := newRoaringSetMemtable(t, newRoaringSetCommitLog(t))

				require.NoError(t, w.write(m))

				assert.Zero(t, m.Size())
				// a zero Size alone would not rule out a node left behind by a
				// write that returned before roaringSetAdjustMeta ran
				assert.Empty(t, m.roaringSet.FlattenInOrder())
				assert.Zero(t, m.DirtyDuration(),
					"a memtable holding nothing cannot be switched, so the dirty "+
						"timer would only wake the flush cycle to be refused")
			})
		}
	})

	t.Run("a tombstone alone makes the memtable non-empty", func(t *testing.T) {
		m := newRoaringSetMemtable(t, newRoaringSetCommitLog(t))

		require.NoError(t, m.roaringSetRemoveOne(key, 7))

		assert.Greater(t, m.Size(), uint64(0),
			"a memtable read as empty is skipped, resurrecting the deleted doc")
		assert.Len(t, m.roaringSet.FlattenInOrder(), 1)
	})

	t.Run("an empty entry in a batch creates no node for its key", func(t *testing.T) {
		m := newRoaringSetMemtable(t, newRoaringSetCommitLog(t))

		require.NoError(t, m.roaringSetAddBatch([]RoaringSetBatchEntry{
			{Key: []byte("key1"), Values: []uint64{1}},
			{Key: []byte("key2")},
			{Key: []byte("key3"), Values: []uint64{3}},
		}))

		assert.Len(t, m.roaringSet.FlattenInOrder(), 2)
	})

	t.Run("rewriting doc IDs already present does not grow the size", func(t *testing.T) {
		m := newRoaringSetMemtable(t, newRoaringSetCommitLog(t))
		require.NoError(t, m.roaringSetAddList(key, []uint64{1, 2, 3}))
		sizeAfterFirstWrite := m.Size()

		for i := 0; i < 100; i++ {
			require.NoError(t, m.roaringSetAddList(key, []uint64{1, 2, 3}))
		}

		assert.Equal(t, sizeAfterFirstWrite, m.Size())
	})

	t.Run("a batch stopped by the commit log reports what it wrote", func(t *testing.T) {
		entries := []RoaringSetBatchEntry{
			{Key: []byte("key1"), Values: []uint64{1}},
			{Key: []byte("key2"), Values: []uint64{2}},
			{Key: []byte("key3"), Values: []uint64{3}},
		}
		batches := []struct {
			name  string
			write func(m *Memtable, entries []RoaringSetBatchEntry) error
		}{
			{"add batch", func(m *Memtable, entries []RoaringSetBatchEntry) error {
				return m.roaringSetAddBatch(entries)
			}},
			{"remove batch", func(m *Memtable, entries []RoaringSetBatchEntry) error {
				return m.roaringSetRemoveBatch(entries)
			}},
		}

		for _, batch := range batches {
			for _, accepted := range []int{0, 2} {
				t.Run(fmt.Sprintf("%s, %d entries accepted", batch.name, accepted), func(t *testing.T) {
					stopped := newRoaringSetMemtable(t, &commitLogRefusingAfter{limit: accepted})
					require.Error(t, batch.write(stopped, entries))

					// what a memtable that was asked for only the accepted entries holds
					complete := newRoaringSetMemtable(t, newRoaringSetCommitLog(t))
					require.NoError(t, batch.write(complete, entries[:accepted]))

					assert.Len(t, stopped.roaringSet.FlattenInOrder(), accepted)
					assert.Equal(t, complete.Size(), stopped.Size())
					if accepted > 0 {
						assert.NotZero(t, stopped.Size(),
							"the entries the commit log accepted are in the tree")
						return
					}
					assert.Zero(t, stopped.DirtyDuration(),
						"a memtable nothing reached must not be marked dirty, or every "+
							"later flush cycle takes the flush lock to switch nothing")
				})
			}
		}
	})

	// a tripwire on sroar rather than something the readers of Size() depend on:
	// every node costs nodeFixedSizeInBytes, so a zero still means no nodes even
	// if a release started shrinking bitmap buffers on Remove
	t.Run("size never decreases", func(t *testing.T) {
		m := newRoaringSetMemtable(t, newRoaringSetCommitLog(t))
		docIDs := make([]uint64, 1024)
		for i := range docIDs {
			docIDs[i] = uint64(i)
		}

		steps := []struct {
			name  string
			write func(m *Memtable) error
		}{
			{"add many", func(m *Memtable) error { return m.roaringSetAddList(key, docIDs) }},
			{"remove all but one", func(m *Memtable) error { return m.roaringSetRemoveList(key, docIDs[1:]) }},
			{"remove the last one", func(m *Memtable) error { return m.roaringSetRemoveList(key, docIDs[:1]) }},
			{"add one back", func(m *Memtable) error { return m.roaringSetAddOne(key, docIDs[0]) }},
			{"remove doc IDs never added", func(m *Memtable) error {
				return m.roaringSetRemoveList(key, []uint64{1 << 40, 1 << 41})
			}},
		}

		previous := m.Size()
		for _, step := range steps {
			require.NoError(t, step.write(m))
			assert.GreaterOrEqual(t, m.Size(), previous, step.name)
			previous = m.Size()
		}
	})
}

// commitLogRefusingAfter refuses the add past limit nodes.
type commitLogRefusingAfter struct {
	dummyCommitLogger
	limit    int
	accepted int
}

func (c *commitLogRefusingAfter) add(node *roaringset.SegmentNodeList) error {
	if c.accepted >= c.limit {
		return errors.New("commit log refused the node")
	}
	c.accepted++
	return nil
}
