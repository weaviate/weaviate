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

package postingedge

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// objectsOfAShard stands for the store of objects of a shard, as the index
// asks it when it opens (PersistentConfig.ObjectExists): every object is
// there but the lost ones.
type objectsOfAShard struct {
	mu    sync.Mutex
	lost  map[uint64]bool
	fail  bool
	asked map[uint64]bool
}

func newObjectsOfAShard() *objectsOfAShard {
	return &objectsOfAShard{lost: map[uint64]bool{}, asked: map[uint64]bool{}}
}

func (o *objectsOfAShard) exists(_ context.Context, docID uint64) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fail {
		return false, errors.New("the store of the shard does not answer")
	}
	o.asked[docID] = true
	return !o.lost[docID], nil
}

func (o *objectsOfAShard) lose(ids ...uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, id := range ids {
		o.lost[id] = true
	}
}

func (o *objectsOfAShard) failing(fail bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fail = fail
}

func (o *objectsOfAShard) wasAsked(id uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.asked[id]
}

// orphansDropped checks an index opened after its objects were lost: it
// holds the vectors of the doc ids [from, to) but the lost ones, which no
// search returns, the shard was asked about each of them, and a lost doc id
// takes a vector again, as the shard hands it out to a new object.
func orphansDropped(t *testing.T, c *persistentFixture, objects *objectsOfAShard, from, to int, lost []uint64) {
	t.Helper()
	ctx := context.Background()
	for id := uint64(from); id < uint64(to); id++ {
		gone := slices.Contains(lost, id)
		require.Equal(t, !gone, c.index.ContainsDoc(id), "doc %d, lost %v", id, gone)
		require.True(t, objects.wasAsked(id), "doc %d: the shard was asked about it", id)
	}
	c.covered(t, "after the orphans were dropped")
	for q, query := range c.queries[:6] {
		ids, _, err := c.index.SearchByVector(ctx, query, 10, nil)
		require.NoError(t, err, "query %d", q)
		for _, id := range ids {
			require.False(t, slices.Contains(lost, id), "query %d returned doc %d, whose object was lost", q, id)
		}
	}
	for _, id := range lost {
		require.NoError(t, c.index.Add(ctx, id, c.vectors[id]), "doc %d takes a vector again", id)
		require.True(t, c.index.ContainsDoc(id), "doc %d", id)
	}
	_, err := os.Stat(filepath.Join(c.index.dir, growCheckFile))
	require.True(t, os.IsNotExist(err), "no doc id is left to check: %v", err)
}

// An index that opens asks the shard, for every vector its journal puts back,
// whether its object is in the store, and drops those whose objects are not:
// a process that died after the index took a vector and before the store
// wrote the log of its object, or a transfer that copied the journal after
// the store. Those whose objects are there stay. Then its doc id is free: a
// transfer can leave the shard a counter behind the index.
func TestPersistentDropsTheVectorsOfLostObjects(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) {
			ctx := context.Background()
			const added = persistentBase + 100
			objects := newObjectsOfAShard()
			f := newPersistentFixtureMode(t, added+40, cacheMode, func(cfg *PersistentConfig) {
				cfg.noBackground = true
				cfg.ObjectExists = objects.exists
			})
			f.add(t, 0, added)
			require.NoError(t, f.index.Flush())

			// The vectors of a batch are in the journal, and nothing is
			// flushed: what a process that dies now leaves.
			require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(40, added), f.vectors[added:added+40]))
			lost := []uint64{added + 3, added + 17, added + 39}
			objects.lose(lost...)
			c := f.crashed(t)
			orphansDropped(t, c, objects, added, added+40, lost)
		})
	}
}

// The index empties its journal as it opens, before it asks the shard: an open
// that dies before the answers are on disk leaves the doc ids in a file of
// their own, and the next open asks about them.
func TestPersistentAsksAgainAfterAnOpenThatFailed(t *testing.T) {
	ctx := context.Background()
	const added = persistentBase + 100
	objects := newObjectsOfAShard()
	f := newPersistentFixtureWith(t, added+30, func(cfg *PersistentConfig) {
		cfg.noBackground = true
		cfg.ObjectExists = objects.exists
	})
	f.add(t, 0, added)
	require.NoError(t, f.index.Flush())
	require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(30, added), f.vectors[added:added+30]))
	lost := []uint64{added, added + 29}
	objects.lose(lost...)

	root := t.TempDir()
	require.NoError(t, os.CopyFS(root, os.DirFS(f.root)))
	objects.failing(true)
	err := f.openFailsAt(t, root)
	require.ErrorContains(t, err, "does not answer")
	dir := filepath.Join(root, filepath.Base(f.index.dir))
	journal, err := os.Stat(filepath.Join(dir, growJournalFile))
	require.NoError(t, err)
	require.Zero(t, journal.Size(), "the open that failed emptied the journal")
	left, err := readGrowCheck(filepath.Join(dir, growCheckFile))
	require.NoError(t, err)
	// With those of the members a split of the batch moved.
	require.Subset(t, left, sequentialIDs(30, added), "the doc ids the journal put back wait to be asked about")

	objects.failing(false)
	c := f.openedAt(t, root)
	t.Cleanup(func() {
		if c.index != nil {
			c.close(t)
		}
	})
	orphansDropped(t, c, objects, added, added+30, lost)
}

// No journal tells the vectors kept aside, before an index has its base: the
// index asks about every one of them, and drops those whose objects are not
// in the store.
func TestPersistentDropsTheVectorsKeptAsideOfLostObjects(t *testing.T) {
	objects := newObjectsOfAShard()
	const added = persistentBase / 2
	f := newPersistentFixtureWith(t, added, func(cfg *PersistentConfig) {
		cfg.noBackground = true
		cfg.ObjectExists = objects.exists
	})
	f.add(t, 0, added)
	require.NoError(t, f.index.Flush())
	require.Equal(t, added, f.keptAside(t))
	lost := []uint64{0, 42, added - 1}
	objects.lose(lost...)
	c := f.crashed(t)
	require.Nil(t, c.index.g, "the index has no base")
	ctx := context.Background()
	for id := range uint64(added) {
		gone := slices.Contains(lost, id)
		require.Equal(t, !gone, c.index.ContainsDoc(id), "doc %d, lost %v", id, gone)
		require.True(t, objects.wasAsked(id), "doc %d: the shard was asked about it", id)
	}
	for _, id := range lost {
		require.NoError(t, c.index.Add(ctx, id, c.vectors[id]), "doc %d takes a vector again", id)
	}
	require.Equal(t, added, c.keptAside(t))
}

// The index has the shard write the log of its objects before what its
// journal tells leaves the journal, so that a vector is outside the journal
// only once its object is in the system: a shard flushes its vector indexes
// before it writes the logs of its store (updateVectorIndex, then putOne).
// And before it writes the log of the vectors kept aside, which no journal
// tells.
func TestPersistentWritesTheLogOfTheObjectsFirst(t *testing.T) {
	ctx := context.Background()
	var (
		mu      sync.Mutex
		dir     string
		calls   int
		records []int64
	)
	f := newPersistentFixtureWith(t, persistentBase+120, func(cfg *PersistentConfig) {
		cfg.noBackground = true
		cfg.WriteObjectsLog = func() error {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if dir == "" {
				return nil
			}
			journal, err := os.Stat(filepath.Join(dir, growJournalFile))
			if err != nil {
				return err
			}
			records = append(records, journal.Size())
			return nil
		}
	})
	reset := func() {
		mu.Lock()
		defer mu.Unlock()
		calls, records = 0, nil
	}
	f.add(t, 0, 100)
	reset()
	require.NoError(t, f.index.Add(ctx, 100, f.vectors[100]))
	require.NoError(t, f.index.Flush())
	mu.Lock()
	require.Positive(t, calls, "the flush of the vectors kept aside writes the log of the objects")
	mu.Unlock()

	f.add(t, 101, persistentBase+100)
	require.NoError(t, f.index.Flush())
	mu.Lock()
	dir = f.index.dir
	mu.Unlock()
	reset()
	require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(20, persistentBase+100), f.vectors[persistentBase+100:]))
	require.NoError(t, f.index.Flush())
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, records, "the sync writes the log of the objects")
	require.Positive(t, records[0], "the log of the objects is written while the journal still holds the batch")
	journal, err := os.Stat(filepath.Join(dir, growJournalFile))
	require.NoError(t, err)
	require.Zero(t, journal.Size(), "and then the journal is emptied")
}
