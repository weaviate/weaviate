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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// searched is what the index answers to every query of the fixture now. Unlike
// answers, it does nothing that is due in the background first: while a
// backup holds the index, a pass over its vectors waits.
func (f *persistentFixture) searched(t *testing.T, k int) [][]uint64 {
	t.Helper()
	out := make([][]uint64, len(f.queries))
	for i, query := range f.queries {
		ids, _, err := f.index.SearchByVector(context.Background(), query, k, nil)
		require.NoError(t, err, "query %d", i)
		out[i] = ids
	}
	return out
}

// holds tells, for every doc id of the fixture, whether the index holds it.
func (f *persistentFixture) holds() []bool {
	out := make([]bool, len(f.vectors))
	for id := range out {
		out[id] = f.index.ContainsDoc(uint64(id))
	}
	return out
}

// backupCut is a backup of the index and of its store, laid out under root as
// their files are under the root of the fixture.
type backupCut struct {
	root           string
	listed, staged []string
	// answers and holds are those of the index when its files were copied,
	// and atFlush those when the memtables of the store were flushed.
	answers, atFlush         [][]uint64
	holds, holdsAtFlush      []bool
	marked                   bool
	generation               int
}

// backup takes a backup of the index and of its store as a shard takes one
// with hardlinks (Shard.haltForTransfer and Shard.CreateBackupSnapshot):
// PrepareForBackup, the flush of the memtables, the files of the store and of
// the index listed, the files that change in place copied, every other one
// linked, and ResumeAfterBackup. prepared runs between PrepareForBackup and
// the flush, and flushed between the flush and the listing: the writes a
// shard takes there, since it does not stop its writes for a backup.
func (f *persistentFixture) backup(t *testing.T, k int, prepared, flushed func()) backupCut {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, f.index.PrepareForBackup(ctx))
	if prepared != nil {
		prepared()
	}
	require.NoError(t, f.store.FlushMemtables(ctx))
	cut := backupCut{root: t.TempDir()}
	cut.atFlush, cut.holdsAtFlush = f.searched(t, k), f.holds()
	if flushed != nil {
		flushed()
	}
	listed, err := f.store.ListFiles(ctx, f.root)
	require.NoError(t, err)
	indexFiles, err := f.index.ListFiles(ctx, f.root)
	require.NoError(t, err)
	listed = append(listed, indexFiles...)
	staged, err := f.index.SnapshotMutableFiles(ctx, f.root, cut.root)
	require.NoError(t, err)
	cut.answers, cut.holds = f.searched(t, k), f.holds()
	if f.index.g != nil {
		cut.marked = f.index.g.graphMarked
		cut.generation = f.index.g.meta.VectorGen
	}
	for _, rel := range listed {
		if slices.Contains(staged, rel) {
			continue
		}
		dst := filepath.Join(cut.root, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
		require.NoError(t, os.Link(filepath.Join(f.root, rel), dst), "link %s", rel)
	}
	require.NoError(t, f.index.ResumeAfterBackup(ctx))
	cut.listed, cut.staged = listed, staged
	return cut
}

// restored opens the index a backup holds, as a shard opens what a restore
// copied back.
func (f *persistentFixture) restored(t *testing.T, cut backupCut) *persistentFixture {
	t.Helper()
	c := f.openedAt(t, cut.root)
	t.Cleanup(func() {
		if c.index != nil {
			c.close(t)
		}
	})
	return c
}

// fileThere tells whether a file of the index is on disk.
func fileThere(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

// A backup of a shard does not take the index while it builds its base: what
// the build writes is not whole until it ends, and a copy of half of it is
// not an index. PrepareForBackup fails as a shard fails a backup in the
// middle of another structural change of an index, with an error that says
// to try again later. And while a backup holds the index no build starts:
// the vectors stay aside, are searched as they are, and the base is built
// once the backup lets go of the index.
func TestPersistentBackupWhileItsBaseIsBuilt(t *testing.T) {
	ctx := context.Background()

	t.Run("refused while the base is built", func(t *testing.T) {
		reading, release := make(chan struct{}), make(chan struct{})
		var once, released sync.Once
		f := newPersistentFixtureWith(t, persistentBase+100, func(cfg *PersistentConfig) {
			cfg.baseHook = func(point string) error {
				if point == "the vectors of the base are read" {
					once.Do(func() {
						close(reading)
						<-release
					})
				}
				return nil
			}
		})
		let := func() { released.Do(func() { close(release) }) }
		t.Cleanup(let)
		f.addBatches(t, 0, persistentBase)
		select {
		case <-reading:
		case <-time.After(30 * time.Second):
			t.Fatal("the base is not built")
		}
		err := f.index.PrepareForBackup(ctx)
		require.ErrorIs(t, err, enterrors.ErrShardBusyStructuralOp, "no backup while the base is built")
		require.NoError(t, f.index.ResumeAfterBackup(ctx), "what a shard calls when a halt failed")

		let()
		require.NoError(t, f.index.settleBase(ctx))
		require.NotNil(t, f.index.g)
		require.NoError(t, f.index.PrepareForBackup(ctx), "the base is built")
		require.NoError(t, f.index.ResumeAfterBackup(ctx))
	})

	t.Run("no build starts during a backup", func(t *testing.T) {
		const k = 10
		const total = persistentBase + 100
		f := newPersistentFixture(t, total)
		f.addBatches(t, 0, persistentBase-10)
		require.NoError(t, f.index.PrepareForBackup(ctx))
		f.addBatches(t, persistentBase-10, total)
		require.NoError(t, f.index.settleBase(ctx))
		require.Nil(t, f.index.g, "no base is built while a backup holds the index")
		require.Equal(t, total, f.keptAside(t))
		assert.Greater(t, recallOf(t, f, f.searched(t, k), total, k), 0.99, "the vectors aside are searched")

		require.NoError(t, f.index.ResumeAfterBackup(ctx))
		require.Eventually(t, func() bool {
			f.index.mu.RLock()
			defer f.index.mu.RUnlock()
			return f.index.g != nil
		}, 30*time.Second, 5*time.Millisecond, "the base is built once the backup lets go")
		require.NoError(t, f.index.settleBase(ctx))
		f.covered(t, "built after the backup")
	})
}

// recallOf measures answers against brute force over the first n vectors of
// the fixture.
func recallOf(t *testing.T, f *persistentFixture, answers [][]uint64, n, k int) float64 {
	t.Helper()
	hits, total := 0, 0
	for i, ids := range answers {
		for _, id := range f.truth(t, f.queries[i], n, k, nil) {
			if slices.Contains(ids, id) {
				hits++
			}
		}
		total += k
	}
	return float64(hits) / float64(total)
}
