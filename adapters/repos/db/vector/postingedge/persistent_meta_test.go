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
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// indexFiles lists every file of the index on disk, its own directory and
// the buckets of the store that are its, with the size of each.
func (f *persistentFixture) indexFiles(t *testing.T) map[string]int64 {
	t.Helper()
	files := map[string]int64{}
	dirs := []string{filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID))}
	for _, name := range helpers.PostingEdgeBucketNames(f.cfg.ID) {
		dirs = append(dirs, filepath.Join(f.root, "lsm", name))
	}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files[path] = info.Size()
			return nil
		})
		require.NoError(t, err)
	}
	return files
}

// An index whose meta is missing is not an empty index: opening it fails,
// and nothing of it is deleted. A copy or a restore that left the meta out
// opens as nothing at all otherwise, with no error. Put back, the meta opens
// the index as it was. An index that never had a base has no meta and no
// files of one, and opens as it always did.
func TestPersistentOpensWithoutItsMeta(t *testing.T) {
	const k = 10
	ctx := context.Background()

	t.Run("a base whose meta is missing", func(t *testing.T) {
		f := newPersistentFixture(t, persistentBase+200)
		f.add(t, 0, persistentBase+200)
		require.NotNil(t, f.index.g)
		before := f.answers(t, k)
		f.close(t)
		files := f.indexFiles(t)
		require.NotEmpty(t, files)
		meta := filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID), diskMetaFile)
		raw, err := os.ReadFile(meta)
		require.NoError(t, err)
		require.NoError(t, os.Remove(meta))
		delete(files, meta)

		noop := cyclemanager.NewCallbackGroupNoop()
		store, err := lsmkv.New(filepath.Join(f.root, "lsm"), f.root, testLogger(), nil, nil, noop, noop, noop)
		require.NoError(t, err)
		index, err := Open(f.cfg, f.uc, store)
		if err == nil {
			require.NoError(t, index.Shutdown(ctx))
		}
		require.NoError(t, store.Shutdown(ctx))
		assert.Error(t, err, "an index without its meta opens")
		assert.Equal(t, files, f.indexFiles(t), "what is on disk of the index, after it was opened without its meta")

		// The meta put back, the index is the one it was.
		require.NoError(t, os.WriteFile(meta, raw, 0o644))
		f.open(t)
		require.NotNil(t, f.index.g)
		f.covered(t, "with its meta back")
		assert.Equal(t, before, f.answers(t, k))
	})

	t.Run("an index that never had a base", func(t *testing.T) {
		f := newPersistentFixture(t, persistentBase)
		f.restart(t)
		require.Nil(t, f.index.g)
		f.add(t, 0, persistentBase/2)
		before := f.answers(t, k)
		f.restart(t)
		require.Nil(t, f.index.g, "still gathering")
		assert.Equal(t, persistentBase/2, f.keptAside(t))
		assert.Equal(t, before, f.answers(t, k))
	})
}

// writeSteps stops the write of the base at each of its points, for a test to
// copy the files or to make the write fail there, until it is told to let
// every write run.
type writeSteps struct {
	stopping atomic.Bool
	reached  chan string
	answer   chan error
}

func newWriteSteps() *writeSteps {
	s := &writeSteps{reached: make(chan string), answer: make(chan error)}
	s.stopping.Store(true)
	return s
}

func (s *writeSteps) hook(point string) error {
	if !s.stopping.Load() {
		return nil
	}
	s.reached <- point
	return <-s.answer
}

// at waits for the write to reach a point.
func (s *writeSteps) at(t *testing.T, point string) {
	t.Helper()
	select {
	case reached := <-s.reached:
		require.Equal(t, point, reached)
	case <-time.After(2 * time.Minute):
		require.FailNow(t, "the write of the base did not get to where "+point)
	}
}

// buildLeftFixture is an index whose base is being written, stopped halfway
// through its postings, with a vector of it deleted meanwhile: the mark of
// that delete is kept aside with the vectors.
type buildLeftFixture struct {
	*persistentFixture
	steps *writeSteps
	logs  *logtest.Hook
}

const (
	buildLeftBase    = persistentBase
	buildLeftTotal   = persistentBase + 100
	buildLeftDeleted = 7
)

func newBuildLeftFixture(t *testing.T, cacheMode string) *buildLeftFixture {
	t.Helper()
	steps := newWriteSteps()
	logger, logs := logtest.NewNullLogger()
	f := newPersistentFixtureMode(t, buildLeftTotal, cacheMode, func(cfg *PersistentConfig) {
		cfg.writeHook = steps.hook
		cfg.Logger = logger
	})
	b := &buildLeftFixture{persistentFixture: f, steps: steps, logs: logs}
	t.Cleanup(func() {
		// A test that failed halfway leaves the write waiting.
		if steps.stopping.Swap(false) {
			select {
			case <-steps.reached:
				steps.answer <- nil
			default:
			}
		}
	})
	for start := 0; start < buildLeftBase; start += 50 {
		ids := sequentialIDs(50, uint64(start))
		require.NoError(t, f.index.AddBatch(context.Background(), ids, f.vectors[start:start+50]))
	}
	steps.at(t, "the files of the base are written")
	steps.answer <- nil
	steps.at(t, "half of the postings are written")
	require.NoError(t, f.index.Delete(buildLeftDeleted))
	return b
}

func buildLeftHeld(id uint64) bool { return id != buildLeftDeleted }

// copyNow copies the files of the shard as they are, with the vectors kept
// aside written to their log: what a process that died now leaves.
func (b *buildLeftFixture) copyNow(t *testing.T) string {
	t.Helper()
	require.NoError(t, b.index.Flush())
	root := t.TempDir()
	require.NoError(t, os.CopyFS(root, os.DirFS(b.root)))
	return root
}

// finish lets the write go on, with the given answer, and waits for the
// build to end, failed or not. The writes after it are not stopped.
func (b *buildLeftFixture) finish(t *testing.T, answer error) {
	t.Helper()
	b.steps.stopping.Store(false)
	b.steps.answer <- answer
	b.index.baseMu.Lock()
	b.index.baseMu.Unlock() //nolint:staticcheck // waits for the build that is running
}

// cleanups returns the lines a cleanup of what a build wrote left in the
// log.
func cleanups(logs *logtest.Hook) []string {
	var lines []string
	for _, entry := range logs.AllEntries() {
		if entry.Data["action"] == "postingedge_clean_build" && entry.Level == logrus.WarnLevel {
			lines = append(lines, entry.Message)
		}
	}
	return lines
}

// diskAt lists every file of the index at a root, with its size: its
// directory, its buckets, and the directory of a build of its base, but for
// the buckets named.
func diskAt(t *testing.T, root, id string, without ...string) map[string]int64 {
	t.Helper()
	files := map[string]int64{}
	dirs := []string{
		filepath.Join(root, helpers.PostingEdgeDirName(id)),
		filepath.Join(root, helpers.PostingEdgeBuildDirName(id)),
	}
	for _, name := range append(helpers.PostingEdgeBucketNames(id), helpers.PostingEdgeBucketName("edges", id)) {
		if !slices.Contains(without, name) {
			dirs = append(dirs, filepath.Join(root, "lsm", name))
		}
	}
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil || entry.IsDir() {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files[path] = info.Size()
			return nil
		})
		require.NoError(t, err)
	}
	return files
}

// openFailsAt opens the index at a root, which must fail, and returns why.
func (f *persistentFixture) openFailsAt(t *testing.T, root string) error {
	t.Helper()
	ctx := context.Background()
	cfg := f.cfg
	cfg.RootPath = root
	noop := cyclemanager.NewCallbackGroupNoop()
	store, err := lsmkv.New(filepath.Join(root, "lsm"), root, testLogger(), nil, nil, noop, noop, noop)
	require.NoError(t, err)
	index, err := Open(cfg, f.uc, store)
	if err == nil {
		require.NoError(t, index.Shutdown(ctx))
	}
	require.NoError(t, store.Shutdown(ctx))
	require.Error(t, err, "the index opened")
	return err
}

// leftOfABase checks that there is part of a base at a root, with the list of
// the build that wrote it: what the tests of a build that did not end start
// from.
func leftOfABase(t *testing.T, root, id string) {
	t.Helper()
	dir := filepath.Join(root, helpers.PostingEdgeDirName(id))
	_, err := os.Stat(filepath.Join(dir, diskMetaFile))
	require.True(t, os.IsNotExist(err), "no meta")
	_, err = os.Stat(filepath.Join(dir, diskMeanFile))
	require.NoError(t, err, "a file of the base")
	_, err = os.Stat(filepath.Join(root, "lsm", helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketPostings, id)))
	require.NoError(t, err, "a bucket of the base")
	_, err = os.Stat(filepath.Join(root, helpers.PostingEdgeBuildDirName(id), buildListFile))
	require.NoError(t, err, "the list of the build")
}

// What a build of the base leaves when it does not end is removed, and only
// that: what the list it left before it wrote anything names, and only when
// nothing else of a base is there and every vector the base was built from is
// still kept aside. Every cleanup leaves a line in the log with what it
// removed and why. So for a build whose process dies while it writes the base
// and for one that fails without dying, and for a copy made in the middle of
// a build and restored somewhere else, as a backup would one day: the next
// build writes the base again, whole. With the vectors the base was built
// from not all kept aside, or something of a base the list does not name, the
// index does not open and nothing is removed (integration-plan.md, section
// 9, the decision of the user of 2026-11-23).
func TestPersistentCleansWhatABuildLeft(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) { persistentCleansWhatABuildLeft(t, cacheMode) })
	}
}

func persistentCleansWhatABuildLeft(t *testing.T, cacheMode string) {
	ctx := context.Background()
	const k = 10

	t.Run("a build that dies while it writes the base", func(t *testing.T) {
		b := newBuildLeftFixture(t, cacheMode)
		root := b.copyNow(t)
		b.finish(t, nil)
		leftOfABase(t, root, b.cfg.ID)

		opened := b.openedAt(t, root)
		lines := cleanups(b.logs)
		require.Len(t, lines, 1, "a line in the log")
		assert.Contains(t, lines[0], filepath.Join(root, helpers.PostingEdgeDirName(b.cfg.ID), diskMeanFile), "what it removed")
		assert.Contains(t, lines[0], "did not end", "and why")
		_, err := os.Stat(filepath.Join(root, helpers.PostingEdgeBuildDirName(b.cfg.ID)))
		assert.True(t, os.IsNotExist(err), "the directory of the build")
		// The vector deleted while the base was built leaves one fewer than
		// a base aside: the next one makes it due.
		opened.add(t, buildLeftBase, buildLeftBase+1)
		opened.expect(t, "with its base built again", buildLeftBase+1, buildLeftHeld)
		opened.close(t)
	})

	// A cleanup removes the files of the directory of the index, the meta
	// first, and then the buckets, and the directory of the build with its
	// list last: one that stopped halfway leaves the list, and the index that
	// opens next removes the rest.
	t.Run("a cleanup that stopped halfway", func(t *testing.T) {
		b := newBuildLeftFixture(t, cacheMode)
		root := b.copyNow(t)
		b.finish(t, nil)
		leftOfABase(t, root, b.cfg.ID)
		dir := filepath.Join(root, helpers.PostingEdgeDirName(b.cfg.ID))
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		for _, entry := range entries {
			require.NoError(t, os.RemoveAll(filepath.Join(dir, entry.Name())))
		}

		opened := b.openedAt(t, root)
		lines := cleanups(b.logs)
		require.Len(t, lines, 1, "a line in the log")
		postings := helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketPostings, b.cfg.ID)
		assert.Contains(t, lines[0], filepath.Join(root, "lsm", postings), "what it removed")
		// The vector deleted while the base was built leaves one fewer than
		// a base aside: the next one makes it due.
		opened.add(t, buildLeftBase, buildLeftBase+1)
		opened.expect(t, "with its base built again", buildLeftBase+1, buildLeftHeld)
		opened.close(t)
	})

	t.Run("a build that fails without dying", func(t *testing.T) {
		b := newBuildLeftFixture(t, cacheMode)
		b.finish(t, errors.New("the write is made to fail"))
		b.index.mu.RLock()
		g, staged, gone := b.index.g, len(b.index.staged), len(b.index.gone)
		b.index.mu.RUnlock()
		require.Nil(t, g, "no base")
		require.Equal(t, buildLeftBase-1, staged, "every vector still aside but the one deleted")
		require.Zero(t, gone, "and the mark of that one let go")
		disk := diskAt(t, b.root, b.cfg.ID, b.index.stagingName())
		assert.Empty(t, disk, "nothing of the base, nor the directory of the build")
		lines := cleanups(b.logs)
		require.Len(t, lines, 1, "a line in the log")
		assert.Contains(t, lines[0], filepath.Join(b.root, helpers.PostingEdgeDirName(b.cfg.ID), diskMeanFile), "what it removed")
		assert.Contains(t, lines[0], "the write is made to fail", "and why")
		for q, query := range b.queries {
			ids, _, err := b.index.SearchByVector(ctx, query, k, nil)
			require.NoError(t, err, "query %d", q)
			require.Equal(t, b.truth(t, query, buildLeftBase, k, buildLeftHeld), ids, "query %d, exact while aside", q)
		}

		// The next build writes the base whole.
		b.add(t, buildLeftBase, buildLeftTotal)
		b.expect(t, "with its base built after the failure", buildLeftTotal, buildLeftHeld)
		b.restart(t)
		b.expect(t, "opened again", buildLeftTotal, buildLeftHeld)
	})

	t.Run("the vectors it was built from not all kept aside", func(t *testing.T) {
		b := newBuildLeftFixture(t, cacheMode)
		root := b.copyNow(t)
		b.finish(t, nil)
		leftOfABase(t, root, b.cfg.ID)
		staging := helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, b.cfg.ID)
		tests := []struct {
			name     string
			lose     func(t *testing.T, root string)
			wantText string
		}{
			{
				name: "their bucket left out",
				lose: func(t *testing.T, root string) {
					require.NoError(t, os.RemoveAll(filepath.Join(root, "lsm", staging)))
				},
				wantText: "there is no bucket " + staging,
			},
			{
				name: "one of them missing",
				lose: func(t *testing.T, root string) {
					noop := cyclemanager.NewCallbackGroupNoop()
					store, err := lsmkv.New(filepath.Join(root, "lsm"), root, testLogger(), nil, nil, noop, noop, noop)
					require.NoError(t, err)
					require.NoError(t, store.CreateOrLoadBucket(ctx, staging, lsmkv.WithStrategy(lsmkv.StrategyReplace)))
					require.NoError(t, store.Bucket(staging).Delete(growDocKey(3)))
					require.NoError(t, store.Shutdown(ctx))
				},
				wantText: "1 of the 600 vectors it was built from are not kept aside (doc 3)",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				copied := t.TempDir()
				require.NoError(t, os.CopyFS(copied, os.DirFS(root)))
				tt.lose(t, copied)
				// The bucket of the vectors kept aside is opened to count them,
				// which may write to its log.
				before := diskAt(t, copied, b.cfg.ID, staging)
				err := b.openFailsAt(t, copied)
				assert.ErrorContains(t, err, tt.wantText)
				assert.ErrorContains(t, err, "nothing is removed")
				assert.Equal(t, before, diskAt(t, copied, b.cfg.ID, staging), "nothing is removed")
				assert.Empty(t, cleanups(b.logs))
			})
		}
		_, err := os.Stat(filepath.Join(root, "lsm", staging))
		require.NoError(t, err, "where the vectors are kept aside")
	})

	t.Run("something of a base the list does not name", func(t *testing.T) {
		b := newBuildLeftFixture(t, cacheMode)
		root := b.copyNow(t)
		b.finish(t, nil)
		foreign := filepath.Join(root, helpers.PostingEdgeDirName(b.cfg.ID), diskLayoutFile)
		require.NoError(t, os.WriteFile(foreign, []byte("not of this build"), 0o644))
		before := diskAt(t, root, b.cfg.ID)
		err := b.openFailsAt(t, root)
		assert.ErrorContains(t, err, foreign)
		assert.ErrorContains(t, err, "nothing is removed")
		assert.Equal(t, before, diskAt(t, root, b.cfg.ID), "nothing is removed")
		assert.Empty(t, cleanups(b.logs))
	})

	// The backup of a shard refuses an index of this kind today
	// (Persistent.ListFiles): this is what the index does with a copy of its
	// files made while a build writes the base, file by file and not all at
	// once, and restored under another directory. A backup that copies them
	// must carry the directory of the build, whose list is what lets the
	// index remove what the build wrote.
	t.Run("a copy made in the middle of a build, restored", func(t *testing.T) {
		steps := newWriteSteps()
		logger, logs := logtest.NewNullLogger()
		f := newPersistentFixtureMode(t, buildLeftBase, cacheMode, func(cfg *PersistentConfig) {
			cfg.writeHook = steps.hook
			cfg.Logger = logger
		})
		for start := 0; start < buildLeftBase; start += 50 {
			ids := sequentialIDs(50, uint64(start))
			require.NoError(t, f.index.AddBatch(ctx, ids, f.vectors[start:start+50]))
		}
		restored := t.TempDir()
		copyPart := func(rel string) {
			t.Helper()
			if _, err := os.Stat(filepath.Join(f.root, rel)); os.IsNotExist(err) {
				return
			}
			require.NoError(t, os.CopyFS(filepath.Join(restored, rel), os.DirFS(filepath.Join(f.root, rel))))
		}
		// First the vectors kept aside and the directory of the build, ...
		steps.at(t, "the files of the base are written")
		require.NoError(t, f.index.Flush())
		copyPart(filepath.Join("lsm", helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketStaging, f.cfg.ID)))
		copyPart(helpers.PostingEdgeBuildDirName(f.cfg.ID))
		steps.answer <- nil
		// ... and later what there is of the base by then.
		steps.at(t, "half of the postings are written")
		copyPart(helpers.PostingEdgeDirName(f.cfg.ID))
		for _, name := range f.index.baseBucketNames() {
			copyPart(filepath.Join("lsm", name))
		}
		steps.stopping.Store(false)
		steps.answer <- nil
		require.NoError(t, f.index.settleBase(ctx))
		leftOfABase(t, restored, f.cfg.ID)

		opened := f.openedAt(t, restored)
		lines := cleanups(logs)
		require.Len(t, lines, 1, "a line in the log")
		assert.Contains(t, lines[0], filepath.Join(restored, helpers.PostingEdgeDirName(f.cfg.ID), diskMeanFile))
		require.NoError(t, opened.index.settleBase(ctx))
		opened.expect(t, "restored, with its base built again", buildLeftBase, func(uint64) bool { return true })
		before := opened.answers(t, k)
		opened.restart(t)
		assert.Equal(t, before, opened.answers(t, k))
		opened.close(t)
	})
}

// The list a build of the base leaves before it writes is what the write
// creates: every file and bucket of the base it wrote is on it, and nothing
// else is but the meta written next to its place, which has its name by then.
// So in every format the index writes its base in.
func TestBuildListIsWhatTheBuildWrites(t *testing.T) {
	tests := []struct {
		name      string
		cacheMode string
		listEdges bool
	}{
		{name: "codes in memory", cacheMode: ent.CacheModeRouting},
		{name: "no codes in memory", cacheMode: ent.CacheModeNone},
		{name: "a value per list of edges", cacheMode: ent.CacheModeRouting, listEdges: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type seen struct {
				list            *buildList
				files, buckets  []string
				listErr, entErr error
			}
			written := make(chan seen, 1)
			var f *persistentFixture
			f = newPersistentFixtureMode(t, persistentBase, tt.cacheMode, func(cfg *PersistentConfig) {
				cfg.listEdges = tt.listEdges
				cfg.baseHook = func(point string) error {
					if point == "the base is written" {
						var s seen
						s.list, s.listErr = f.index.readBuildList()
						s.files, s.buckets, s.entErr = f.index.baseLeft()
						written <- s
					}
					return nil
				}
			})
			f.add(t, 0, persistentBase)
			require.NotNil(t, f.index.g)
			s := <-written
			require.NoError(t, s.listErr)
			require.NoError(t, s.entErr)
			require.NotNil(t, s.list, "the list of the build")
			assert.ElementsMatch(t, s.list.Files, append(slices.Clone(s.files), diskMetaFile+".tmp"), "the files")
			assert.ElementsMatch(t, s.list.Buckets, s.buckets, "the buckets")
			assert.Equal(t, persistentBase, s.list.Sources)
			assert.ElementsMatch(t, sequentialIDs(persistentBase, 0), s.list.sources, "the doc ids it was built from")
			_, err := os.Stat(filepath.Join(f.root, helpers.PostingEdgeBuildDirName(f.cfg.ID)))
			assert.True(t, os.IsNotExist(err), "the directory of the build, after it")
		})
	}
}

// A build of the base does not start while anything of a base is there,
// named by the list it would write or not: what a cleanup removes is never
// older than the build that listed it. The vectors stay aside, searched as
// before, and what was there is left as it was.
func TestPersistentBuildDoesNotStartOverWhatIsThere(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		where func(f *persistentFixture) string
	}{
		{
			name: "a bucket the list names",
			where: func(f *persistentFixture) string {
				return filepath.Join(f.root, "lsm", helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketRouting, f.cfg.ID))
			},
		},
		{
			name: "a directory in the one of the index that the list does not name",
			where: func(f *persistentFixture) string {
				return filepath.Join(f.root, helpers.PostingEdgeDirName(f.cfg.ID), "not-of-a-build")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPersistentFixture(t, persistentBase)
			f.add(t, 0, persistentBase-1)
			there := tt.where(f)
			require.NoError(t, os.MkdirAll(there, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(there, "x"), []byte("x"), 0o644))
			require.NoError(t, f.index.AddBatch(ctx, []uint64{persistentBase - 1}, f.vectors[persistentBase-1:]))
			// Whether the build runs here or in the background, it has run
			// when this returns.
			if err := f.index.settleBase(ctx); err != nil {
				require.ErrorContains(t, err, "does not start")
			}
			f.index.mu.RLock()
			g, failed := f.index.g, f.index.buildFailed
			f.index.mu.RUnlock()
			require.Nil(t, g, "no base")
			require.Equal(t, persistentBase, failed, "the build failed")
			got, err := os.ReadFile(filepath.Join(there, "x"))
			require.NoError(t, err)
			assert.Equal(t, "x", string(got), "what was there")
			_, err = os.Stat(filepath.Join(f.root, helpers.PostingEdgeBuildDirName(f.cfg.ID), buildListFile))
			assert.True(t, os.IsNotExist(err), "no list was written")
			for q, query := range f.queries {
				ids, _, err := f.index.SearchByVector(ctx, query, 10, nil)
				require.NoError(t, err, "query %d", q)
				require.Equal(t, f.truth(t, query, persistentBase, 10, nil), ids, "query %d", q)
			}
		})
	}
}
