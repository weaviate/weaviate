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

package hfresh

import (
	"context"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// crashTestIndex is an index whose store and centroid commit log live under
// one directory, so that a copy of the directory is what a crash leaves.
type crashTestIndex struct {
	TestHFresh
	store *lsmkv.Store
}

// newTestIndexAt opens the index stored under dir, with a real commit log
// for the centroid HNSW. vectors serves the full-precision vectors and is
// shared by the index and its reopened copies, as the objects bucket would
// be.
func newTestIndexAt(t *testing.T, dir string, vectors *testVectorStore) crashTestIndex {
	t.Helper()

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	root := filepath.Join(dir, "root")
	require.NoError(t, os.MkdirAll(root, 0o755))

	cfg := DefaultConfig()
	cfg.RootPath = root
	cfg.DistanceProvider = distancer.NewL2SquaredProvider()
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath: root,
		ID:       "centroids",
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(root, "centroids", logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		DistanceProvider:  distancer.NewL2SquaredProvider(),
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger
	cfg.VectorForIDThunk = vectors.get
	setDelegatingTempThunk(cfg)

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger})
	cfg.Scheduler = scheduler
	scheduler.Start()
	t.Cleanup(func() {
		scheduler.Close(t.Context())
	})

	store := testinghelpers.NewDummyStoreFromFolder(filepath.Join(dir, "store"), t)
	createObjectsBucket(t, store)

	index, err := New(cfg, ent.NewDefaultUserConfig(), store)
	require.NoError(t, err)
	// a test that fails before its close leaves no index running; the
	// errors of an index or store closed already are ignored
	t.Cleanup(func() {
		_ = index.Shutdown(context.Background())
		_ = store.Shutdown(context.Background())
	})

	return crashTestIndex{
		TestHFresh: TestHFresh{
			Index:   index,
			Logs:    hook,
			Vectors: vectors,
		},
		store: store,
	}
}

func (tf *crashTestIndex) close(t *testing.T) {
	t.Helper()
	require.NoError(t, tf.Index.Shutdown(t.Context()))
	require.NoError(t, tf.store.Shutdown(t.Context()))
}

// crash copies the index's directory as it is on disk now, closes the
// index, and opens the copy. What the buffered writers still held is not
// in the copy.
func (tf *crashTestIndex) crash(t *testing.T, dir string) crashTestIndex {
	t.Helper()
	crashed := t.TempDir()
	copyDir(t, dir, crashed)
	tf.close(t)
	return newTestIndexAt(t, crashed, tf.Vectors)
}

// registerCentroid stores a posting and inserts its centroid, as a split
// leaves them.
func registerCentroid(t *testing.T, tf *TestHFresh, postingID uint64, posting Posting, centroid []float32) {
	t.Helper()
	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(centroid))
	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
	require.NoError(t, tf.Index.setPostingVectorIDs(t.Context(), postingID, posting))
	require.NoError(t, tf.Index.Centroids.Insert(postingID, &Centroid{
		Uncompressed: centroid,
		Compressed:   compressed,
	}))
}

// livePlacement counts, for every vector id, the postings of live centroids
// that hold a current entry of it.
func livePlacement(t *testing.T, h *HFresh) map[uint64]int {
	t.Helper()
	var ids []uint64
	h.Centroids.hnsw.Iterate(func(id uint64) bool {
		if h.Centroids.Exists(id) {
			ids = append(ids, id)
		}
		return true
	})
	seen := make(map[uint64]int)
	for _, id := range ids {
		p, err := h.PostingStore.Get(t.Context(), id)
		require.NoError(t, err)
		live, err := p.GarbageCollect(h.VersionMap)
		require.NoError(t, err)
		for _, v := range live {
			seen[v.ID()]++
		}
	}
	return seen
}

// checkEveryVectorInOnePosting requires every id of ids to be in exactly one
// posting of a live centroid, and no other id to be in any.
func checkEveryVectorInOnePosting(t *testing.T, h *HFresh, ids []uint64) {
	t.Helper()
	seen := livePlacement(t, h)
	for _, id := range ids {
		require.Equal(t, 1, seen[id], "vector %d is not in exactly one posting of a live centroid", id)
	}
	require.Len(t, seen, len(ids), "the postings hold vectors never inserted")
}

func vectorIDs(start uint64, n int) []uint64 {
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = start + uint64(i)
	}
	return ids
}

// TestCrashAfterSplitOrMergeKeepsEveryVector runs a split or a merge, flushes
// the centroid HNSW's commit log only, and reopens a copy of the directory
// as a crash would leave it. Every vector must still be in exactly one
// posting of a live centroid.
func TestCrashAfterSplitOrMergeKeepsEveryVector(t *testing.T) {
	tests := []struct {
		name string
		// run builds the postings and returns the vector ids and the
		// operation to run on them
		run func(t *testing.T, tf *crashTestIndex) ([]uint64, func() error)
	}{
		{
			name: "split",
			run: func(t *testing.T, tf *crashTestIndex) ([]uint64, func() error) {
				vectors := createTestVectors(4, 15)
				postingID, posting := createPostingWithVectors(t, &tf.TestHFresh, vectors, 200)
				registerCentroid(t, &tf.TestHFresh, postingID, posting, vectors[7])
				tf.Index.maxPostingSize = 10
				return vectorIDs(200, len(vectors)), func() error {
					return tf.Index.doSplit(t.Context(), postingID, false)
				}
			},
		},
		{
			name: "merge",
			run: func(t *testing.T, tf *crashTestIndex) ([]uint64, func() error) {
				// every vector equals both centroids, so the merge
				// enqueues no reassign that would move them afterwards
				e1 := []float32{1, 0, 0, 0}
				small := [][]float32{e1, e1, e1}
				large := [][]float32{e1, e1, e1, e1, e1, e1}
				smallID, smallPosting := createPostingWithVectors(t, &tf.TestHFresh, small, 300)
				largeID, largePosting := createPostingWithVectors(t, &tf.TestHFresh, large, 400)
				registerCentroid(t, &tf.TestHFresh, smallID, smallPosting, e1)
				registerCentroid(t, &tf.TestHFresh, largeID, largePosting, e1)
				ids := append(vectorIDs(300, len(small)), vectorIDs(400, len(large))...)
				return ids, func() error {
					return tf.Index.doMerge(t.Context(), smallID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			tf := newTestIndexAt(t, dir, &testVectorStore{m: make(map[uint64][]float32)})
			ids, operation := tt.run(t, &tf)

			// everything before the operation is on disk, as after the
			// shard's write of the WALs at the end of a batch
			require.NoError(t, tf.store.WriteWALs())
			require.NoError(t, tf.Index.Centroids.hnsw.Flush())

			require.NoError(t, operation())
			checkEveryVectorInOnePosting(t, tf.Index, ids)

			require.NoError(t, tf.Index.Centroids.hnsw.Flush())
			reopened := tf.crash(t, dir)
			checkEveryVectorInOnePosting(t, reopened.Index, ids)
			reopened.close(t)
		})
	}
}

// TestCrashAfterFirstPostingKeepsTheIDSequence creates the first posting,
// flushes the centroid HNSW's commit log only, and reopens a copy of the
// directory as a crash would leave it. The id sequence must not hand out the
// id of the centroid that survived.
func TestCrashAfterFirstPostingKeepsTheIDSequence(t *testing.T) {
	dir := t.TempDir()
	tf := newTestIndexAt(t, dir, &testVectorStore{m: make(map[uint64][]float32)})
	v := []float32{1, 0, 0, 0}
	require.NoError(t, tf.Index.initDimensions(v))
	require.NoError(t, tf.store.WriteWALs())

	compressed := tf.Index.quantizer.CompressedBytes(tf.Index.quantizer.Encode(v))
	targets, err := tf.Index.ensureInitialPosting(v, compressed)
	require.NoError(t, err)
	require.Equal(t, 1, targets.Len())
	postingID := targets.data[0].ID

	require.NoError(t, tf.Index.Centroids.hnsw.Flush())
	reopened := tf.crash(t, dir)
	require.True(t, reopened.Index.Centroids.Exists(postingID))
	next, err := reopened.Index.IDs.Next()
	require.NoError(t, err)
	require.Greater(t, next, postingID, "the sequence handed out the id of a centroid on disk")
	reopened.close(t)
}

// TestCrashAfterFlushKeepsEveryVector inserts vectors, lets the splits,
// merges and reassigns run, calls the index's Flush and nothing else, and
// reopens a copy of the directory as a crash would leave it. Every vector
// must still be in a posting of a live centroid.
func TestCrashAfterFlushKeepsEveryVector(t *testing.T) {
	const (
		n   = 300
		dim = 8
	)
	dir := t.TempDir()
	tf := newTestIndexAt(t, dir, &testVectorStore{m: make(map[uint64][]float32)})

	rng := rand.New(rand.NewSource(11))
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rng.Float32()
		}
	}
	require.NoError(t, tf.Index.initDimensions(vectors[0]))
	// small postings, so that the inserts split and merge
	tf.Index.maxPostingSize = 20
	tf.Index.minPostingSize = 6
	for i, v := range vectors {
		addVectorToIndex(t, &tf.TestHFresh, uint64(i), v)
	}
	tf.Index.waitForMaintenance(t)

	check := func(h *HFresh) {
		t.Helper()
		seen := livePlacement(t, h)
		for i := range uint64(n) {
			require.GreaterOrEqual(t, seen[i], 1, "vector %d is in no posting of a live centroid", i)
		}
		require.Len(t, seen, n, "the postings hold vectors never inserted")
	}
	check(tf.Index)

	require.NoError(t, tf.Index.Flush())
	reopened := tf.crash(t, dir)
	check(reopened.Index)
	reopened.close(t)
}

// copyDir copies the files under src to dst as they are on disk now.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	require.NoError(t, err)
}

// TestCrashAfterPutWithOnlyTheVersionOnDisk puts a posting again and lets
// only the shared bucket's write buffer reach the disk, as it does when that
// buffer fills on its own. The reopened index must still read the posting's
// vectors.
//
// It fails today: PostingStore.Put writes the posting and then its version
// in two separately buffered buckets, so the version can reach the disk
// without the posting, which then reads as empty. The test is skipped until
// it is fixed; remove the Skip to reproduce it.
func TestCrashAfterPutWithOnlyTheVersionOnDisk(t *testing.T) {
	t.Skip("known defect: a posting's new version can reach the disk before the posting, which then reads as empty after a crash")
	dir := t.TempDir()
	tf := newTestIndexAt(t, dir, &testVectorStore{m: make(map[uint64][]float32)})
	vectors := createTestVectors(4, 5)
	postingID, posting := createPostingWithVectors(t, &tf.TestHFresh, vectors, 500)
	registerCentroid(t, &tf.TestHFresh, postingID, posting, vectors[2])
	require.NoError(t, tf.store.WriteWALs())
	require.NoError(t, tf.Index.Centroids.hnsw.Flush())

	require.NoError(t, tf.Index.PostingStore.Put(t.Context(), postingID, posting))
	shared, release, err := newBucketRef(tf.store, sharedBucketName(tf.Index.id)).acquire()
	require.NoError(t, err)
	require.NoError(t, shared.WriteWAL())
	release()

	reopened := tf.crash(t, dir)
	checkEveryVectorInOnePosting(t, reopened.Index, vectorIDs(500, len(vectors)))
	reopened.close(t)
}
