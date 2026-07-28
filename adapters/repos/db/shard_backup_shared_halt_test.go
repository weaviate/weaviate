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

package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	dbqueue "github.com/weaviate/weaviate/adapters/repos/db/queue"
	vcommon "github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	vhnsw "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// Pins the shared-halt lost-write bug: a transfer consumer that halts a shard
// another consumer already holds used to hit an early return and skip every
// preparation step. A write that landed after the first consumer's flush then
// lived only in the active memtable/WAL, which ListBackupFiles excludes, so the
// second consumer's file list silently dropped it.
//
// The two successive overlaps prove the re-seal is repeatable rather than
// one-shot: obj2 halts at count 1->2 and obj3 at 2->3.
func TestShard_SharedHaltSealsLateWrites(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)
	t.Cleanup(func() {
		_ = idx.drop()
		_ = os.RemoveAll(idx.Config.RootPath)
	})
	s := shd.(*Shard)

	// Sealed into a segment by op A's halt below, so its presence in every file
	// list distinguishes a genuinely empty snapshot from a dropped late write.
	baseline := testObject(className)
	require.NoError(t, s.PutObject(ctx, baseline))

	// op A halts first and never resumes, holding the shard for both overlaps.
	require.NoError(t, s.HaltForTransfer(ctx, false, 0))
	t.Cleanup(func() {
		for s.haltForTransferCount > 0 {
			require.NoError(t, s.resumeMaintenanceCycles(ctx))
		}
	})

	for _, tc := range []struct {
		name          string
		expectedCount int
	}{
		{name: "second consumer", expectedCount: 2},
		{name: "third consumer", expectedCount: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Lands in the memtable/WAL freshly created by the previous halt's
			// flush; a halt never trips the bucket read-only guard, so the write
			// is accepted but unsealed.
			late := testObject(className)
			require.NoError(t, s.PutObject(ctx, late))

			require.NoError(t, s.HaltForTransfer(ctx, false, 0))
			require.Equal(t, tc.expectedCount, s.haltForTransferCount)

			files, err := s.ListBackupFiles(ctx, &backup.ShardDescriptor{})
			require.NoError(t, err)

			require.True(t, objectsSnapshotHas(t, s.index.Config.RootPath, files, baseline.ID().String()),
				"write sealed before any halt is missing from the file list — the snapshot reconstruction itself is broken")
			require.True(t, objectsSnapshotHas(t, s.index.Config.RootPath, files, late.ID().String()),
				"write applied before this consumer's halt is missing from its file list — the shared halt skipped the re-seal")
		})
	}
}

// Pins the position of the trailing FlushMemtables. The queue and vector-index
// preparation steps drain in-flight tasks that write into the shard's LSM store
// (compressed vectors, HFresh postings). A flush placed before those steps
// leaves such writes in the WAL only, absent from the file list while the HNSW
// commit log already references them.
//
// The write is driven through a real in-flight queue task so the assertion
// covers production HaltForTransfer rather than a copy of its step order.
func TestShard_HaltForTransferSealsQueueDrainWrites(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)
	t.Cleanup(func() {
		_ = idx.drop()
		_ = os.RemoveAll(idx.Config.RootPath)
	})
	s := shd.(*Shard)

	// Establishes the objects bucket on disk so the reconstruction below has a
	// segment to read even when the drain write is lost.
	require.NoError(t, s.PutObject(ctx, testObject(className)))

	drained := testObject(className)
	drainedKey, err := uuid.MustParse(drained.ID().String()).MarshalBinary()
	require.NoError(t, err)
	drainedValue, err := drained.MarshalBinary()
	require.NoError(t, err)

	const queueID = "geo-drain-write"
	taskStarted := make(chan struct{})
	releaseTask := make(chan struct{})

	scheduler := dbqueue.NewScheduler(dbqueue.SchedulerOptions{
		Logger:           s.index.logger,
		Workers:          1,
		ScheduleInterval: time.Millisecond,
	})
	scheduler.Start()
	t.Cleanup(func() { scheduler.Close(context.Background()) })

	q, err := dbqueue.NewDiskQueue(dbqueue.DiskQueueOptions{
		ID:        queueID,
		Scheduler: scheduler,
		Logger:    s.index.logger,
		Dir:       t.TempDir(),
		TaskDecoder: &lateWriteTaskDecoder{
			started: taskStarted,
			release: releaseTask,
			write: func() error {
				return s.store.Bucket(helpers.ObjectsBucketLSM).Put(drainedKey, drainedValue,
					lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, drainedKey))
			},
		},
		StaleTimeout: time.Millisecond,
		ChunkSize:    50,
	})
	require.NoError(t, err)
	require.NoError(t, q.Init())
	scheduler.RegisterQueue(q)

	s.propertyIndicesLock.Lock()
	s.geoQueues["location"] = &VectorIndexQueue{DiskQueue: q}
	s.propertyIndicesLock.Unlock()

	require.NoError(t, q.Push([]byte{1}))
	require.NoError(t, q.Flush())
	<-taskStarted

	halted := make(chan error, 1)
	enterrors.GoWrapper(func() { halted <- s.HaltForTransfer(ctx, false, 0) }, s.index.logger)

	// The queue is marked paused before PrepareForBackup blocks waiting on the
	// in-flight task, so this is a precise signal that the halt has entered its
	// seal steps — releasing earlier would let an early flush capture the write
	// and mask the defect.
	require.Eventually(t, func() bool { return scheduler.IsQueuePaused(queueID) },
		10*time.Second, time.Millisecond, "halt never reached queue preparation")
	close(releaseTask)
	require.NoError(t, <-halted)
	t.Cleanup(func() { require.NoError(t, s.resumeMaintenanceCycles(ctx)) })

	files, err := s.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err)

	require.True(t, objectsSnapshotHas(t, s.index.Config.RootPath, files, drained.ID().String()),
		"write performed by the queue drain is missing from the file list — the memtable flush runs before the drain instead of after it")
}

// A failed halt at count>1 must roll back to the holder's count rather than
// unhalting the shard the first consumer still holds.
func TestShard_SharedHaltPrepErrorKeepsShardHalted(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)
	t.Cleanup(func() {
		_ = idx.drop()
		_ = os.RemoveAll(idx.Config.RootPath)
	})
	s := shd.(*Shard)

	require.NoError(t, s.HaltForTransfer(ctx, false, 0))

	// At count>1 the pause steps are gated out, so the cancelled context can
	// only be observed by a seal step — which is the point being pinned.
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, s.HaltForTransfer(cancelledCtx, false, 0))

	require.Equal(t, 1, s.haltForTransferCount,
		"failed count>1 halt must roll back to the first consumer's hold")

	require.NoError(t, s.resumeMaintenanceCycles(ctx))
	require.Equal(t, 0, s.haltForTransferCount)
}

// objectsSnapshotHas rebuilds the objects bucket from exactly the listed files —
// what a transfer target receives — and reports whether id is retrievable, i.e.
// whether the write survives in a sealed, listable segment.
func objectsSnapshotHas(t *testing.T, rootPath string, files []string, id string) bool {
	t.Helper()
	ctx := context.Background()

	bucketDir := filepath.Join(t.TempDir(), helpers.ObjectsBucketLSM)
	segmentDir := filepath.Join("lsm", helpers.ObjectsBucketLSM) + string(filepath.Separator)
	require.Positive(t, copyListedFiles(t, rootPath, files, segmentDir, bucketDir),
		"no objects-bucket segment was listed for the snapshot")

	bucket, err := lsmkv.NewBucketCreator().NewBucket(ctx, bucketDir, "", logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	key, err := uuid.MustParse(id).MarshalBinary()
	require.NoError(t, err)
	value, err := bucket.Get(key)
	require.NoError(t, err)
	return value != nil
}

type lateWriteTaskDecoder struct {
	started chan struct{}
	release chan struct{}
	write   func() error
	once    sync.Once
}

func (d *lateWriteTaskDecoder) DecodeTask([]byte) (dbqueue.Task, error) {
	return &lateWriteTask{started: d.started, release: d.release, write: d.write, once: &d.once}, nil
}

type lateWriteTask struct {
	started chan struct{}
	release chan struct{}
	write   func() error
	once    *sync.Once
}

func (t *lateWriteTask) Op() uint8 { return 1 }

func (t *lateWriteTask) Key() uint64 { return 0 }

func (t *lateWriteTask) Execute(ctx context.Context) error {
	t.once.Do(func() { close(t.started) })
	select {
	case <-t.release:
		return t.write()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// The HNSW half of the same defect, and the half that stays exposed even where
// the LSM half does not: IncomingListFiles flushes memtables itself before
// listing, but nothing outside index.PrepareForBackup switches the commit log,
// and hnsw.ListFiles deliberately excludes the active one. So a replica COPY
// overlapping another COPY — which the replication FSM explicitly permits on a
// shared source shard — silently ships a replica missing every vector written
// since the first COPY halted.
func TestShard_SharedHaltSealsLateVectors(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className},
		enthnsw.NewDefaultUserConfig(), false, false, false)
	t.Cleanup(func() {
		_ = idx.drop()
		_ = os.RemoveAll(idx.Config.RootPath)
	})
	s := shd.(*Shard)

	baseline := testObject(className)
	baseline.Vector = []float32{1, 0, 0}
	require.NoError(t, s.PutObject(ctx, baseline))

	require.NoError(t, s.HaltForTransfer(ctx, false, 0))
	t.Cleanup(func() {
		for s.haltForTransferCount > 0 {
			require.NoError(t, s.resumeMaintenanceCycles(ctx))
		}
	})

	// Commit-log file names are unix seconds, so a switch within the same second
	// as the previous one reopens the same path in append mode and the late
	// write's file stays the active — and therefore unlisted — one.
	time.Sleep(1100 * time.Millisecond)

	late := testObject(className)
	late.Vector = []float32{0, 1, 0}
	require.NoError(t, s.PutObject(ctx, late))

	require.NoError(t, s.HaltForTransfer(ctx, false, 0))
	require.Equal(t, 2, s.haltForTransferCount)

	files, err := s.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err)

	baselineDocID := docIDOf(t, s, baseline.ID())
	lateDocID := docIDOf(t, s, late.ID())

	restored := restoreIndexFromListedCommitLogs(t, s.index.Config.RootPath, files, map[uint64][]float32{
		baselineDocID: baseline.Vector,
		lateDocID:     late.Vector,
	})

	require.True(t, restored.ContainsDoc(baselineDocID),
		"vector sealed before any halt is missing from the restored index — the reconstruction itself is broken")
	require.True(t, restored.ContainsDoc(lateDocID),
		"vector written before this consumer's halt is missing from its file list — the commit log was never switched")

	ids, _, err := restored.SearchByVector(ctx, late.Vector, 2, nil)
	require.NoError(t, err)
	require.Contains(t, ids, lateDocID, "late vector is present but unreachable in the restored graph")
}

// restoreIndexFromListedCommitLogs rebuilds an HNSW index from exactly the
// commit-log files in the listing — what a transfer target receives — by
// replaying them into a throwaway root. vectors backs the restored index's
// VectorForID, standing in for the LSM store that would be shipped alongside.
func restoreIndexFromListedCommitLogs(t *testing.T, rootPath string, files []string, vectors map[uint64][]float32) *vhnsw.HNSW {
	t.Helper()

	// The commit-log directory name embeds the shard's legacy (unnamed) vector
	// index id, which is what a default single-vector collection uses.
	const commitLogDir = "main.hnsw.commitlog.d"
	restoreRoot := filepath.Join(t.TempDir(), "shard")
	// Deliberately not asserting a non-zero copy count: when the switch never
	// happens the listing legitimately holds no commit log at all, and that
	// belongs on the missing-vector assertion rather than on file plumbing.
	copyListedFiles(t, rootPath, files, commitLogDir, filepath.Join(restoreRoot, commitLogDir))

	logger := logrus.New()
	restored, err := vhnsw.New(vhnsw.Config{
		RootPath:         restoreRoot,
		ID:               "main",
		Logger:           logger,
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		AllocChecker:     memwatch.NewDummyMonitor(),
		DisableSnapshots: true,
		MakeCommitLoggerThunk: func() (vhnsw.CommitLogger, error) {
			return vhnsw.NewCommitLogger(restoreRoot, "main", logger,
				cyclemanager.NewCallbackGroupNoop(), vhnsw.WithSnapshotDisabled(true))
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[id], nil
		},
		GetViewThunk:      func() vcommon.BucketView { return sharedHaltNoopBucketView{} },
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
	}, enthnsw.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Shutdown(context.Background()) })

	return restored
}

// copyListedFiles copies every listed file whose path contains dirMarker into
// dstDir, flattened, and returns how many were copied.
func copyListedFiles(t *testing.T, rootPath string, files []string, dirMarker, dstDir string) int {
	t.Helper()
	require.NoError(t, os.MkdirAll(dstDir, 0o755))

	copied := 0
	for _, relPath := range files {
		if !strings.Contains(relPath, dirMarker) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(rootPath, relPath))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dstDir, filepath.Base(relPath)), data, 0o644))
		copied++
	}
	return copied
}

func docIDOf(t *testing.T, s *Shard, id strfmt.UUID) uint64 {
	t.Helper()

	obj, err := s.ObjectByID(context.Background(), id, search.SelectProperties{}, additional.Properties{})
	require.NoError(t, err)
	require.NotNil(t, obj)
	return obj.DocID
}

type sharedHaltNoopBucketView struct{}

func (sharedHaltNoopBucketView) ReleaseView() {}
