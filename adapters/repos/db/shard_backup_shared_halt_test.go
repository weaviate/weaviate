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

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	dbqueue "github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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
	require.NoError(t, os.MkdirAll(bucketDir, 0o755))

	segmentPrefix := filepath.Join("lsm", helpers.ObjectsBucketLSM) + string(filepath.Separator)
	copied := 0
	for _, relPath := range files {
		if !strings.Contains(relPath, segmentPrefix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(rootPath, relPath))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(bucketDir, filepath.Base(relPath)), data, 0o644))
		copied++
	}
	require.Positive(t, copied, "no objects-bucket segment was listed for the snapshot")

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
