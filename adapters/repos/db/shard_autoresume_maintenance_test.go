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

//go:build integrationTest

package db

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestShard_IllegalStateForTransfer(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	amount := 10

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	t.Run("insert data into shard", func(t *testing.T) {
		for range amount {
			obj := testObject(className)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("attempt to list backup files without halting for transfer should fail", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("attempt to get file metadata without halting for transfer should fail", func(t *testing.T) {
		_, err := shd.GetFileMetadata(ctx, "any.db")
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("attempt to get file content without halting for transfer should fail", func(t *testing.T) {
		_, err := shd.GetFile(ctx, "any.db")
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("halt for transfer", func(t *testing.T) {
		inactivityTimeout := 100 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)
	})

	t.Run("resume maintenance tasks", func(t *testing.T) {
		err := shd.resumeMaintenanceCycles(ctx)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files after explicitly resuming maintenance tasks should fail", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("attempt to get file metadata after explicitly resuming maintenance tasks should fail", func(t *testing.T) {
		_, err := shd.GetFileMetadata(ctx, "any.db")
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("attempt to get file content after explicitly resuming maintenance tasks should fail", func(t *testing.T) {
		_, err := shd.GetFile(ctx, "any.db")
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("halt for transfer and wait for inactivity timeout", func(t *testing.T) {
		inactivityTimeout := 10 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)

		time.Sleep(inactivityTimeout * 10) // wait for inactivity timeout to elapse
	})

	t.Run("attempt to list backup files after inactivity time should fail", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("attempt to get file metadata after inactivity time should fail", func(t *testing.T) {
		_, err := shd.GetFileMetadata(ctx, "any.db")
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("attempt to get file content after inactivity time should fail", func(t *testing.T) {
		_, err := shd.GetFile(ctx, "any.db")
		require.ErrorContains(t, err, "not paused for transfer")
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_HaltingBeforeTransfer(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	amount := 10

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	t.Run("insert data into shard", func(t *testing.T) {
		for range amount {
			obj := testObject(className)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("halt for transfer", func(t *testing.T) {
		inactivityTimeout := 100 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)
	})

	backupDescriptor := &backup.ShardDescriptor{}

	t.Run("attempt to list backup files should succeed", func(t *testing.T) {
		files, err := shd.ListBackupFiles(ctx, backupDescriptor)
		require.NoError(t, err)
		backupDescriptor.Files = files
	})

	t.Run("attempt to get file metadata should succeed", func(t *testing.T) {
		_, err := shd.GetFileMetadata(ctx, backupDescriptor.Files[0])
		require.NoError(t, err)
	})

	t.Run("attempt to get file content should succeed", func(t *testing.T) {
		_, err := shd.GetFile(ctx, backupDescriptor.Files[0])
		require.NoError(t, err)
	})

	t.Run("resume maintenance tasks", func(t *testing.T) {
		err := shd.resumeMaintenanceCycles(ctx)
		require.NoError(t, err)
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_ConcurrentTransfers(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	amount := 10

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	t.Run("insert data into shard", func(t *testing.T) {
		for range amount {
			obj := testObject(className)

			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}

		objs, err := shd.ObjectList(ctx, amount, nil, nil, additional.Properties{}, shd.Index().Config.ClassName)
		require.Nil(t, err)
		require.Equal(t, amount, len(objs))
	})

	t.Run("halt for transfer", func(t *testing.T) {
		inactivityTimeout := 100 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files should succeed", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
	})

	t.Run("attempt to insert objects with paused maintenance tasks should succeed", func(t *testing.T) {
		obj := testObject(className)

		err := shd.PutObject(ctx, obj)
		require.NoError(t, err)
	})

	t.Run("halt for transfer with already paused maintenance tasks should succed", func(t *testing.T) {
		inactivityTimeout := 150 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files for a second time should succeed", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
	})

	t.Run("resume maintenance tasks", func(t *testing.T) {
		err := shd.resumeMaintenanceCycles(ctx)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files with one halt request still active should succeed", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
	})

	t.Run("resume maintenance tasks", func(t *testing.T) {
		err := shd.resumeMaintenanceCycles(ctx)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files after resuming for a second time should fail", func(t *testing.T) {
		_, err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer")
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

// haltForTransferForTest is a test-only variant of HaltForTransfer that lets
// the caller inject a side effect at a specific point in the halt sequence
// and toggle whether a final flush runs at the end. It exists to deterministically
// reproduce the pre-fix bug (writes into the LSM store after the initial flush
// that never get persisted because there is no second flush) without relying on
// racing with the async indexing queue.
//
// Layout mirrors production HaltForTransfer (see shard_backup.go):
//   - pauseCompaction + FlushMemtables (the original "early flush")
//   - deactivate vector / geo maintenance
//   - PrepareForBackup on vector queues, geo queues, vector indexes
//   - inject() — simulates the hidden writes that queue drain and HNSW
//     Preload perform into the compressed vectors bucket during the
//     steps above
//   - optional final FlushMemtables (the fix)
//
// With finalFlush=false this is exactly the pre-branch behavior.
// With finalFlush=true this is the current fix.
func (s *Shard) haltForTransferForTest(ctx context.Context, inject func() error, finalFlush bool) error {
	s.haltForTransferMux.Lock()
	defer s.haltForTransferMux.Unlock()

	s.haltForTransferCount++

	if err := s.store.PauseCompaction(ctx); err != nil {
		return fmt.Errorf("pause compaction: %w", err)
	}
	if !finalFlush {
		if err := s.store.FlushMemtables(ctx); err != nil {
			return fmt.Errorf("flush memtables: %w", err)
		}
	}
	if err := s.cycleCallbacks.vectorCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause vector maintenance: %w", err)
	}
	if err := s.cycleCallbacks.geoPropsCombinedCallbacksCtrl.Deactivate(ctx); err != nil {
		return fmt.Errorf("pause geo props maintenance: %w", err)
	}

	if err := s.ForEachVectorQueue(func(targetVector string, q *VectorIndexQueue) error {
		return q.PrepareForBackup(ctx)
	}); err != nil {
		return fmt.Errorf("prepare vector queues: %w", err)
	}
	if err := s.ForEachGeoQueue(func(_ string, q *VectorIndexQueue) error {
		return q.PrepareForBackup(ctx)
	}); err != nil {
		return fmt.Errorf("prepare geo queues: %w", err)
	}
	if err := s.ForEachVectorIndex(func(_ string, index VectorIndex) error {
		return index.PrepareForBackup(ctx)
	}); err != nil {
		return fmt.Errorf("prepare vector indexes: %w", err)
	}

	if inject != nil {
		if err := inject(); err != nil {
			return fmt.Errorf("inject: %w", err)
		}
	}

	if finalFlush {
		if err := s.store.FlushMemtables(ctx); err != nil {
			return fmt.Errorf("flush memtables after queue drain: %w", err)
		}
	}
	return nil
}

// TestShard_BackupPostFlushWritesAreLostWithoutLateFlush validates the fix in
// HaltForTransfer for the "entrypoint was deleted in the object store" bug.
//
// The real-world race: after the early FlushMemtables runs, queue drain and
// HNSW PrepareForBackup can still call compressor.Preload() which writes
// compressed vectors into the (now fresh) memtable of the compressed vectors
// bucket. Without a second flush those writes sit in the WAL only and never
// reach the backup's SST file list, while the HNSW commit log captures the
// corresponding nodes. On restore, the entrypoint lookup then fails.
//
// To make this deterministic we stand in for the racy queue drain write with
// an explicit Put into the compressed vectors bucket injected between the
// prepare steps and the (optional) late flush.
//
//   - pre-fix variant (finalFlush=false): key lives only in memtable/WAL, so
//     no SST file containing the key appears in the backup listing.
//   - fix variant (finalFlush=true): the late flush turns the memtable into an
//     SST, and that SST shows up in the backup listing.
func TestShard_BackupPostFlushWritesAreLostWithoutLateFlush(t *testing.T) {
	markerKey := []byte("post-flush-marker-key")
	markerValue := []byte("post-flush-marker-value")

	// countSSTsWithMarker lists backup files, reads each .db SST in the
	// compressed bucket, and returns how many contain markerValue. We check
	// the on-disk SSTs (not a bucket Get) because a bucket Get would also
	// return data still in the memtable — exactly the state we want to
	// exclude from the backup.
	countSSTsWithMarker := func(t *testing.T, shard *Shard, files []string) int {
		t.Helper()
		compressedBucketName := helpers.GetCompressedBucketName("")
		hits := 0
		for _, rel := range files {
			if !strings.Contains(rel, compressedBucketName) {
				continue
			}
			if !strings.HasSuffix(rel, ".db") {
				continue
			}
			abs := filepath.Join(shard.Index().Config.RootPath, rel)
			data, err := os.ReadFile(abs)
			require.NoError(t, err)
			if bytes.Contains(data, markerValue) {
				hits++
			}
		}
		return hits
	}

	runScenario := func(t *testing.T, finalFlush bool) int {
		ctx := testCtx()
		className := "BackupCompressedClass"

		hnswCfg := enthnsw.NewDefaultUserConfig()
		hnswCfg.BQ = enthnsw.BQConfig{Enabled: true}

		class := &models.Class{
			Class:               className,
			VectorIndexConfig:   hnswCfg,
			InvertedIndexConfig: invertedConfig(),
		}

		shd, idx := testShardWithSettings(t, ctx, class, hnswCfg,
			false, // withStopwords
			true,  // withCheckpoints (required for async indexing)
			true,  // withAsyncIndexingEnabled
		)
		t.Cleanup(func() {
			_ = idx.drop()
			_ = os.RemoveAll(idx.Config.RootPath)
			_ = os.RemoveAll(shd.Index().Config.RootPath)
		})

		shard := shd.(*Shard)

		// Seed the compressed bucket with a few flushed vectors so the bucket
		// exists on disk. We do not rely on their presence for the assertion —
		// the marker is what we check.
		r := rand.New(rand.NewSource(42))
		objects := createRandomObjects(r, className, 20, 32)
		for _, obj := range objects {
			require.NoError(t, shard.PutObject(ctx, obj))
		}
		if queue, ok := shard.GetVectorIndexQueue(""); ok && queue != nil {
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				assert.EqualValues(collect, 0, queue.Size())
			}, 30*time.Second, 100*time.Millisecond, "queue should drain")
		}

		compressedBucket := shard.Store().Bucket(helpers.GetCompressedBucketName(""))
		require.NotNil(t, compressedBucket, "compressed bucket must exist")

		// Halt using the test variant. inject runs AFTER the early flush and
		// AFTER prepare steps, simulating the queue-drain writes that the
		// production code missed when there was no late flush.
		inject := func() error {
			return compressedBucket.Put(markerKey, markerValue)
		}
		require.NoError(t, shard.haltForTransferForTest(ctx, inject, finalFlush))

		files, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)

		hits := countSSTsWithMarker(t, shard, files)

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		return hits
	}

	t.Run("pre-fix: post-flush writes are lost from the backup", func(t *testing.T) {
		hits := runScenario(t, false)
		assert.Equal(t, 0, hits,
			"expected marker to be missing from backup SSTs when no late flush runs; "+
				"this proves the bug the fix targets is actually reproducible")
	})

	t.Run("fix: late flush captures post-flush writes in the backup", func(t *testing.T) {
		hits := runScenario(t, true)
		assert.GreaterOrEqual(t, hits, 1,
			"expected marker to appear in at least one compressed-bucket SST after the late flush; "+
				"the fix must persist writes made between the early flush and list-files")
	})
}
