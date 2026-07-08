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
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
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

func TestShard_HaltForTransferZeroPreparationTimeout(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"

	// a zeroed HaltForTransferTimeout must mean "no bound", not an
	// immediately-expired preparation context
	shd, idx := testShard(t, ctx, className, func(idx *Index) {
		idx.Config.HaltForTransferTimeout = 0
	})

	obj := testObject(className)
	require.NoError(t, shd.PutObject(ctx, obj))

	err := shd.HaltForTransfer(ctx, false, 0)
	require.NoError(t, err)

	require.NoError(t, shd.resumeMaintenanceCycles(ctx))

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

func TestShard_ListBackupFilesExtendsInactivityDeadline(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	for range 10 {
		require.NoError(t, shd.PutObject(ctx, testObject(className)))
	}

	s := shd.(*Shard)

	require.NoError(t, s.HaltForTransfer(ctx, false, time.Hour))

	s.haltForTransferMux.Lock()
	s.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
	s.haltForTransferMux.Unlock()

	_, err := s.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err)

	s.haltForTransferMux.Lock()
	deadline := s.haltForTransferInactivityDeadline
	s.haltForTransferMux.Unlock()

	require.True(t, deadline.After(time.Now()))

	require.NoError(t, s.resumeMaintenanceCycles(ctx))
	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_InactivityFireResumesWhenIdle(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	s := shd.(*Shard)

	require.NoError(t, s.HaltForTransfer(ctx, false, time.Hour))

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	s.haltForTransferMux.Lock()
	s.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
	s.haltForTransferMux.Unlock()

	keepWatching := s.handleInactivityFire(context.Background(), timer)

	s.haltForTransferMux.Lock()
	countAfterFire := s.haltForTransferCount
	cancelAfterFire := s.haltForTransferCtxCancel
	s.haltForTransferMux.Unlock()

	require.False(t, keepWatching)
	require.Zero(t, countAfterFire)
	require.Nil(t, cancelAfterFire)

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_ResumeClearsInactivityMonitorSentinel(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	err := shd.HaltForTransfer(ctx, false, time.Hour)
	require.NoError(t, err)

	s := shd.(*Shard)

	s.haltForTransferMux.Lock()
	cancelBeforeResume := s.haltForTransferCtxCancel
	resumeErr := s.mayForceResumeMaintenanceCycles(ctx, true)
	cancelAfterResume := s.haltForTransferCtxCancel
	countAfterResume := s.haltForTransferCount
	s.haltForTransferMux.Unlock()

	require.NotNil(t, cancelBeforeResume)
	require.NoError(t, resumeErr)
	require.Zero(t, countAfterResume)
	require.Nil(t, cancelAfterResume)

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_DropClearsInactivityMonitorSentinel(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, _ := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	s := shd.(*Shard)

	err := s.HaltForTransfer(ctx, false, time.Hour)
	require.NoError(t, err)

	err = s.drop(false)
	require.NoError(t, err)

	s.haltForTransferMux.Lock()
	cancelAfterDrop := s.haltForTransferCtxCancel
	s.haltForTransferMux.Unlock()

	require.Nil(t, cancelAfterDrop)
}

func TestShard_ShutdownClearsInactivityMonitorSentinel(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, _ := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	s := shd.(*Shard)

	err := s.HaltForTransfer(ctx, false, time.Hour)
	require.NoError(t, err)

	err = s.Shutdown(ctx)
	require.NoError(t, err)

	s.haltForTransferMux.Lock()
	cancelAfterShutdown := s.haltForTransferCtxCancel
	s.haltForTransferMux.Unlock()

	require.Nil(t, cancelAfterShutdown)
}

func TestShard_StaleMonitorFireIsDropped(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	s := shd.(*Shard)

	require.NoError(t, s.HaltForTransfer(ctx, false, time.Hour))

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	staleCtx, cancelStale := context.WithCancel(context.Background())
	cancelStale()

	s.haltForTransferMux.Lock()
	s.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
	s.haltForTransferMux.Unlock()

	keepWatching := s.handleInactivityFire(staleCtx, timer)

	s.haltForTransferMux.Lock()
	countAfterStaleFire := s.haltForTransferCount
	s.haltForTransferMux.Unlock()

	require.False(t, keepWatching)
	require.Equal(t, 1, countAfterStaleFire)

	require.NoError(t, s.resumeMaintenanceCycles(ctx))
	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_FutureDeadlinePreventsResumeOnFire(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	s := shd.(*Shard)

	require.NoError(t, s.HaltForTransfer(ctx, false, time.Hour))

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	s.haltForTransferMux.Lock()
	s.haltForTransferInactivityDeadline = time.Now().Add(time.Hour)
	s.haltForTransferMux.Unlock()

	keepWatching := s.handleInactivityFire(context.Background(), timer)

	s.haltForTransferMux.Lock()
	countAfterFire := s.haltForTransferCount
	s.haltForTransferMux.Unlock()

	require.True(t, keepWatching)
	require.Equal(t, 1, countAfterFire)

	require.NoError(t, s.resumeMaintenanceCycles(ctx))
	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}

func TestShard_FullResumeResetsInactivityTimeout(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"
	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(shd.Index().Config.RootPath)

	s := shd.(*Shard)

	err := s.HaltForTransfer(ctx, false, 10*time.Millisecond)
	require.NoError(t, err)

	err = s.resumeMaintenanceCycles(ctx)
	require.NoError(t, err)

	s.haltForTransferMux.Lock()
	timeoutAfterResume := s.haltForTransferInactivityTimeout
	deadlineAfterResume := s.haltForTransferInactivityDeadline
	s.haltForTransferMux.Unlock()

	require.Zero(t, timeoutAfterResume)
	require.True(t, deadlineAfterResume.IsZero())

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}
