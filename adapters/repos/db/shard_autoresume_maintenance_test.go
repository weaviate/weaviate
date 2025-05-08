//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
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
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
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
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
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
		inactivityTimeout := 100 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)

		time.Sleep(inactivityTimeout * 2)
	})

	t.Run("attempt to list backup files after inactivity time should fail", func(t *testing.T) {
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
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
		err := shd.ListBackupFiles(ctx, backupDescriptor)
		require.NoError(t, err)
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
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
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
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
	})

	t.Run("resume maintenance tasks", func(t *testing.T) {
		err := shd.resumeMaintenanceCycles(ctx)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files with one halt request still active should succeed", func(t *testing.T) {
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
	})

	t.Run("resume maintenance tasks", func(t *testing.T) {
		err := shd.resumeMaintenanceCycles(ctx)
		require.NoError(t, err)
	})

	t.Run("attempt to list backup files after resuming for a second time should fail", func(t *testing.T) {
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer")
	})

	require.Nil(t, idx.drop())
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
}
