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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/backup"
)

func shardIsHalted(idx *Index, shardName string) bool {
	_, ok := idx.haltedShardsForTransfer.Load(shardName)
	return ok
}

func TestShard_InitHaltedDuringBackup(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"

	shd, idx := testShard(t, ctx, className)

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(idx.Config.RootPath)

	amount := 10

	t.Run("insert data into shard", func(t *testing.T) {
		for range amount {
			obj := testObject(className)
			err := shd.PutObject(ctx, obj)
			require.Nil(t, err)
		}
	})

	t.Run("halt shard for backup", func(t *testing.T) {
		require.NoError(t, idx.initBackup("test-backup"))
		require.NoError(t, shd.HaltForTransfer(ctx, false, 0))

		// Shard should be in halted map
		require.True(t, shardIsHalted(idx, shd.Name()))

		// Backup files should be listable while halted
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.NoError(t, err)
	})

	t.Run("releaseBackupAndResume should resume shard and clear state", func(t *testing.T) {
		err := idx.releaseBackupAndResume(ctx)
		require.NoError(t, err)

		// Halted map should be cleared
		require.False(t, shardIsHalted(idx, shd.Name()))

		// lastBackup should be cleared
		require.Nil(t, idx.lastBackup.Load())
	})

	t.Run("backup files cannot be listed after release", func(t *testing.T) {
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer")
	})

	t.Run("objects can be inserted after release", func(t *testing.T) {
		for range amount {
			obj := testObject(className)
			err := shd.PutObject(ctx, obj)
			require.NoError(t, err)
		}

		objs, err := shd.ObjectList(ctx, 2*amount, nil, nil, additional.Properties{}, idx.Config.ClassName)
		require.NoError(t, err)
		require.Equal(t, 2*amount, len(objs))
	})
}

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
		inactivityTimeout := 10 * time.Millisecond

		err := shd.HaltForTransfer(ctx, false, inactivityTimeout)
		require.NoError(t, err)

		time.Sleep(inactivityTimeout * 10) // wait for inactivity timeout to elapse
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

// TestShard_ReleaseBeforeShardStored tests the scenario where releaseBackupAndResume
// completes before a halted shard is stored in the shard map. The shard must
// self-resume via maybeResumeAfterInit because releaseBackupAndResume's ForEachShard
// iteration won't see it.
func TestShard_ReleaseBeforeShardStored(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"

	// Create shard without storing it in the index's shard map yet.
	// We pass an indexOpt that sets DisableLazyLoadShards so we get a *Shard.
	shd, idx := testShard(t, ctx, className, func(i *Index) {
		i.Config.DisableLazyLoadShards = true
	})

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(idx.Config.RootPath)

	// Insert data
	for range 10 {
		require.NoError(t, shd.PutObject(ctx, testObject(className)))
	}

	s := shd.(*Shard)

	// Start a backup and halt the shard
	require.NoError(t, idx.initBackup("test-backup"))
	require.NoError(t, shd.HaltForTransfer(ctx, false, 0))
	require.True(t, shardIsHalted(idx, shd.Name()))

	// Simulate the shard having been initialized halted (as NewShard would set it)
	s.haltedOnInit = true

	// Now simulate the race: release completes BEFORE the shard calls maybeResumeAfterInit.
	// Remove the shard from the shard map so releaseBackupAndResume won't find it.
	idx.shards.LoadAndDelete(shd.Name())

	require.NoError(t, idx.releaseBackupAndResume(ctx))

	// The shard is still halted because releaseBackupAndResume couldn't see it.
	err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err, "shard should still be halted since release didn't see it")

	// Now store the shard and call maybeResumeAfterInit, as production code does.
	idx.shards.Store(shd.Name(), shd)
	s.maybeResumeAfterInit(ctx)

	// Shard should now be resumed via self-resume
	err = shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.ErrorContains(t, err, "not paused for transfer")

	// Objects should be insertable
	require.NoError(t, shd.PutObject(ctx, testObject(className)))
}

// TestShard_ReleaseDuringShardInit tests concurrent releaseBackupAndResume and
// shard init. Regardless of which completes first, the shard must end up resumed.
func TestShard_ReleaseDuringShardInit(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"

	shd, idx := testShard(t, ctx, className, func(i *Index) {
		i.Config.DisableLazyLoadShards = true
	})

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(idx.Config.RootPath)

	s := shd.(*Shard)

	for range 10 {
		require.NoError(t, shd.PutObject(ctx, testObject(className)))
	}

	// Start backup and halt
	require.NoError(t, idx.initBackup("test-backup"))
	require.NoError(t, shd.HaltForTransfer(ctx, false, 0))

	// Simulate the shard having been initialized halted
	s.haltedOnInit = true

	// Remove shard from map to simulate it not being stored yet
	idx.shards.LoadAndDelete(shd.Name())

	// Race: releaseBackupAndResume and shard registration run concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		idx.releaseBackupAndResume(ctx)
	}()

	go func() {
		defer wg.Done()
		idx.shards.Store(shd.Name(), shd)
		s.maybeResumeAfterInit(ctx)
	}()

	wg.Wait()

	// Regardless of ordering, the shard must be resumed.
	// Either releaseBackupAndResume saw it in ForEachShard, or
	// maybeResumeAfterInit saw the halted map was cleared.
	err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.ErrorContains(t, err, "not paused for transfer",
		"shard must be resumed regardless of race outcome")

	require.NoError(t, shd.PutObject(ctx, testObject(className)))
}

// TestShard_ReleaseDuringShardInitRepeated runs the concurrent race between
// releaseBackupAndResume and shard init many times to exercise different
// interleavings and catch races reliably.
func TestShard_ReleaseDuringShardInitRepeated(t *testing.T) {
	ctx := testCtx()
	className := "TestClass"

	shd, idx := testShard(t, ctx, className, func(i *Index) {
		i.Config.DisableLazyLoadShards = true
	})

	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Println(err)
		}
	}(idx.Config.RootPath)

	s := shd.(*Shard)

	for range 10 {
		require.NoError(t, shd.PutObject(ctx, testObject(className)))
	}

	for iteration := range 20 {
		// Start backup and halt
		require.NoError(t, idx.initBackup(fmt.Sprintf("backup-%d", iteration)))
		require.NoError(t, shd.HaltForTransfer(ctx, false, 0))

		// Simulate the shard having been initialized halted
		s.haltedOnInit = true

		// Remove shard from map to simulate it not being stored yet
		idx.shards.LoadAndDelete(shd.Name())

		// Race: releaseBackupAndResume and shard registration run concurrently
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			idx.releaseBackupAndResume(ctx)
		}()

		go func() {
			defer wg.Done()
			idx.shards.Store(shd.Name(), shd)
			s.maybeResumeAfterInit(ctx)
		}()

		wg.Wait()

		// Shard must always be resumed
		err := shd.ListBackupFiles(ctx, &backup.ShardDescriptor{})
		require.ErrorContains(t, err, "not paused for transfer",
			"iteration %d: shard must be resumed regardless of race outcome", iteration)
	}
}
