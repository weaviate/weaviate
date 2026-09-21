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

package shardusage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
)

// The dimensions bucket is migrated next to itself. The roaring set bucket is
// built under the build suffix and renamed to the ready suffix once it is
// complete and durable. The switch then takes two renames:
//
//	dimensions       -> dimensions___del   (the point of no return)
//	dimensions…ready -> dimensions
//
// Up to the first rename the map bucket is untouched and whatever was built is
// thrown away and rebuilt. Past it the ready bucket is the only complete copy,
// so [RecoverDimensionsBucketMigration] finishes the switch, and it has to run
// before anything opens the bucket, whether or not the migration is enabled.
const (
	dimensionsMigrationBuildSuffix = "__to_roaringset_build"
	dimensionsMigrationReadySuffix = "__to_roaringset_ready"
	dimensionsMigrationDelSuffix   = "___del"
)

// LockUnloadedDimensionsBucket waits for exclusive use of the dimensions bucket
// dir of a shard, against usage scans that open the bucket of an unloaded shard.
// The shard takes it while it migrates and while it loads the bucket, as lsmkv
// refuses to open one bucket dir twice.
func LockUnloadedDimensionsBucket(ctx context.Context, indexPath, shardName string) (unlock func(), err error) {
	bucketPath := shardPathDimensionsLSM(indexPath, shardName)
	if err := unloadedDimensionsBucketLocks.LockWithContext(bucketPath, ctx); err != nil {
		return nil, fmt.Errorf("lock dimensions bucket: %w", err)
	}
	return func() { unloadedDimensionsBucketLocks.Unlock(bucketPath) }, nil
}

// PrepareDimensionsBucket has to run before a shard opens its dimensions bucket.
// It recovers an interrupted migration, also with migrate turned off since, and
// with migrate set migrates a bucket still using the map strategy.
//
// A migration that fails is logged and not returned: the map bucket keeps
// working and the next load of the shard tries again. The caller must hold the
// lock of [LockUnloadedDimensionsBucket].
func PrepareDimensionsBucket(ctx context.Context, logger logrus.FieldLogger,
	indexPath, shardName string, migrate bool,
) error {
	if err := RecoverDimensionsBucketMigration(logger, indexPath, shardName); err != nil {
		return fmt.Errorf("recover dimensions bucket migration: %w", err)
	}
	if !migrate {
		return nil
	}
	if _, err := MigrateDimensionsBucketToRoaringSet(ctx, logger, indexPath, shardName); err != nil {
		logger.WithField("action", "dimensions_bucket_migration").
			WithField("path", shardPathDimensionsLSM(indexPath, shardName)).
			Errorf("migrate dimensions bucket to roaring set: %v", err)
		// it may have failed past the point of no return
		if err := RecoverDimensionsBucketMigration(logger, indexPath, shardName); err != nil {
			return fmt.Errorf("recover dimensions bucket migration: %w", err)
		}
	}
	return nil
}

// RecoverDimensionsBucketMigration finishes or rolls back a migration that was
// interrupted, so that the dimensions bucket dir holds a complete bucket again.
// It is cheap when there is nothing to recover. The caller must hold the lock of
// [LockUnloadedDimensionsBucket] and the bucket must not be open.
func RecoverDimensionsBucketMigration(logger logrus.FieldLogger, indexPath, shardName string) error {
	return recoverDimensionsBucketMigration(logger, shardPathDimensionsLSM(indexPath, shardName))
}

// MigrateDimensionsBucketToRoaringSet rewrites a dimensions bucket of the map
// strategy into one of the roaring set strategy and reports whether it did. Any
// other bucket, as well as a missing or empty one, is left alone. The doc ids
// are copied from the map bucket, objects are not read.
//
// The caller must hold the lock of [LockUnloadedDimensionsBucket], must have run
// [RecoverDimensionsBucketMigration], and the bucket must not be open.
func MigrateDimensionsBucketToRoaringSet(ctx context.Context, logger logrus.FieldLogger,
	indexPath, shardName string,
) (bool, error) {
	bucketPath := shardPathDimensionsLSM(indexPath, shardName)

	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	if err != nil {
		return false, fmt.Errorf("determine dimensions bucket strategy: %w", err)
	}
	if strategy != lsmkv.StrategyMapCollection {
		return false, nil
	}

	start := time.Now()
	rootPath := shardPathLSM(indexPath, shardName)
	buildPath := bucketPath + dimensionsMigrationBuildSuffix
	readyPath := bucketPath + dimensionsMigrationReadySuffix

	rows, err := buildRoaringSetDimensionsBucket(ctx, logger, rootPath, bucketPath, buildPath)
	if err != nil {
		if rmErr := os.RemoveAll(buildPath); rmErr != nil {
			logger.WithField("path", buildPath).Warnf("failed to remove dir: %v", rmErr)
		}
		return false, err
	}

	if err := os.Rename(buildPath, readyPath); err != nil {
		return false, fmt.Errorf("mark roaring set dimensions bucket ready: %w", err)
	}
	if err := diskio.Fsync(rootPath); err != nil {
		return false, fmt.Errorf("fsync %q: %w", rootPath, err)
	}
	if err := switchDimensionsBucket(bucketPath, rootPath); err != nil {
		return false, err
	}

	logger.WithField("action", "dimensions_bucket_migration").
		WithField("path", bucketPath).
		WithField("rows", rows).
		WithField("took", time.Since(start)).
		Info("migrated dimensions bucket to roaring set")
	return true, nil
}

func buildRoaringSetDimensionsBucket(ctx context.Context, logger logrus.FieldLogger,
	rootPath, mapPath, buildPath string,
) (rows int, err error) {
	if err := os.RemoveAll(buildPath); err != nil {
		return 0, fmt.Errorf("remove stale dir %q: %w", buildPath, err)
	}

	source, err := openDimensionsBucket(ctx, logger, rootPath, mapPath, lsmkv.StrategyMapCollection)
	if err != nil {
		return 0, fmt.Errorf("open map dimensions bucket: %w", err)
	}
	defer func() {
		if shutdownErr := source.Shutdown(ctx); shutdownErr != nil && err == nil {
			err = fmt.Errorf("shutdown map dimensions bucket: %w", shutdownErr)
		}
	}()

	target, err := openDimensionsBucket(ctx, logger, rootPath, buildPath, lsmkv.StrategyRoaringSet)
	if err != nil {
		return 0, fmt.Errorf("open roaring set dimensions bucket: %w", err)
	}
	targetOpen := true
	defer func() {
		if targetOpen {
			if shutdownErr := target.Shutdown(ctx); shutdownErr != nil {
				logger.WithField("path", buildPath).Warnf("failed to shutdown bucket: %v", shutdownErr)
			}
		}
	}()

	rows, err = copyDimensionsMapToRoaringSet(ctx, source, target)
	if err != nil {
		return 0, err
	}

	// a shutdown alone may keep a small memtable in its commit log
	if err := target.FlushAndSwitch(); err != nil {
		return 0, fmt.Errorf("flush roaring set dimensions bucket: %w", err)
	}
	targetOpen = false
	if err := target.Shutdown(ctx); err != nil {
		return 0, fmt.Errorf("shutdown roaring set dimensions bucket: %w", err)
	}
	if err := diskio.Fsync(buildPath); err != nil {
		return 0, fmt.Errorf("fsync %q: %w", buildPath, err)
	}
	return rows, nil
}

func copyDimensionsMapToRoaringSet(ctx context.Context, source, target *lsmkv.Bucket) (int, error) {
	cursor, err := source.MapCursor()
	if err != nil {
		return 0, fmt.Errorf("create cursor: %w", err)
	}
	defer cursor.Close()

	rows := 0
	// the cursor drops deleted doc ids and rows left without any
	for key, pairs := cursor.First(ctx); key != nil; key, pairs = cursor.Next(ctx) {
		docIDs := sroar.NewBitmap()
		for i := range pairs {
			if len(pairs[i].Key) != 8 {
				return 0, fmt.Errorf("dimensions key %x: doc id of %d bytes", key, len(pairs[i].Key))
			}
			docIDs.Set(binary.LittleEndian.Uint64(pairs[i].Key))
		}
		if err := target.RoaringSetAddBitmap(key, docIDs); err != nil {
			return 0, fmt.Errorf("write dimensions key %x: %w", key, err)
		}
		rows++
	}
	// an expired context ends the cursor like the last row does
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("copy dimensions: %w", err)
	}
	return rows, nil
}

func openDimensionsBucket(ctx context.Context, logger logrus.FieldLogger,
	rootPath, bucketPath, strategy string,
) (*lsmkv.Bucket, error) {
	return lsmkv.NewBucketCreator().NewBucket(ctx,
		bucketPath,
		rootPath,
		logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(strategy),
		lsmkv.WithSequentialAccess(true),
	)
}

// switchDimensionsBucket puts the ready bucket in place of the dimensions
// bucket. It is safe to repeat from any point it was interrupted at.
func switchDimensionsBucket(bucketPath, rootPath string) error {
	readyPath := bucketPath + dimensionsMigrationReadySuffix
	delPath := bucketPath + dimensionsMigrationDelSuffix

	delExists, err := dirExists(delPath)
	if err != nil {
		return err
	}
	if !delExists {
		if err := os.Rename(bucketPath, delPath); err != nil {
			return fmt.Errorf("move map dimensions bucket aside: %w", err)
		}
	}
	if err := os.Rename(readyPath, bucketPath); err != nil {
		return fmt.Errorf("move roaring set dimensions bucket in place: %w", err)
	}
	if err := diskio.Fsync(rootPath); err != nil {
		return fmt.Errorf("fsync %q: %w", rootPath, err)
	}
	if err := os.RemoveAll(delPath); err != nil {
		return fmt.Errorf("remove map dimensions bucket %q: %w", delPath, err)
	}
	return nil
}

func recoverDimensionsBucketMigration(logger logrus.FieldLogger, bucketPath string) error {
	buildPath := bucketPath + dimensionsMigrationBuildSuffix
	readyPath := bucketPath + dimensionsMigrationReadySuffix
	delPath := bucketPath + dimensionsMigrationDelSuffix
	rootPath := filepath.Dir(bucketPath)

	buildExists, err := dirExists(buildPath)
	if err != nil {
		return err
	}
	if buildExists {
		if err := os.RemoveAll(buildPath); err != nil {
			return fmt.Errorf("remove incomplete dimensions bucket %q: %w", buildPath, err)
		}
	}

	readyExists, err := dirExists(readyPath)
	if err != nil {
		return err
	}
	delExists, err := dirExists(delPath)
	if err != nil {
		return err
	}

	switch {
	case !readyExists && !delExists:
		return nil
	case !readyExists:
		// the switch was done, only the map bucket was left to remove
		if err := os.RemoveAll(delPath); err != nil {
			return fmt.Errorf("remove map dimensions bucket %q: %w", delPath, err)
		}
		return nil
	case !delExists:
		// the map bucket was not moved aside yet and may have been written to since
		if err := os.RemoveAll(readyPath); err != nil {
			return fmt.Errorf("remove unused dimensions bucket %q: %w", readyPath, err)
		}
		return nil
	}

	// Between the two renames. A bucket that was opened meanwhile has left an
	// empty dir behind, which the second rename would trip over.
	bucketExists, err := dirExists(bucketPath)
	if err != nil {
		return err
	}
	if bucketExists {
		hasData, err := dirHasData(bucketPath)
		if err != nil {
			return err
		}
		if hasData {
			// Written to by a version that does not know this migration. Neither
			// copy is complete, so nothing is removed.
			logger.WithField("action", "dimensions_bucket_migration").
				WithField("path", bucketPath).
				Errorf("torn state: %q holds data next to an unfinished migration (%q, %q), "+
					"tracked dimensions are incomplete until they are recalculated",
					bucketPath, readyPath, delPath)
			return nil
		}
		if err := os.RemoveAll(bucketPath); err != nil {
			return fmt.Errorf("remove empty dimensions bucket %q: %w", bucketPath, err)
		}
	}

	logger.WithField("action", "dimensions_bucket_migration").
		WithField("path", bucketPath).
		Info("finishing interrupted dimensions bucket migration")
	return switchDimensionsBucket(bucketPath, rootPath)
}

func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("stat %q: %w", path, err)
	}
	return info.IsDir(), nil
}

// dirHasData reports whether any file in the dir is not empty.
func dirHasData(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false, fmt.Errorf("read dir %q: %w", path, err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return false, fmt.Errorf("stat %q: %w", entry.Name(), err)
		}
		if info.IsDir() || info.Size() > 0 {
			return true, nil
		}
	}
	return false, nil
}
