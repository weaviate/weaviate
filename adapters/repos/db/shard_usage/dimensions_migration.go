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

// The dimensions bucket is migrated next to itself: the roaring set copy is built
// under the build suffix, renamed to the ready suffix once complete and durable,
// then swapped in via two renames:
//
//	dimensions       -> dimensions___del
//	dimensions…ready -> dimensions
//
// Before the first rename, the map bucket is untouched and any partial build is
// discarded and retried. Past it, no bucket is under the name and opening it would
// create an empty one, so [RecoverDimensionsBucketMigration] must finish the switch
// before anything opens the bucket, migration enabled or not.
const (
	dimensionsMigrationBuildSuffix = "__to_roaringset_build"
	dimensionsMigrationReadySuffix = DimensionsReplacementBucketSuffix
	dimensionsMigrationDelSuffix   = DimensionsReplacedBucketSuffix
)

// DimensionsReplacedBucketSuffix is the one lsmkv.Store.ReplaceBuckets uses too.
const DimensionsReplacedBucketSuffix = "___del"

// DimensionsReplacementBucketSuffix names a bucket built to replace the dimensions
// bucket. It is complete once the bucket in place is moved to DimensionsReplacedBucketSuffix.
const DimensionsReplacementBucketSuffix = "__to_roaringset_ready"

// LockUnloadedDimensionsBucket keeps usage scans of an unloaded shard off the
// dimensions bucket dir, as lsmkv refuses to open one bucket dir twice.
func LockUnloadedDimensionsBucket(ctx context.Context, indexPath, shardName string) (unlock func(), err error) {
	bucketPath := shardPathDimensionsLSM(indexPath, shardName)
	if err := unloadedDimensionsBucketLocks.LockWithContext(bucketPath, ctx); err != nil {
		return nil, fmt.Errorf("lock dimensions bucket: %w", err)
	}
	return func() { unloadedDimensionsBucketLocks.Unlock(bucketPath) }, nil
}

// PrepareDimensionsBucket must run, under [LockUnloadedDimensionsBucket], before a
// shard opens its dimensions bucket. A failed migration is only logged: the map
// bucket keeps working and the next load retries.
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
		// it may have failed between the two renames
		if err := RecoverDimensionsBucketMigration(logger, indexPath, shardName); err != nil {
			return fmt.Errorf("recover dimensions bucket migration: %w", err)
		}
	}
	return nil
}

// RecoverDimensionsBucketMigration finishes or rolls back an interrupted migration.
// It needs [LockUnloadedDimensionsBucket] held and the bucket not open.
func RecoverDimensionsBucketMigration(logger logrus.FieldLogger, indexPath, shardName string) error {
	return recoverDimensionsBucketMigration(logger, shardPathDimensionsLSM(indexPath, shardName))
}

// MigrateDimensionsBucketToRoaringSet rewrites a map dimensions bucket as a roaring
// set one and reports whether it did. It needs [LockUnloadedDimensionsBucket] held,
// [RecoverDimensionsBucketMigration] run, and the bucket not open.
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

	// Left over when recovery could not remove it. The switch would fail over it,
	// after a full copy on every load.
	for _, leftover := range []string{readyPath, bucketPath + dimensionsMigrationDelSuffix} {
		exists, err := dirExists(leftover)
		if err != nil {
			return false, err
		}
		if exists {
			return false, fmt.Errorf("dimensions bucket %q is still there", leftover)
		}
	}

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
	if err := switchDimensionsBucket(logger, bucketPath, rootPath); err != nil {
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

// switchDimensionsBucket is safe to repeat from any point it was interrupted at.
func switchDimensionsBucket(logger logrus.FieldLogger, bucketPath, rootPath string) error {
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
	removeReplacedDimensionsBucket(logger, delPath)
	return nil
}

// removeReplacedDimensionsBucket only logs a failure, the next recovery retries.
func removeReplacedDimensionsBucket(logger logrus.FieldLogger, delPath string) {
	if err := os.RemoveAll(delPath); err != nil {
		logger.WithField("action", "dimensions_bucket_migration").
			WithField("path", delPath).
			Warnf("failed to remove replaced dimensions bucket: %v", err)
	}
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
		return recoverDimensionsBucketMovedAside(logger, bucketPath, delPath, rootPath)
	case !delExists:
		// The bucket in place was not moved aside yet and may have been written to
		// since. Nothing renames the ready one without a bucket moved aside, so one
		// that cannot be removed is only a leftover dir, not worth failing the load.
		if err := os.RemoveAll(readyPath); err != nil {
			logger.WithField("action", "dimensions_bucket_migration").
				WithField("path", readyPath).
				Warnf("failed to remove unused dimensions bucket: %v", err)
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
					"tracked dimensions are incomplete until REINDEX_VECTOR_DIMENSIONS_AT_STARTUP recalculates them",
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
	return switchDimensionsBucket(logger, bucketPath, rootPath)
}

// recoverDimensionsBucketMovedAside handles a bucket moved aside with no
// replacement to take its place: the switch went through, so the bucket in place
// is kept even if empty, since the moved-aside one may be partly removed. Only if
// no bucket is in place is the moved-aside one restored.
func recoverDimensionsBucketMovedAside(logger logrus.FieldLogger, bucketPath, delPath, rootPath string) error {
	bucketExists, err := dirExists(bucketPath)
	if err != nil {
		return err
	}
	if bucketExists {
		removeReplacedDimensionsBucket(logger, delPath)
		return nil
	}

	logger.WithField("action", "dimensions_bucket_migration").
		WithField("path", bucketPath).
		Warn("dimensions bucket was moved aside and not replaced, moving it back")
	if err := os.Rename(delPath, bucketPath); err != nil {
		return fmt.Errorf("move dimensions bucket back: %w", err)
	}
	if err := diskio.Fsync(rootPath); err != nil {
		return fmt.Errorf("fsync %q: %w", rootPath, err)
	}
	return nil
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
