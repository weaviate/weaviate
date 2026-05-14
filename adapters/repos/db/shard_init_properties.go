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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func (s *Shard) initProperties(eg *enterrors.ErrorGroupWrapper, class *models.Class) {
	ctx := context.TODO()

	s.propertyIndices = propertyspecific.Indices{}
	if class == nil {
		return
	}

	s.initPropertyBuckets(ctx, eg, s.lazySegmentLoadingEnabled, class.Properties...)

	eg.Go(func() error {
		return s.addIDProperty(ctx)
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			return s.addTimestampProperties(ctx)
		})
	}

	if s.index.Config.TrackVectorDimensions {
		eg.Go(func() error {
			return s.addDimensionsProperty(ctx)
		})
	}
}

func (s *Shard) initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper,
	lazyLoadSegments bool, props ...*models.Property,
) {
	makeBucketOptions := s.makeDefaultBucketOptions
	if lazyLoadSegments != s.lazySegmentLoadingEnabled {
		makeBucketOptions = s.overwrittenMakeDefaultBucketOptions(lsmkv.WithLazySegmentLoading(lazyLoadSegments))
	}

	for _, prop := range props {
		if !inverted.HasAnyInvertedIndex(prop) {
			continue
		}

		propCopy := *prop // prevent loop variable capture

		eg.Go(func() error {
			if err := s.createPropertyValueIndex(ctx, &propCopy, makeBucketOptions); err != nil {
				return fmt.Errorf("init prop %q: value index: %w", propCopy.Name, err)
			}
			return nil
		})

		if s.index.invertedIndexConfig.IndexNullState {
			eg.Go(func() error {
				if err := s.createPropertyNullIndex(ctx, &propCopy, makeBucketOptions); err != nil {
					return fmt.Errorf("init prop %q: null index: %w", prop.Name, err)
				}
				return nil
			})
		}

		if s.index.invertedIndexConfig.IndexPropertyLength {
			eg.Go(func() error {
				if err := s.createPropertyLengthIndex(ctx, &propCopy, makeBucketOptions); err != nil {
					return fmt.Errorf("init prop %q: length index: %w", prop.Name, err)
				}
				return nil
			})
		}
	}
}

func (s *Shard) updatePropertyBuckets(ctx context.Context,
	eg *enterrors.ErrorGroupWrapper,
	prop *models.Property,
) {
	eg.Go(func() error {
		if !inverted.HasFilterableIndex(prop) {
			mainBucket := helpers.BucketFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove filterable index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "filterable")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		if !inverted.HasSearchableIndex(prop) {
			mainBucket := helpers.BucketSearchableFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove searchable index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "searchable")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		if !inverted.HasRangeableIndex(prop) {
			mainBucket := helpers.BucketRangeableFromPropNameLSM(prop.Name)
			err := s.removeBucket(ctx, mainBucket)
			if err != nil {
				return fmt.Errorf("cannot remove rangeable index for %s property: %w", prop.Name, err)
			}
			s.cleanStaleMigrationDirs(prop.Name, "rangeable")
			s.cleanStaleSidecarDirs(mainBucket)
		}
		return nil
	})
}

// cleanStaleMigrationDirs removes the per-property runtime-reindex
// migration directories whose tidied sentinel would lie now that the
// (propName, indexType) bucket has been removed. Without this, a
// subsequent re-enable of the same index would short-circuit on the
// stale sentinel, re-flip the schema flag to true, and report success
// while leaving the underlying bucket empty.
//
// Errors are logged but not propagated: the bucket has already been
// removed by the time we get here, so the user's DELETE has succeeded
// at the only level that matters for correctness. A failure here only
// affects the next re-enable, which will trigger the defense-in-depth
// check in OnAfterLsmInitAsync and fail with a clear operator error.
func (s *Shard) cleanStaleMigrationDirs(propName, indexType string) {
	migrationsRoot := filepath.Join(s.pathLSM(), ".migrations")
	for _, dir := range migrationDirsForPropertyIndex(propName, indexType) {
		path := filepath.Join(migrationsRoot, dir)
		if err := os.RemoveAll(path); err != nil {
			s.index.logger.WithField("path", path).
				Error(fmt.Errorf("failed to clean up stale migration directory after index DELETE: %w; subsequent re-enable will fail loudly via the stale-sentinel check until this directory is removed manually", err))
		}
	}
}

// cleanStaleSidecarDirs removes leftover __reindex / __ingest / __backup
// sidecar directories that share the just-removed bucket's name as their
// prefix. A successful migration moves the new data into the main bucket
// dir at runtime but defers the actual filesystem renames (old-main ->
// __backup, ingest-dir cleanup) to OnBeforeLsmInit on the next restart.
// Between completion and restart these sidecars live on disk; a DELETE
// then re-enable in the same process lifetime would otherwise hit
// "rename: file exists" the next time RunSwapOnShard tries to move the
// fresh main into __backup.
//
// The sidecar names are <mainBucket>__<strategy-specific-suffix>, where
// suffixes vary per strategy. Matching by prefix avoids hard-coding every
// strategy's suffixes here and naturally absorbs future strategies.
func (s *Shard) cleanStaleSidecarDirs(mainBucketName string) {
	entries, err := os.ReadDir(s.pathLSM())
	if err != nil {
		s.index.logger.WithField("path", s.pathLSM()).
			Error(fmt.Errorf("failed to enumerate LSM dir for sidecar cleanup after DELETE: %w; a subsequent re-enable may fail with 'file exists' when RunSwapOnShard tries to rotate buckets", err))
		return
	}
	prefix := mainBucketName + "__"
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		path := filepath.Join(s.pathLSM(), entry.Name())
		if err := os.RemoveAll(path); err != nil {
			s.index.logger.WithField("path", path).
				Error(fmt.Errorf("failed to remove stale sidecar bucket dir after index DELETE: %w", err))
		}
	}
}

func (s *Shard) removeBucket(ctx context.Context, bucketName string) error {
	bucket := s.store.Bucket(bucketName)
	if bucket == nil {
		return nil // bucket doesn't exist, nothing to remove
	}
	// Shutdown the bucket first - after this point, the bucket cannot be used
	if err := s.store.ShutdownBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("failed to shutdown bucket %s: %w", bucketName, err)
	}
	// Remove the bucket's directory from disk
	// If this fails after successful shutdown, we're in an inconsistent state:
	// the bucket is removed from the store but its data remains on disk
	if err := s.removeBucketDir(s.pathLSM(), bucketName); err != nil {
		return fmt.Errorf("bucket %s shut down successfully but directory removal failed: %w", bucketName, err)
	}
	return nil
}

func (s *Shard) removeBucketDir(pathLSM, bucketName string) error {
	bucketDir := filepath.Join(pathLSM, bucketName)
	if _, err := os.Stat(bucketDir); !os.IsNotExist(err) {
		if err := os.RemoveAll(bucketDir); err != nil {
			return fmt.Errorf("failed to remove data for %s bucket: "+
				"orphaned data remains at %s (manual cleanup may be required): %w", bucketName, bucketDir, err)
		}
	}
	return nil
}

func (s *Shard) createPropertyValueIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	if inverted.HasFilterableIndex(prop) {
		if dt, _ := schema.AsPrimitive(prop.DataType); dt == schema.DataTypeGeoCoordinates {
			return s.initGeoProp(prop)
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := s.store.CreateOrLoadBucket(ctx,
				helpers.BucketFromPropNameMetaCountLSM(prop.Name),
				makeBucketOptions(lsmkv.StrategyRoaringSet)...,
			); err != nil {
				return err
			}
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketFromPropNameLSM(prop.Name),
			makeBucketOptions(lsmkv.StrategyRoaringSet)...,
		); err != nil {
			return err
		}
	}

	if inverted.HasSearchableIndex(prop) {
		strategy := lsmkv.DefaultSearchableStrategy(s.usingBlockMaxWAND)
		searchableBucketOpts := makeBucketOptions(strategy)

		if s.class.InvertedIndexConfig != nil {
			searchableBucketOpts = append(searchableBucketOpts, lsmkv.WithBM25Config(s.class.InvertedIndexConfig.Bm25))
		}

		bucketName := helpers.BucketSearchableFromPropNameLSM(prop.Name)
		if err := s.store.CreateOrLoadBucket(ctx, bucketName, searchableBucketOpts...); err != nil {
			return err
		}

		if actualStrategy := s.store.Bucket(bucketName).Strategy(); actualStrategy == lsmkv.StrategyInverted {
			s.markSearchableBlockmaxProperties(prop.Name)
		}
	}

	if inverted.HasRangeableIndex(prop) {
		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketRangeableFromPropNameLSM(prop.Name),
			makeBucketOptions(lsmkv.StrategyRoaringSetRange)...,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) createPropertyLengthIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	// some datatypes are not added to the inverted index, so we can skip them here
	switch schema.DataType(prop.DataType[0]) {
	case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeInt,
		schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return nil
	default:
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLengthLSM(prop.Name),
		makeBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) createPropertyNullIndex(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameNullLSM(prop.Name),
		makeBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) addIDProperty(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	err := s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropID),
		s.makeDefaultBucketOptions(lsmkv.StrategySetCollection)...,
	)
	if err != nil {
		return fmt.Errorf("create id property: %w", err)
	}
	return nil
}

func (s *Shard) createDimensionsBucket(ctx context.Context, name string) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	bucketPath := filepath.Join(s.pathLSM(), name)
	strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
	if err != nil {
		return fmt.Errorf("determine dimensions bucket strategy: %w", err)
	}

	if err = s.store.CreateOrLoadBucket(ctx, name, s.makeDefaultBucketOptions(strategy)...); err != nil {
		return fmt.Errorf("create dimensions bucket: %w", err)
	}
	return nil
}

func (s *Shard) addDimensionsProperty(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	// Note: this data would fit the "Set" type better, but since the "Map" type
	// is currently optimized better, it is more efficient to use a Map here.
	err := s.createDimensionsBucket(ctx, helpers.DimensionsBucketLSM)
	if err != nil {
		return fmt.Errorf("create dimensions tracking property: %w", err)
	}

	return nil
}

func (s *Shard) addTimestampProperties(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	if err := s.addCreationTimeUnixProperty(ctx); err != nil {
		return fmt.Errorf("create creation time property: %w", err)
	}

	if err := s.addLastUpdateTimeUnixProperty(ctx); err != nil {
		return fmt.Errorf("create last update time property: %w", err)
	}

	return nil
}

func (s *Shard) addCreationTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropCreationTimeUnix),
		s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) addLastUpdateTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		s.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...,
	)
}

func (s *Shard) markSearchableBlockmaxProperties(propNames ...string) {
	s.searchableBlockmaxPropNamesLock.Lock()
	s.searchableBlockmaxPropNames = append(s.searchableBlockmaxPropNames, propNames...)
	s.searchableBlockmaxPropNamesLock.Unlock()
}

func (s *Shard) getSearchableBlockmaxProperties() []string {
	// since slice is only appended, it should be safe to return it that way
	s.searchableBlockmaxPropNamesLock.Lock()
	defer s.searchableBlockmaxPropNamesLock.Unlock()
	return s.searchableBlockmaxPropNames
}
