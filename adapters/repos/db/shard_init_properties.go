//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
			err := s.removeBucket(ctx, helpers.BucketFromPropNameLSM(prop.Name))
			if err != nil {
				return fmt.Errorf("cannot remove filterable index for property: %s %w", prop.Name, err)
			}
		}
		if !inverted.HasSearchableIndex(prop) {
			err := s.removeBucket(ctx, helpers.BucketSearchableFromPropNameLSM(prop.Name))
			if err != nil {
				return fmt.Errorf("cannot remove searchable index for property: %s %w", prop.Name, err)
			}
		}
		if !inverted.HasRangeableIndex(prop) {
			err := s.removeBucket(ctx, helpers.BucketRangeableFromPropNameLSM(prop.Name))
			if err != nil {
				return fmt.Errorf("cannot remove rangeable index for property: %s %w", prop.Name, err)
			}
		}
		return nil
	})
}

func (s *Shard) removeBucket(ctx context.Context, bucketName string) error {
	if bucket := s.store.Bucket(bucketName); bucket != nil {
		bucketDir := bucket.GetDir()
		err := s.store.ShutdownBucket(ctx, bucketName)
		if err != nil {
			return fmt.Errorf("cannot shutdown bucket %s: %w", bucketName, err)
		}
		err = os.RemoveAll(bucketDir)
		if err != nil {
			return fmt.Errorf("cannot delete bucket directory %s: %w", bucketDir, err)
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
