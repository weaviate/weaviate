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

package db

import (
	"context"
	"fmt"
	"time"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/models"
)

func (s *Shard) initProperties(eg *enterrors.ErrorGroupWrapper, class *models.Class) {
	s.propertyIndices = propertyspecific.Indices{}
	if class == nil {
		return
	}

	s.initPropertyBuckets(context.Background(), eg, class.Properties...)

	eg.Go(func() error {
		return s.addIDProperty(context.TODO())
	})

	if s.index.invertedIndexConfig.IndexTimestamps {
		eg.Go(func() error {
			return s.addTimestampProperties(context.TODO())
		})
	}

	if s.index.Config.TrackVectorDimensions {
		eg.Go(func() error {
			return s.addDimensionsProperty(context.TODO())
		})
	}
}

func (s *Shard) initPropertyBuckets(ctx context.Context, eg *enterrors.ErrorGroupWrapper, props ...*models.Property) {
	for _, prop := range props {
		if !inverted.HasInvertedIndex(prop) {
			continue
		}

		propCopy := *prop // prevent loop variable capture

		eg.Go(func() error {
			if err := s.createPropertyValueIndex(ctx, &propCopy); err != nil {
				return fmt.Errorf("init prop %q: value index: %w", propCopy.Name, err)
			}
			return nil
		})

		if s.index.invertedIndexConfig.IndexNullState {
			eg.Go(func() error {
				if err := s.createPropertyNullIndex(ctx, &propCopy); err != nil {
					return fmt.Errorf("init prop %q: null index: %w", prop.Name, err)
				}
				return nil
			})
		}

		if s.index.invertedIndexConfig.IndexPropertyLength {
			eg.Go(func() error {
				if err := s.createPropertyLengthIndex(ctx, &propCopy); err != nil {
					return fmt.Errorf("init prop %q: length index: %w", prop.Name, err)
				}
				return nil
			})
		}
	}
}

func (s *Shard) createPropertyValueIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	bucketOpts := []lsmkv.BucketOption{
		s.memtableDirtyConfig(),
		s.dynamicMemtableSizing(),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours) * time.Hour),
	}

	if inverted.HasFilterableIndex(prop) {
		if dt, _ := schema.AsPrimitive(prop.DataType); dt == schema.DataTypeGeoCoordinates {
			return s.initGeoProp(prop)
		}

		if schema.IsRefDataType(prop.DataType) {
			if err := s.store.CreateOrLoadBucket(ctx,
				helpers.BucketFromPropNameMetaCountLSM(prop.Name),
				append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
			); err != nil {
				return err
			}
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketFromPropNameLSM(prop.Name),
			append(bucketOpts, lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))...,
		); err != nil {
			return err
		}
	}

	if inverted.HasSearchableIndex(prop) {
		searchableBucketOpts := append(bucketOpts,
			lsmkv.WithStrategy(lsmkv.StrategyMapCollection), lsmkv.WithPread(s.index.Config.AvoidMMap))
		if s.versioner.Version() < 2 {
			searchableBucketOpts = append(searchableBucketOpts, lsmkv.WithLegacyMapSorting())
		}

		if err := s.store.CreateOrLoadBucket(ctx,
			helpers.BucketSearchableFromPropNameLSM(prop.Name),
			searchableBucketOpts...,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) createPropertyLengthIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
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
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours)*time.Hour),
	)
}

func (s *Shard) createPropertyNullIndex(ctx context.Context, prop *models.Property) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameNullLSM(prop.Name),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours)*time.Hour),
	)
}

func (s *Shard) addIDProperty(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	err := s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropID),
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategySetCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours)*time.Hour),
	)
	if err != nil {
		return fmt.Errorf("create id property: %w", err)
	}
	return nil
}

func (s *Shard) addDimensionsProperty(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	// Note: this data would fit the "Set" type better, but since the "Map" type
	// is currently optimized better, it is more efficient to use a Map here.
	err := s.store.CreateOrLoadBucket(ctx,
		helpers.DimensionsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours)*time.Hour),
	)
	if err != nil {
		return fmt.Errorf("create dimensions tracking property: %w", err)
	}

	return nil
}

func (s *Shard) addTimestampProperties(ctx context.Context) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
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
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours)*time.Hour),
	)
}

func (s *Shard) addLastUpdateTimeUnixProperty(ctx context.Context) error {
	return s.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(filters.InternalPropLastUpdateTimeUnix),
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithSegmentsCleanupInterval(time.Duration(s.index.Config.SegmentsCleanupIntervalHours)*time.Hour),
	)
}
