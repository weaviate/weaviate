package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	usagetypes "github.com/weaviate/weaviate/cluster/usage/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

var dimensionTrackingVersion = "v2"

func DimensionsBucketLSM() string {
	if dimensionTrackingVersion == "v2" {
		return helpers.DimensionsBucketLSM_v2
	}
	return helpers.DimensionsBucketLSM_v1
}

func shardPathDimensionsLSM(indexPath, shardName string) string {
	return path.Join(shardPathLSM(indexPath, shardName), DimensionsBucketLSM())
}

func (s *Shard) addDimensionsProperty(ctx context.Context) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	// Note: this data would fit the "Set" type better, but since the "Map" type
	// is currently optimized better, it is more efficient to use a Map here.
	err := s.createDimensionsBucket(ctx, DimensionsBucketLSM())
	if err != nil {
		return fmt.Errorf("create dimensions tracking property: %w", err)
	}

	return nil
}



// Empty the dimensions bucket, quickly and efficiently
func (s *Shard) resetDimensionsLSM(ctx context.Context) (time.Time, error) {
	s.dimensionTrackingLock.Lock()
	defer s.dimensionTrackingLock.Unlock()
	// Load the current one, or an empty one if it doesn't exist
	err := s.store.CreateOrLoadBucket(ctx,
		DimensionsBucketLSM(),
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithMinMMapSize(s.index.Config.MinMMapSize),
		lsmkv.WithMinWalThreshold(s.index.Config.MaxReuseWalSize),
		lsmkv.WithWriteSegmentInfoIntoFileName(s.index.Config.SegmentInfoIntoFileNameEnabled),
		lsmkv.WithWriteMetadata(s.index.Config.WriteMetadataFilesEnabled),
		s.segmentCleanupConfig(),
	)
	if err != nil {
		s.index.logger.WithError(err).WithField("shard", s.Name()).Error("resetDimensionsLSM: failed to create or load dimensions bucket")
	}

	// Fetch the actual bucket
	b := s.store.Bucket(DimensionsBucketLSM())
	if b == nil {
		s.index.logger.WithField("shard", s.Name()).Error("resetDimensionsLSM: no bucket dimensions")
		return time.Now(), errors.Errorf("resetDimensionsLSM: no bucket dimensions")
	}

	// Clear the bucket
	cursor := b.Cursor()
	defer cursor.Close()
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		b.Delete(k)
	}

	return time.Now(), nil
}

func (s *Shard) addToDimensionBucket(
	dimLength int, docID uint64, vecName string, tombstone bool,
) error {
	if dimensionTrackingVersion == "v2" {
		return s.addToDimensionBucket_v2(dimLength, docID, vecName, tombstone)
	}
	return s.addToDimensionBucket_v1(dimLength, docID, vecName, tombstone)
}

func (s *Shard) addToDimensionBucket_v2(
	dimLength int, docID uint64, vecName string, tombstone bool,
) error {
	s.dimensionTrackingLock.Lock()
	defer s.dimensionTrackingLock.Unlock()
	err := s.addDimensionsProperty(context.Background())
	if err != nil {
		return errors.Wrap(err, "add dimensions property")
	}
	b := s.store.Bucket(DimensionsBucketLSM())
	if b == nil {
		return errors.Errorf("add dimension bucket: no bucket dimensions")
	}

	// Find the key, which is the target vector name and dimensionality, and the number of times it appears
	keybuff := make([]byte, 4+len(vecName))
	copy(keybuff[4:], vecName)
	binary.LittleEndian.PutUint32(keybuff[:4], uint32(dimLength))
	countbuff_r, err := b.Get(keybuff)
	if err != nil {
		return err
	}
	countbuff := make([]byte, len(countbuff_r))
	if countbuff_r == nil {
		// if the bucket is empty, initialize the count to 0
		countbuff = make([]byte, 8)
		binary.LittleEndian.PutUint64(countbuff, 0)
	} else {
		copy(countbuff, countbuff_r)
	}
	count := binary.LittleEndian.Uint64(countbuff)

	// Update the count based on whether it's being created or deleted
	if tombstone {
		if count > 0 {
			count = count - 1
		}
	} else {
		count = count + 1
	}

	binary.LittleEndian.PutUint64(countbuff, count)
	if err := b.Put(keybuff, countbuff); err != nil {
		return errors.Wrapf(err, "add dimension bucket: set key %s", string(countbuff))
	}

	var objCount_byte []byte
	// Update the object count in the dimensions bucket
	objCount_byte, _ = b.Get([]byte("cnt")) // If it doesn't exist, it will be created

	if len(objCount_byte) != 8 {
		objCount_byte = make([]byte, 8)
		binary.LittleEndian.PutUint64(objCount_byte, 0) // Initialize to 0 if not found
	}

	objCount := binary.LittleEndian.Uint64(objCount_byte)

	if tombstone {
		objCount = objCount - 1
	} else {
		objCount = objCount + 1
	}
	countBytesOut := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytesOut, objCount)

	if err := b.Put([]byte("cnt"), countBytesOut); err != nil {
		return fmt.Errorf("failed to put object count in dimensions bucket: %w", err)
	}
	return nil
}

// calcTargetVectorDimensionsFromStore calculates dimensions and object count for a target vector from an LSMKV store
func calcTargetVectorDimensionsFromStore(ctx context.Context, store *lsmkv.Store, targetVector string, calcEntry func(dimLen int, v int) (int, int)) usagetypes.Dimensionality {
	b := store.Bucket(DimensionsBucketLSM())
	if b == nil {
		return usagetypes.Dimensionality{}
	}
	return calcTargetVectorDimensionsFromBucket(ctx, b, targetVector, calcEntry)
}


func (t *ShardInvertedReindexTask_SpecifiedIndex) GetPropertiesToReindex(ctx context.Context,
	shard ShardLike,
) ([]ReindexableProperty, error) {
	reindexableProperties := []ReindexableProperty{}

	// shard of selected class
	props, ok := t.classNamesWithPropertyNames[shard.Index().Config.ClassName.String()]
	if !ok {
		return reindexableProperties, nil
	}

	bucketOptions := []lsmkv.BucketOption{
		lsmkv.WithDirtyThreshold(time.Duration(shard.Index().Config.MemtablesFlushDirtyAfter) * time.Second),
	}

	for name := range shard.Store().GetBucketsByName() {
		// skip non prop buckets
		switch name {
		case helpers.ObjectsBucketLSM:
		case helpers.VectorsBucketLSM:
		case helpers.VectorsCompressedBucketLSM:
			case helpers.DimensionsBucketLSM_v1:
		case helpers.DimensionsBucketLSM_v2:
			continue
		}

		propName, indexType := GetPropNameAndIndexTypeFromBucketName(name)
		if _, ok := props[propName]; !ok {
			continue
		}

		switch indexType {
		case IndexTypePropValue:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropValue,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropSearchableValue:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropSearchableValue,
					DesiredStrategy: lsmkv.StrategyMapCollection,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropLength:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropLength,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropNull:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropNull,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		case IndexTypePropMetaCount:
			reindexableProperties = append(reindexableProperties,
				ReindexableProperty{
					PropertyName:    propName,
					IndexType:       IndexTypePropMetaCount,
					DesiredStrategy: lsmkv.StrategyRoaringSet,
					BucketOptions:   bucketOptions,
				},
			)
		default:
			// skip remaining
		}

	}

	return reindexableProperties, nil
}


func (m *Migrator) RecalculateVectorDimensions(ctx context.Context, reindexVectorDimensionsAtStartup bool, trackVectorDimensions bool) error {
	if !trackVectorDimensions {
		return nil
	}
	m.logger.
		WithField("action", "reindex").
		Info("Reindexing dimensions, this may take a while")

	indices := make(map[string]*Index)
	func() {
		m.db.indexLock.Lock()
		defer m.db.indexLock.Unlock()
		indices_orig := m.db.indices
		for k, v := range indices_orig {
			indices[k] = v
		}
	}()

	m.logger.WithField("action", "reindex").Infof("Found %v indexes to reindex", len(indices))

	var classes []*models.Class
	objects := m.db.schemaGetter.GetSchemaSkipAuth().Objects
	if objects != nil {
		classes = objects.Classes
	}
	m.logger.WithField("action", "reindex").Infof("Found %v classes to reindex", len(classes))

	// Iterate over all indexes
	for _, index := range m.db.indices {

		m.logger.WithField("action", "reindex").Infof("reindexing dimensions for index %q", index.ID())
		shards := index.ShardState().AllLocalPhysicalShards()
		m.logger.WithField("action", "reindex").Infof("Found %v shards to reindex", len(shards))
		err := index.ForEachPhysicalShard(func(name string, shard ShardLike) error {
			// Iterate over all shards
			if !m.detectDimensionBucketv2(shard) || reindexVectorDimensionsAtStartup {
				m.logger.WithField("action", "reindex").Infof("resetting vector dimensions for shard %q", name)
				rtime, err := shard.resetDimensionsLSM(ctx)
				if err != nil {
					m.logger.WithField("action", "reindex").WithError(err).Warn("could not reset vector dimensions")
					// If we cannot reset the dimensions, we skip this shard
					return nil
				}

				resetTime := rtime.UnixNano() / int64(time.Millisecond) //

				err = func() error {
					m.logger.WithField("action", "reindex").Infof("reindexing objects for shard %q", name)
					return shard.IterateObjects(ctx, func(index *Index, shard ShardLike, object *storobj.Object) error {
						if object.Object.LastUpdateTimeUnix > resetTime {
							// Skip objects that were updated after the reset time, they will be handled elsewhere
							m.logger.WithField("action", "reindex").Infof("skipping object %v with last update time %d after reset time %d",
								object.DocID, object.Object.LastUpdateTimeUnix, resetTime)
							// Continue with the next object, but log the skip
							return nil
						}

						b := shard.Store().Bucket(DimensionsBucketLSM())
						if b == nil {
							return fmt.Errorf("dimensions bucket %q not found for shard %q", DimensionsBucketLSM(), shard.Name())
						}
						var objCount_byte []byte
						// Update the object count in the dimensions bucket
						objCount_byte, _ = b.Get([]byte("cnt")) // If it doesn't exist, it will be created

						if len(objCount_byte) != 8 {
							objCount_byte = make([]byte, 8)
							binary.LittleEndian.PutUint64(objCount_byte, 0) // Initialize to 0 if not found
						}

						objCount := binary.LittleEndian.Uint64(objCount_byte)

						objCount = objCount + 1

						countBytesOut := make([]byte, 8)
						binary.LittleEndian.PutUint64(countBytesOut, objCount)

						if err := b.Put([]byte("cnt"), countBytesOut); err != nil {
							return fmt.Errorf("failed to put object count in dimensions bucket: %w", err)
						}
						return object.IterateThroughVectorDimensions(func(targetVector string, dims int) error {
							if err := shard.extendDimensionTrackerLSM(dims, object.DocID, targetVector); err != nil {
								m.logger.WithField("action", "reindex").WithError(err).Warnf("could not extend vector dimensions for vector %q", targetVector)
								// Continue with the next vector, but log the error
								return nil
							}

							return nil
						})
					})
				}()
				return err
			}

			return nil
		})
		if err != nil {
			m.logger.WithField("action", "reindex").WithError(err).Warn("could not extend vector dimensions")
			return err
		}
	}
	f := func() {
		for {
			m.logger.
				WithField("action", "reindex").
				Warnf("Reindexed objects. Reindexing dimensions complete. Please remove environment variable REINDEX_VECTOR_DIMENSIONS_AT_STARTUP before next startup")
			time.Sleep(5 * time.Minute)
		}
	}
	enterrors.GoWrapper(f, m.logger)
	return nil
}

func (m *Migrator) detectDimensionBucketv2(s ShardLike) bool {
	metrics := calculateShardDimensionMetrics(context.TODO(), s)
	if metrics.Compressed+metrics.Uncompressed == 0 {
		m.logger.WithField("action", "reindex").Infof("no dimensions found for shard %q", s.Name())
		return false
	}
	return true
}