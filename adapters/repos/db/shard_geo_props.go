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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

// A geoCoordinates property with a filterable index is served by a geopoints
// bucket, helpers.BucketGeoFromPropNameLSM, that holds a BKD tree per segment.
// It is an lsmkv bucket like every other property index, so flushing,
// compaction, backups and crash recovery need nothing geo-specific. Until
// v1.40 the index was an HNSW graph beside the shard; initGeoProp rebuilds
// such a graph's bucket from the objects and removes its directories.

// initGeoProp opens propName's geo bucket, building it from the objects when
// the shard still holds the geo HNSW of an earlier version.
func (s *Shard) initGeoProp(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.store.CreateOrLoadBucket(ctx, helpers.BucketGeoFromPropNameLSM(prop.Name),
		makeBucketOptions(lsmkv.StrategyGeoPoints)...); err != nil {
		return err
	}
	return s.migrateLegacyGeoIndex(ctx, prop.Name, makeBucketOptions)
}

// legacyGeoIndexDirs are the directories the geo HNSW of versions before 1.40
// kept beside the shard's lsm directory: its commit log and, with async
// indexing, its queue.
func legacyGeoIndexDirs(shardPath, propName string) []string {
	return []string{
		filepath.Join(shardPath, fmt.Sprintf("geo.%s.hnsw.commitlog.d", propName)),
		filepath.Join(shardPath, fmt.Sprintf("geo.%s.queue.d", propName)),
	}
}

// migrateLegacyGeoIndex rebuilds propName's bucket from the objects when a geo
// HNSW directory is present, then removes the directories. The directories go
// last, and only once the rebuilt bucket is on disk, so a crash at any point
// repeats the rebuild on the next start rather than leaving a partial bucket
// behind with nothing to trigger its repair.
func (s *Shard) migrateLegacyGeoIndex(ctx context.Context, propName string,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	dirs := legacyGeoIndexDirs(s.path(), propName)
	found := false
	for _, dir := range dirs {
		if _, err := os.Stat(dir); err == nil {
			found = true
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("stat legacy geo index dir %q: %w", dir, err)
		}
	}
	if !found {
		return nil
	}

	logger := s.index.logger.WithField("shard", s.name).WithField("geo", propName)
	logger.Info("legacy geo index found, rebuilding the geo bucket from the stored objects")

	if err := s.rebuildGeoBucketFromObjects(ctx, propName, makeBucketOptions); err != nil {
		return fmt.Errorf("migrate legacy geo index of prop %q: %w", propName, err)
	}

	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove legacy geo index dir %q: %w", dir, err)
		}
	}
	logger.Info("legacy geo index removed")
	return nil
}

// rebuildGeoBucketFromObjects replaces propName's geo bucket with one built
// from every stored object and flushed to a segment. The bucket is gone for
// the duration, so writes and geo filters on the property fail while it runs;
// the startup migration runs before the shard serves, the debug endpoint is
// for a node without traffic.
func (s *Shard) rebuildGeoBucketFromObjects(ctx context.Context, propName string,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	start := time.Now()
	bucketName := helpers.BucketGeoFromPropNameLSM(propName)

	// an earlier attempt may have left a partial bucket
	if err := s.removeBucket(ctx, bucketName); err != nil {
		return fmt.Errorf("remove geo bucket: %w", err)
	}
	if err := s.store.CreateOrLoadBucket(ctx, bucketName,
		makeBucketOptions(lsmkv.StrategyGeoPoints)...); err != nil {
		return fmt.Errorf("create geo bucket: %w", err)
	}
	bucket := s.store.Bucket(bucketName)
	if bucket == nil {
		return fmt.Errorf("geo bucket %q missing after creation", bucketName)
	}

	count, err := s.indexGeoPointsFromObjects(ctx, bucket, propName)
	if err != nil {
		return errors.Wrap(err, "index stored objects")
	}

	// The points go to a segment, fsynced like every flush, and the directory
	// entry naming it is synced too. A caller that then removes what the
	// rebuild replaces can rely on the bucket being on disk; the write-ahead
	// log alone is only in the page cache until the kernel writes it.
	if err := bucket.FlushAndSwitch(); err != nil {
		return fmt.Errorf("flush geo bucket: %w", err)
	}
	if err := fsyncDir(bucket.GetDir()); err != nil {
		return fmt.Errorf("fsync geo bucket dir: %w", err)
	}

	s.index.logger.
		WithField("count", count).
		WithField("took", time.Since(start)).
		WithField("shard_id", s.ID()).
		WithField("geo", propName).
		Info("rebuilt geo bucket from LSM store")
	return nil
}

// indexGeoPointsFromObjects adds the coordinates of every live object to the
// bucket and returns how many it added. It scans the objects bucket in storage
// order: the doc ID counter counts every version ever written, so walking its
// range by doc ID would cost as much as the shard's history, not its size.
func (s *Shard) indexGeoPointsFromObjects(ctx context.Context, bucket *lsmkv.Bucket,
	propName string,
) (int, error) {
	objects, release, err := s.objectsBucket()
	if err != nil {
		return 0, err
	}
	defer release()
	className, err := objects.ClassName()
	if err != nil {
		return 0, fmt.Errorf("getting bucket class name: %w", err)
	}
	// read-only once built, so every object can share it
	propExtraction := storobj.NewPropExtraction().Add(propName)

	cursor := objects.CursorReplaceReusable()
	defer cursor.Close()

	count, seen := 0, 0
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		seen++
		if seen%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return count, err
			}
		}
		obj, err := storobj.FromBinaryOptionalDisk(v, className, additional.Properties{}, propExtraction)
		if err != nil {
			return count, errors.Wrap(err, "unmarshal object")
		}
		coordinates, err := geoCoordinatesOfProp(obj, propName)
		if err != nil {
			return count, fmt.Errorf("doc %d: %w", obj.DocID, err)
		}
		if coordinates == nil {
			continue
		}
		if err := bucket.GeoPointsAdd(obj.DocID, *coordinates.Latitude, *coordinates.Longitude); err != nil {
			return count, fmt.Errorf("doc %d: %w", obj.DocID, err)
		}
		count++
	}
	return count, nil
}

// geoProps lists the class properties served by a geo bucket.
func geoProps(class *models.Class) []*models.Property {
	var out []*models.Property
	for _, prop := range class.Properties {
		if createsGeoIndex(prop) {
			out = append(out, prop)
		}
	}
	return out
}

// geoBucket returns propName's geo bucket, or an error when the shard has none:
// an object with coordinates that go unindexed would be a silent gap.
func (s *Shard) geoBucket(propName string) (*lsmkv.Bucket, error) {
	bucket := s.store.Bucket(helpers.BucketGeoFromPropNameLSM(propName))
	if bucket == nil {
		return nil, fmt.Errorf("no geo bucket for prop %q found", propName)
	}
	return bucket, nil
}

// geoCoordinatesOfProp returns propName's coordinates, or nil if the object
// carries no such property. A value arriving as a map, as a merge produces, is
// accepted alongside the model type.
func geoCoordinatesOfProp(obj *storobj.Object, propName string) (*models.GeoCoordinates, error) {
	props, ok := obj.Properties().(map[string]interface{})
	if !ok {
		return nil, nil
	}

	prop, ok := props[propName]
	if !ok || prop == nil {
		return nil, nil
	}

	var coordinates *models.GeoCoordinates
	switch val := prop.(type) {
	case *models.GeoCoordinates:
		coordinates = val
	case map[string]any:
		asBytes, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("adjust geo property type: marshal geo property map: %w", err)
		}
		var adjusted models.GeoCoordinates
		if err := json.Unmarshal(asBytes, &adjusted); err != nil {
			return nil, fmt.Errorf("adjust geo property type: unmarshal geo property map: %w", err)
		}
		coordinates = &adjusted
	default:
		return nil, fmt.Errorf("expected property %q to be of type %T, got: %T",
			propName, &models.GeoCoordinates{}, prop)
	}

	if coordinates.Latitude == nil {
		return nil, fmt.Errorf("property %q: latitude must be set", propName)
	}
	if coordinates.Longitude == nil {
		return nil, fmt.Errorf("property %q: longitude must be set", propName)
	}
	return coordinates, nil
}

// updatePropertySpecificIndices writes object's geo coordinates to every geo
// bucket of its class, after removing the doc ID the object had before. It
// removes from every geo bucket, not only those the object has a value for,
// since the update may have dropped the property. The caller holds the
// object's doc ID lock: a delete or update of the same object that ran between
// the object write and this one would remove a doc ID this add then writes.
func (s *Shard) updatePropertySpecificIndices(ctx context.Context, object *storobj.Object,
	status objectInsertStatus,
) error {
	if err := s.isReadOnly(); err != nil {
		return err
	}

	// geo props were not changed
	if status.docIDPreserved || status.skipUpsert {
		return nil
	}

	class := s.index.getSchema.ReadOnlyClass(object.Class().String())
	if class == nil {
		return fmt.Errorf("could not find class %s in schema", object.Class().String())
	}

	for _, prop := range geoProps(class) {
		bucket, err := s.geoBucket(prop.Name)
		if err != nil {
			return err
		}

		if status.docIDChanged {
			if err := bucket.GeoPointsRemove(status.oldDocID); err != nil {
				return errors.Wrapf(err, "delete old doc id from geo index of prop %q", prop.Name)
			}
		}

		coordinates, err := geoCoordinatesOfProp(object, prop.Name)
		if err != nil {
			return err
		}
		if coordinates == nil {
			continue
		}
		if err := bucket.GeoPointsAdd(status.docID, *coordinates.Latitude, *coordinates.Longitude); err != nil {
			return errors.Wrapf(err, "insert into geo index of prop %q", prop.Name)
		}
	}
	return nil
}

// deleteFromGeoIndexes removes docID from the geo bucket of every property the
// deleted object carried a coordinate for.
func (s *Shard) deleteFromGeoIndexes(previous *storobj.Object, docID uint64) error {
	class := s.index.getSchema.ReadOnlyClass(previous.Class().String())
	if class == nil {
		return fmt.Errorf("could not find class %s in schema", previous.Class().String())
	}

	for _, prop := range geoProps(class) {
		coordinates, err := geoCoordinatesOfProp(previous, prop.Name)
		if err != nil {
			return err
		}
		if coordinates == nil {
			continue
		}
		bucket, err := s.geoBucket(prop.Name)
		if err != nil {
			return err
		}
		if err := bucket.GeoPointsRemove(docID); err != nil {
			return errors.Wrapf(err, "delete from geo index of prop %q", prop.Name)
		}
	}
	return nil
}

// hasGeoIndexForProp reports whether propName's geo bucket is open on this shard.
func (s *Shard) hasGeoIndexForProp(propName string) bool {
	return s.store.Bucket(helpers.BucketGeoFromPropNameLSM(propName)) != nil
}

// createsGeoIndex reports whether createPropertyValueIndex builds a geo bucket
// for prop rather than inverted buckets.
func createsGeoIndex(prop *models.Property) bool {
	if !inverted.HasFilterableIndex(prop) {
		return false
	}
	dt, _ := schema.AsPrimitive(prop.DataType)
	return dt == schema.DataTypeGeoCoordinates
}
