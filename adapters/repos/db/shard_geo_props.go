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
// bucket (docs/geo-index.md). Until v1.40 it was an HNSW graph beside the
// shard; initGeoProp rebuilds such a graph's bucket from the objects.
func (s *Shard) initGeoProp(ctx context.Context, prop *models.Property,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	if err := s.store.CreateOrLoadBucket(ctx, helpers.BucketGeoFromPropNameLSM(prop.Name),
		makeBucketOptions(lsmkv.StrategyGeoPoints)...); err != nil {
		return err
	}
	return s.migrateLegacyGeoIndex(ctx, prop.Name, makeBucketOptions)
}

// legacyGeoIndexDirs: the geo HNSW's commit log, the snapshot directory of
// versions before the two were unified, and, with async indexing, its queue.
func legacyGeoIndexDirs(shardPath, propName string) []string {
	return []string{
		filepath.Join(shardPath, fmt.Sprintf("geo.%s.hnsw.commitlog.d", propName)),
		filepath.Join(shardPath, fmt.Sprintf("geo.%s.hnsw.snapshot.d", propName)),
		filepath.Join(shardPath, fmt.Sprintf("geo.%s.queue.d", propName)),
	}
}

// migrateLegacyGeoIndex rebuilds propName's bucket from the objects when a geo
// HNSW directory is present, then removes the directories. They go last, once
// the bucket is on disk, so a crash at any point repeats the rebuild.
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
// from the stored objects and flushed to a segment. The bucket is gone for the
// duration, so writes and geo filters on the property fail while it runs.
func (s *Shard) rebuildGeoBucketFromObjects(ctx context.Context, propName string,
	makeBucketOptions lsmkv.MakeBucketOptions,
) error {
	start := time.Now()
	bucketName := helpers.BucketGeoFromPropNameLSM(propName)

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

	// on disk before the caller removes what the rebuild replaces: a written
	// but unsynced WAL is only in the page cache, and the bucket directory
	// itself was just recreated, so its entry in the parent needs syncing too
	if err := bucket.FlushAndSwitch(); err != nil {
		return fmt.Errorf("flush geo bucket: %w", err)
	}
	for _, dir := range []string{bucket.GetDir(), filepath.Dir(bucket.GetDir())} {
		if err := fsyncDir(dir); err != nil {
			return fmt.Errorf("fsync %q: %w", dir, err)
		}
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
// bucket, scanning the objects bucket rather than probing every doc ID ever
// written, and returns how many it added.
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

func geoProps(class *models.Class) []*models.Property {
	var out []*models.Property
	for _, prop := range class.Properties {
		if createsGeoIndex(prop) {
			out = append(out, prop)
		}
	}
	return out
}

// geoBucket errors when the shard has none: an unindexed coordinate would be a
// silent gap.
func (s *Shard) geoBucket(propName string) (*lsmkv.Bucket, error) {
	bucket := s.store.Bucket(helpers.BucketGeoFromPropNameLSM(propName))
	if bucket == nil {
		return nil, fmt.Errorf("no geo bucket for prop %q found", propName)
	}
	return bucket, nil
}

// geoCoordinatesOfProp returns propName's coordinates, or nil if the object has
// none. A merge produces the value as a map rather than the model type.
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

// updatePropertySpecificIndices writes the object's coordinates to every geo
// bucket of its class, after removing its previous doc ID from each (the
// update may have dropped the property). The caller holds the doc ID lock, so
// a delete of the same object cannot retire the doc ID this adds in between.
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

func (s *Shard) hasGeoIndexForProp(propName string) bool {
	return s.store.Bucket(helpers.BucketGeoFromPropNameLSM(propName)) != nil
}

// createsGeoIndex: prop gets a geo bucket rather than inverted buckets.
func createsGeoIndex(prop *models.Property) bool {
	if !inverted.HasFilterableIndex(prop) {
		return false
	}
	dt, _ := schema.AsPrimitive(prop.DataType)
	return dt == schema.DataTypeGeoCoordinates
}
