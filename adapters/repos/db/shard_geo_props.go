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

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/geo"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) initGeoProp(prop *models.Property) error {
	// starts geo props cycles if actual geo property is present
	// (safe to start multiple times)
	s.index.cycleCallbacks.geoPropsCommitLoggerCycle.Start()
	s.index.cycleCallbacks.geoPropsTombstoneCleanupCycle.Start()

	idx, err := geo.NewIndex(geo.Config{
		ID:                 geoPropID(prop.Name),
		RootPath:           s.path(),
		CoordinatesForID:   s.makeCoordinatesForID(prop.Name),
		DisablePersistence: false,
		Logger:             s.index.logger,
	},
		s.cycleCallbacks.geoPropsCommitLoggerCallbacks,
		s.cycleCallbacks.geoPropsTombstoneCleanupCallbacks,
		s.cycleCallbacks.compactionCallbacks,
		s.cycleCallbacks.flushCallbacks,
	)
	if err != nil {
		return errors.Wrapf(err, "create geo index for prop %q", prop.Name)
	}

	s.propertyIndicesLock.Lock()
	s.propertyIndices[prop.Name] = propertyspecific.Index{
		Type:     schema.DataTypeGeoCoordinates,
		GeoIndex: idx,
		Name:     prop.Name,
	}
	s.propertyIndicesLock.Unlock()

	idx.PostStartup()

	return nil
}

func (s *Shard) makeCoordinatesForID(propName string) geo.CoordinatesForID {
	return func(ctx context.Context, id uint64) (*models.GeoCoordinates, error) {
		obj, err := s.objectByIndexID(ctx, id, true)
		if err != nil {
			return nil, storobj.NewErrNotFoundf(id, "retrieve object")
		}

		if obj.Properties() == nil {
			return nil, storobj.NewErrNotFoundf(id,
				"object has no properties")
		}

		prop, ok := obj.Properties().(map[string]interface{})[propName]
		if !ok {
			return nil, storobj.NewErrNotFoundf(id,
				"object has no property %q", propName)
		}

		geoProp, ok := prop.(*models.GeoCoordinates)
		if !ok {
			return nil, fmt.Errorf("expected property to be of type %T, got: %T",
				&models.GeoCoordinates{}, prop)
		}

		return geoProp, nil
	}
}

func geoPropID(propName string) string {
	return fmt.Sprintf("geo.%s", propName)
}

func (s *Shard) updatePropertySpecificIndices(object *storobj.Object,
	status objectInsertStatus,
) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}
	s.propertyIndicesLock.RLock()
	defer s.propertyIndicesLock.RUnlock()

	for propName, propIndex := range s.propertyIndices {
		if err := s.updatePropertySpecificIndex(propName, propIndex,
			object, status); err != nil {
			return errors.Wrapf(err, "property %q", propName)
		}
	}

	return nil
}

func (s *Shard) updatePropertySpecificIndex(propName string,
	index propertyspecific.Index, obj *storobj.Object,
	status objectInsertStatus,
) error {
	if index.Type != schema.DataTypeGeoCoordinates {
		return fmt.Errorf("unsupported per-property index type %q", index.Type)
	}

	// currently the only property-specific index we support
	return s.updateGeoIndex(propName, index, obj, status)
}

func (s *Shard) updateGeoIndex(propName string, index propertyspecific.Index,
	obj *storobj.Object, status objectInsertStatus,
) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if status.docIDChanged {
		if err := s.deleteFromGeoIndex(index, status.oldDocID); err != nil {
			return errors.Wrap(err, "delete old doc id from geo index")
		}
	}

	return s.addToGeoIndex(propName, index, obj, status)
}

func (s *Shard) addToGeoIndex(propName string, index propertyspecific.Index,
	obj *storobj.Object, status objectInsertStatus,
) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if obj.Properties() == nil {
		return nil
	}

	asMap := obj.Properties().(map[string]interface{})
	propValue, ok := asMap[propName]
	if !ok {
		return nil
	}

	// geo coordinates is the only supported one at the moment
	asGeo, ok := propValue.(*models.GeoCoordinates)
	if !ok {
		return fmt.Errorf("expected prop to be of type %T, but got: %T",
			&models.GeoCoordinates{}, propValue)
	}

	if err := index.GeoIndex.Add(status.docID, asGeo); err != nil {
		return errors.Wrapf(err, "insert into geo index")
	}

	return nil
}

func (s *Shard) deleteFromGeoIndex(index propertyspecific.Index,
	docID uint64,
) error {
	if s.isReadOnly() {
		return storagestate.ErrStatusReadOnly
	}

	if err := index.GeoIndex.Delete(docID); err != nil {
		return errors.Wrapf(err, "delete from geo index")
	}

	return nil
}
