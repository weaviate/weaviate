//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/propertyspecific"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/geo"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func (s *Shard) initPerPropertyIndices() error {
	s.propertyIndices = propertyspecific.Indices{}
	sch := s.index.getSchema.GetSchemaSkipAuth()
	c := sch.FindClassByName(s.index.Config.ClassName)
	if c == nil {
		return nil
	}

	for _, prop := range c.Properties {
		if schema.DataType(prop.DataType[0]) == schema.DataTypeGeoCoordinates {
			if err := s.initGeoProp(prop); err != nil {
				return errors.Wrapf(err, "init property %s", prop.Name)
			}
		}
	}
	return nil
}

func (s *Shard) initGeoProp(prop *models.Property) error {
	idx, err := geo.NewIndex(geo.Config{
		ID:                 geoPropID(s.ID(), prop.Name),
		RootPath:           s.index.Config.RootPath,
		CoordinatesForID:   s.makeCoordinatesForID(prop.Name),
		DisablePersistence: false,
		Logger:             s.index.logger,
	})
	if err != nil {
		return errors.Wrapf(err, "create geo index for prop %q", prop.Name)
	}

	s.propertyIndices[prop.Name] = propertyspecific.Index{
		Type:     schema.DataTypeGeoCoordinates,
		GeoIndex: idx,
		Name:     prop.Name,
	}

	return nil
}

func (s *Shard) makeCoordinatesForID(propName string) geo.CoordinatesForID {
	return func(ctx context.Context, id int64) (*models.GeoCoordinates, error) {
		obj, err := s.objectByIndexID(ctx, id, true)
		if err != nil {
			return nil, errors.Wrap(err, "retrieve object")
		}

		if obj.Schema() == nil {
			return nil, storobj.NewErrNotFoundf(id,
				"object has no properties")
		}

		prop, ok := obj.Schema().(map[string]interface{})[propName]
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

func geoPropID(shardID string, propName string) string {
	return fmt.Sprintf("%s_%s", shardID, propName)
}

func (s *Shard) updatePropertySpecificIndices(object *storobj.Object,
	status objectInsertStatus) error {
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
	status objectInsertStatus) error {
	if index.Type != schema.DataTypeGeoCoordinates {
		return fmt.Errorf("unsupported per-property index type %q", index.Type)
	}

	// currently the only property-specific index we support
	return s.updateGeoIndex(propName, index, obj, status)
}

func (s *Shard) updateGeoIndex(propName string, index propertyspecific.Index,
	obj *storobj.Object, status objectInsertStatus) error {
	if status.docIDChanged {
		if err := s.deleteFromGeoIndex(index, status.oldDocID); err != nil {
			return errors.Wrap(err, "delete old doc id from geo index")
		}
	}

	return s.addToGeoIndex(propName, index, obj, status)
}

func (s *Shard) addToGeoIndex(propName string, index propertyspecific.Index,
	obj *storobj.Object, status objectInsertStatus) error {
	if obj.Schema() == nil {
		return nil
	}

	asMap := obj.Schema().(map[string]interface{})
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

	if err := index.GeoIndex.Add(int64(status.docID), asGeo); err != nil {
		return errors.Wrapf(err, "insert into geo index")
	}

	return nil
}

func (s *Shard) deleteFromGeoIndex(index propertyspecific.Index,
	docID uint64) error {
	if err := index.GeoIndex.Delete(int64(docID)); err != nil {
		return errors.Wrapf(err, "delete from geo index")
	}

	return nil
}
