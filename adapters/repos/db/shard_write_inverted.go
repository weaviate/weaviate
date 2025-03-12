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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func isPropertyForLength(dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return false
	default:
		return true
	}
}

func (s *Shard) AnalyzeObject(object *storobj.Object) ([]inverted.Property, []inverted.NilProperty, error) {
	c := s.index.getSchema.ReadOnlyClass(object.Class().String())
	if c == nil {
		return nil, nil, fmt.Errorf("could not find class %s in schema", object.Class().String())
	}

	var schemaMap map[string]interface{}

	if object.Properties() == nil {
		schemaMap = make(map[string]interface{})
	} else {
		maybeSchemaMap, ok := object.Properties().(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("expected schema to be map, but got %T", object.Properties())
		}
		schemaMap = maybeSchemaMap
	}

	// add nil for all properties that are not part of the object so that they can be added to the inverted index for
	// the null state (if enabled)
	var nilProps []inverted.NilProperty
	if s.index.invertedIndexConfig.IndexNullState {
		for _, prop := range c.Properties {
			dt := schema.DataType(prop.DataType[0])
			// some datatypes are not added to the inverted index, so we can skip them here
			if dt == schema.DataTypeGeoCoordinates || dt == schema.DataTypePhoneNumber || dt == schema.DataTypeBlob {
				continue
			}

			// Add props as nil props if
			// 1. They are not in the schema map ( == nil)
			// 2. Their inverted index is enabled
			_, ok := schemaMap[prop.Name]
			if !ok && inverted.HasAnyInvertedIndex(prop) {
				nilProps = append(nilProps, inverted.NilProperty{
					Name:                prop.Name,
					AddToPropertyLength: isPropertyForLength(dt),
				})
			}
		}
	}

	if s.index.invertedIndexConfig.IndexTimestamps {
		if schemaMap == nil {
			schemaMap = make(map[string]interface{})
		}
		schemaMap[filters.InternalPropCreationTimeUnix] = object.Object.CreationTimeUnix
		schemaMap[filters.InternalPropLastUpdateTimeUnix] = object.Object.LastUpdateTimeUnix
	}

	props, err := inverted.NewAnalyzer(s.isFallbackToSearchable).Object(schemaMap, c.Properties, object.ID())
	return props, nilProps, err
}
