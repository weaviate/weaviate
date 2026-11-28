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
	"fmt"
	"maps"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func isPropertyForLength(dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeInt, schema.DataTypeNumber, schema.DataTypeBoolean, schema.DataTypeDate:
		return false
	case schema.DataTypeObject, schema.DataTypeObjectArray:
		// Nested types have dedicated nested buckets; no flat length bucket is created.
		return false
	default:
		return true
	}
}

func (s *Shard) analyzeObjectCommon(object *storobj.Object, c *models.Class) (map[string]interface{}, []inverted.NilProperty, error) {
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

	// nilProps lists the indexed properties the object lacks, so each is recorded as null under IndexNullState
	// and, for a type with a length, as length 0 under IndexPropertyLength
	var nilProps []inverted.NilProperty
	if s.index.invertedIndexConfig.IndexNullState || s.index.invertedIndexConfig.IndexPropertyLength {
		for _, prop := range c.Properties {
			dt := schema.DataType(prop.DataType[0])
			switch dt {
			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber, schema.DataTypeBlob, schema.DataTypeBlobHash:
				// not added to the inverted index
				continue
			case schema.DataTypeObject, schema.DataTypeObjectArray:
				// nested types use dedicated nested buckets — no flat null/length buckets are created for them
				continue
			default:
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
		// The map is shared with goroutines that read it concurrently; write only a private clone.
		schemaMap = maps.Clone(schemaMap)
		schemaMap[filters.InternalPropCreationTimeUnix] = object.Object.CreationTimeUnix
		schemaMap[filters.InternalPropLastUpdateTimeUnix] = object.Object.LastUpdateTimeUnix
	}

	return schemaMap, nilProps, nil
}

func (s *Shard) AnalyzeObject(object *storobj.Object) ([]inverted.Property, []inverted.NilProperty, []inverted.NestedProperty, error) {
	return s.analyzeObjectInView(object, s.loadView())
}

func (s *Shard) analyzeObjectInView(object *storobj.Object, v *shardView,
) ([]inverted.Property, []inverted.NilProperty, []inverted.NestedProperty, error) {
	c := s.index.getSchema.ReadOnlyClass(object.Class().String())
	if c == nil {
		return nil, nil, nil, fmt.Errorf("could not find class %s in schema", object.Class().String())
	}
	schemaMap, nilProps, err := s.analyzeObjectCommon(object, c)
	if err != nil {
		return nil, nil, nil, err
	}
	props := c.Properties
	if len(v.props) > 0 {
		props = make([]*models.Property, len(c.Properties))
		for i, p := range c.Properties {
			pv, governed := v.props[p.Name]
			props[i] = viewProperty(p, pv, governed)
		}
	}
	analyzer := inverted.NewAnalyzer(v.FallbackToSearchable, object.Class().String())
	out, nestedProps, err := analyzer.Object(schemaMap, props, object.ID())
	return out, nilProps, nestedProps, err
}
