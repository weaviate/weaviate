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

	// add nil for all properties that are not part of the object so that they can be added to the inverted index for
	// the null state (if enabled)
	var nilProps []inverted.NilProperty
	if s.index.invertedIndexConfig.IndexNullState {
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
		schemaMap[filters.InternalPropCreationTimeUnix] = object.Object.CreationTimeUnix
		schemaMap[filters.InternalPropLastUpdateTimeUnix] = object.Object.LastUpdateTimeUnix
	}

	return schemaMap, nilProps, nil
}

func (s *Shard) AnalyzeObject(object *storobj.Object) ([]inverted.Property, []inverted.NilProperty, []inverted.NestedProperty, error) {
	c := s.index.getSchema.ReadOnlyClass(object.Class().String())
	if c == nil {
		return nil, nil, nil, fmt.Errorf("could not find class %s in schema", object.Class().String())
	}

	schemaMap, nilProps, err := s.analyzeObjectCommon(object, c)
	if err != nil {
		return nil, nil, nil, err
	}

	props, nestedProps, err := inverted.NewAnalyzer(s.isFallbackToSearchable, object.Class().String()).Object(schemaMap, c.Properties, object.ID())
	return props, nilProps, nestedProps, err
}

// AnalyzeObjectForMigration is like AnalyzeObject but captures raw (pre-tokenization)
// text values in Property.RawValues. Use this only during migration reads where
// retokenize strategies need the original text.
func (s *Shard) AnalyzeObjectForMigration(object *storobj.Object) ([]inverted.Property, []inverted.NilProperty, error) {
	return s.AnalyzeObjectForMigrationWithOverlay(object, nil)
}

// AnalyzeObjectForMigrationWithOverlay is the overlay-aware variant of
// AnalyzeObjectForMigration. The overlay forces the analyzer to treat
// specific properties as if their inverted-index flag (and optionally their
// tokenization) were already updated, even though the live RAFT-stored
// schema still has them in the pre-migration state. The live schema is
// never mutated.
//
// This is required by runtime reindex strategies that build a brand-new
// inverted bucket (e.g. EnableFilterableStrategy, EnableSearchableStrategy):
// during the backfill scan the schema flag is still false, so without the
// overlay the analyzer would skip the very property the migration is
// trying to populate, producing an empty target bucket.
//
// Pass overlay == nil for migrations that do not need a schema override
// (e.g. retokenize / map→blockmax, where the source bucket already exists
// and the schema flag is already true).
func (s *Shard) AnalyzeObjectForMigrationWithOverlay(object *storobj.Object,
	overlay map[string]inverted.PropertyOverlay,
) ([]inverted.Property, []inverted.NilProperty, error) {
	c := s.index.getSchema.ReadOnlyClass(object.Class().String())
	if c == nil {
		return nil, nil, fmt.Errorf("could not find class %s in schema", object.Class().String())
	}

	schemaMap, nilProps, err := s.analyzeObjectCommon(object, c)
	if err != nil {
		return nil, nil, err
	}

	analyzer := inverted.NewAnalyzerWithRawValues(s.isFallbackToSearchable, object.Class().String())
	if len(overlay) > 0 {
		analyzer = analyzer.WithSchemaOverlay(overlay)
	}
	props, _, err := analyzer.Object(schemaMap, c.Properties, object.ID())
	return props, nilProps, err
}
