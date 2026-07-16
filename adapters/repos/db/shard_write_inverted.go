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
	return s.analyzeObjectCommonWithOverlay(object, c, nil)
}

// analyzeObjectCommonWithOverlay is analyzeObjectCommon plus an optional
// migration overlay (see AnalyzeObjectForMigrationWithOverlay). Without the
// overlay, a property targeted by a from-scratch enable-* migration is
// never nil-tracked during the backfill scan: HasAnyInvertedIndex(prop)
// reads the LIVE (pre-migration) schema, which is false for the very
// property the migration is trying to populate, so a pre-existing object
// missing that property would silently produce no NilProperty entry at
// all - the property is neither in `props` (analyzer skips it, no value)
// nor in `nilProps` (this gate skips it too). That is the null-state half
// of weaviate/0-weaviate-issues#322: the sidecar backfill in
// OnAfterLsmInitAsync can only write what analyzeObjectCommon hands it.
//
// The overlay-aware check treats a property as "has an inverted index" for
// nil-tracking purposes if EITHER the live schema already says so, OR the
// migration overlay forces one of Filterable/Searchable/Rangeable on for
// it - the exact same condition the analyzer already uses downstream for
// non-nil properties (see AnalyzerOverlay implementations per strategy).
// AnalyzeObject (the normal write path) always passes a nil overlay, so
// this is unchanged for ordinary writes.
func (s *Shard) analyzeObjectCommonWithOverlay(object *storobj.Object, c *models.Class,
	overlay map[string]inverted.PropertyOverlay,
) (map[string]interface{}, []inverted.NilProperty, error) {
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

			hasIndex := inverted.HasAnyInvertedIndex(prop)
			if !hasIndex {
				if ov, ok := overlay[prop.Name]; ok && (ov.ForceFilterable || ov.ForceSearchable || ov.ForceRangeable) {
					hasIndex = true
				}
			}

			// Add props as nil props if
			// 1. They are not in the schema map ( == nil)
			// 2. Their inverted index is enabled (live schema or migration overlay)
			_, ok := schemaMap[prop.Name]
			if !ok && hasIndex {
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

	analyzer := inverted.NewAnalyzer(s.isFallbackToSearchable, object.Class().String())
	// Mirror the query-path overlay handling (BM25Searcher.effectiveTokenization)
	// so writes during a change-tokenization SWAPPING window land in the
	// canonical bucket with TARGET-tokenized keys. weaviate/0-weaviate-issues#240.
	if overlay := s.tokenizationAnalyzerOverlay(c.Properties); overlay != nil {
		analyzer = analyzer.WithSchemaOverlay(overlay)
	}
	props, nestedProps, err := analyzer.Object(schemaMap, c.Properties, object.ID())
	return props, nilProps, nestedProps, err
}

// tokenizationAnalyzerOverlay projects the per-shard tokenization
// overlay onto the inverted-analyzer PropertyOverlay shape. Only
// `Tokenization` is populated — the Force* flags are owned by
// from-scratch backfill strategies and must not affect ordinary writes.
func (s *Shard) tokenizationAnalyzerOverlay(props []*models.Property) map[string]inverted.PropertyOverlay {
	if len(props) == 0 {
		return nil
	}
	propNames := make([]string, 0, len(props))
	liveTok := make(map[string]string, len(props))
	for _, p := range props {
		if p == nil {
			continue
		}
		propNames = append(propNames, p.Name)
		liveTok[p.Name] = p.Tokenization
	}
	snap := s.SnapshotTokenizationOverlay(propNames)
	if len(snap) == 0 {
		return nil
	}
	var out map[string]inverted.PropertyOverlay
	for name, target := range snap {
		if target == liveTok[name] {
			// Live schema already matches the overlay target. The
			// authoritative clear happens via ClearTokenizationOverlay
			// at migration completion; query-path TokenizationFor
			// self-clears as a secondary nicety.
			continue
		}
		if out == nil {
			out = make(map[string]inverted.PropertyOverlay, len(snap))
		}
		out[name] = inverted.PropertyOverlay{Tokenization: target}
	}
	return out
}

// AnalyzeObjectForMigrationWithOverlay is the migration-time variant of
// [Shard.AnalyzeObject]. Unlike AnalyzeObject it captures raw
// (pre-tokenization) text values in Property.RawValues, which the
// retokenize strategies need to apply the new tokenization without
// re-fetching the source object. The overlay forces the analyzer to treat
// specific properties as if their inverted-index flag (and optionally their
// tokenization) were already updated, even though the live RAFT-stored
// schema still has them in the pre-migration state. The live schema is
// never mutated.
//
// This is required by runtime reindex strategies that build a brand-new
// inverted bucket (e.g. EnableFilterableStrategy, EnableSearchableStrategy,
// FilterableToRangeableStrategy): during the backfill scan the target
// schema flag is still false, so without the overlay the analyzer would
// either skip the very property the migration is trying to populate, or
// emit it with the wrong HasXIndex flags, producing an empty target
// bucket and a silent FINISHED-with-empty-bucket failure.
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

	schemaMap, nilProps, err := s.analyzeObjectCommonWithOverlay(object, c, overlay)
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
