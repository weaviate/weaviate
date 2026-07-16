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

	analyzer := inverted.NewAnalyzer(s.isFallbackToSearchable, object.Class().String())
	if overlay := s.writeAnalyzerOverlay(c.Properties); overlay != nil {
		analyzer = analyzer.WithSchemaOverlay(overlay)
	}
	props, nestedProps, err := analyzer.Object(schemaMap, c.Properties, object.ID())
	return props, nilProps, nestedProps, err
}

// writeAnalyzerOverlay merges the per-shard write-path overlays:
//   - tokenization overlay: SWAPPING-window writes use TARGET-tokenized
//     keys so they land in the canonical bucket (weaviate/0-weaviate-issues#240).
//   - rangeable force overlay: post-swap pre-flip writes to an
//     enable-rangeable/repair-rangeable property are analyzed with
//     IndexRangeFilters forced on, closing the write-loss window GH
//     weaviate/weaviate#12189's deferred flip opened
//     (weaviate/0-weaviate-issues#319, rangeable instance; see
//     [Shard.rangeableForceIndexOverlay]).
//
// The two never target the same property in practice (tokenization only
// ever carries text tokenization targets; rangeable only ever targets
// int/number/date properties), but the merge is a plain union regardless,
// tokenization taking precedence on the pathological case where both
// hooks somehow name the same property (its ForceRangeable would still
// be lost, but that combination cannot occur: CheckConflict rejects two
// migrations racing the same property's flag concurrently).
func (s *Shard) writeAnalyzerOverlay(props []*models.Property) map[string]inverted.PropertyOverlay {
	overlay := s.tokenizationAnalyzerOverlay(props)
	forced := s.rangeableForceIndexOverlay(props)
	if len(forced) == 0 {
		return overlay
	}
	if overlay == nil {
		return forced
	}
	for name, f := range forced {
		if _, exists := overlay[name]; !exists {
			overlay[name] = f
		}
	}
	return overlay
}

// rangeableForceIndexOverlay closes the post-swap pre-flip write-loss
// window that GH weaviate/weaviate#12189's deferred schema flip opened for
// enable-rangeable/repair-rangeable (weaviate/0-weaviate-issues#319,
// rangeable instance).
//
// Mechanism: once this shard's rangeable bucket is locally ready
// ([Shard.IsRangeableLocallyReady]) but the cluster-wide schema flag
// (IndexRangeFilters) hasn't flipped yet, a live write to the property is
// analyzed under the pre-flip schema (HasRangeableIndex=false) and never
// reaches the already-canonical rangeable bucket - the double-write
// callbacks that covered writes during the active migration are torn down
// per shard at swap, and the schema-flag path doesn't cover them either
// until the cluster-wide flip lands. This overlay forces the property to
// analyze as rangeable in that window, same as the ordinary inline write
// path once the flag is genuinely true.
//
// Symmetric to the read-side gate (IsRangeableLocallyReady, wired into
// query planning in shard_read.go / shard_aggregate.go /
// shard_write_batch_delete.go): reads fall back to filterable until
// local-ready; writes force the target flag on once local-ready, until the
// schema catches up. No new per-shard state is needed - local-ready
// already tracks exactly this window (set true by
// FilterableToRangeableStrategy.OnMigrationComplete on this shard's swap,
// reset false by markInFlightRangeableMigrationsNotReady on restart
// mid-migration).
//
// Self-limiting:
//   - Stops firing the instant the cluster flag flips: HasRangeableIndex(p)
//     becomes true and the loop below skips the property.
//   - Never fires on a not-yet-swapped replica: IsRangeableLocallyReady is
//     false there, so that replica's own double-write callbacks / backfill
//     scan (not this overlay) are the active write coverage.
//   - Never fires for repair-rangeable: its flag is already true before
//     the migration starts (submit-time validator), so HasRangeableIndex
//     is already true and the loop skips it on every call.
//   - Never fires for a property that was never migrated: its rangeable
//     bucket doesn't exist, so IsRangeableLocallyReady's bucket-existence
//     fallback returns false.
func (s *Shard) rangeableForceIndexOverlay(props []*models.Property) map[string]inverted.PropertyOverlay {
	var out map[string]inverted.PropertyOverlay
	for _, p := range props {
		if p == nil || inverted.HasRangeableIndex(p) {
			continue
		}
		if !s.IsRangeableLocallyReady(p.Name) {
			continue
		}
		if out == nil {
			out = make(map[string]inverted.PropertyOverlay, len(props))
		}
		out[p.Name] = inverted.PropertyOverlay{ForceRangeable: true}
	}
	return out
}

// tokenizationAnalyzerOverlay projects the per-shard tokenization
// overlay onto the inverted-analyzer PropertyOverlay shape. Only
// `Tokenization` is populated; Force* flags come from the separate
// rangeable force overlay, merged in by [Shard.writeAnalyzerOverlay].
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
