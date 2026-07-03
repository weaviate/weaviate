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

package inverted

import (
	"fmt"
	"maps"
	"slices"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Has* helpers — check whether any descendant of a top-level Property has a
// given index type enabled. Used to decide which buckets to create.

// HasNestedFilterableIndex returns true if any descendant NestedProperty
// of the given property has a filterable index. Used to decide whether
// the nested value bucket needs to be created.
func HasNestedFilterableIndex(prop *models.Property) bool {
	return slices.ContainsFunc(prop.NestedProperties, hasNestedFilterableRecursive)
}

func hasNestedFilterableRecursive(prop *models.NestedProperty) bool {
	return schema.IsNestedFilterable(prop) ||
		slices.ContainsFunc(prop.NestedProperties, hasNestedFilterableRecursive)
}

// HasNestedSearchableIndex returns true if any descendant NestedProperty
// of the given property has a searchable index. Used to decide whether
// the nested searchable bucket needs to be created.
func HasNestedSearchableIndex(prop *models.Property) bool {
	return slices.ContainsFunc(prop.NestedProperties, hasNestedSearchableRecursive)
}

func hasNestedSearchableRecursive(prop *models.NestedProperty) bool {
	dt := schema.DataType(prop.DataType[0])
	return schema.IsNestedSearchable(prop, dt) ||
		slices.ContainsFunc(prop.NestedProperties, hasNestedSearchableRecursive)
}

// HasNestedRangeableIndex returns true if any descendant NestedProperty
// of the given property has a rangeable index. Used to decide whether
// the nested rangeable bucket needs to be created.
func HasNestedRangeableIndex(prop *models.Property) bool {
	return slices.ContainsFunc(prop.NestedProperties, hasNestedRangeableRecursive)
}

func hasNestedRangeableRecursive(prop *models.NestedProperty) bool {
	dt := schema.DataType(prop.DataType[0])
	return schema.IsNestedRangeable(prop, dt) ||
		slices.ContainsFunc(prop.NestedProperties, hasNestedRangeableRecursive)
}

// HasAnyNestedInvertedIndex returns true if any descendant NestedProperty
// of the given property has any inverted index enabled. Used to decide
// whether the nested metadata bucket needs to be created.
func HasAnyNestedInvertedIndex(prop *models.Property) bool {
	return slices.ContainsFunc(prop.NestedProperties, hasAnyNestedInvertedIndexRecursive)
}

func hasAnyNestedInvertedIndexRecursive(prop *models.NestedProperty) bool {
	dt := schema.DataType(prop.DataType[0])
	return schema.IsNestedFilterable(prop) ||
		schema.IsNestedSearchable(prop, dt) ||
		schema.IsNestedRangeable(prop, dt) ||
		slices.ContainsFunc(prop.NestedProperties, hasAnyNestedInvertedIndexRecursive)
}

// nestedIndexConfig holds which index types are enabled for a single leaf path.
// collectNestedIndexConfig produces one entry per primitive leaf in the schema tree.
type nestedIndexConfig struct {
	filterable bool
	searchable bool
	rangeable  bool
}

func (c nestedIndexConfig) hasAny() bool {
	return c.filterable || c.searchable || c.rangeable
}

// collectNestedIndexConfig walks the NestedProperty tree and returns index
// configuration for each dot-notation path.
func collectNestedIndexConfig(prefix string, props []*models.NestedProperty) map[string]nestedIndexConfig {
	configs := map[string]nestedIndexConfig{}
	for _, np := range props {
		path := np.Name
		if prefix != "" {
			path = prefix + "." + np.Name
		}

		dt := schema.DataType(np.DataType[0])
		if !schema.IsNested(dt) {
			configs[path] = nestedIndexConfig{
				filterable: schema.IsNestedFilterable(np),
				searchable: schema.IsNestedSearchable(np, dt),
				rangeable:  schema.IsNestedRangeable(np, dt),
			}
		}
		maps.Copy(configs, collectNestedIndexConfig(path, np.NestedProperties))
	}
	return configs
}

// analyzeNestedProp is the v2 counterpart to analyzeNestedProp. It calls
// nested.AssignPositionsFromSchema (which requires a pre-built LevelSchema)
// and builds a NestedProperty whose Values hold PosRange references into the
// posArena rather than materialised []uint64 positions.
//
// Gating is identical to the v1 path:
//   - Values: gated at analyze time; entries with !cfg.hasAny() are dropped.
//   - Idx: ungated; all Result.Idx entries are read by the bucket-builder.
//   - Exists/Anchors: gated at build time via the iterator methods on
//     NestedProperty; leaf paths with no index are skipped; root ("") and
//     intermediate array paths are always kept.
//
// Returns (nil, nil) for nil value, no-index schema, or an empty assign result
// (e.g. object[] with an empty array). Callers skip bucket-writes for nil.
func (a *Analyzer) analyzeNestedProp(
	ls nested.LevelSchema,
	prop *models.Property,
	value any,
) (*NestedProperty, error) {
	if value == nil {
		return nil, nil
	}

	if !HasAnyNestedInvertedIndex(prop) {
		return nil, nil
	}

	result, err := nested.AssignPositionsFromSchema(ls, prop, value)
	if err != nil {
		return nil, err
	}

	// Only reachable for object[] with an empty array: AssignPositionsFromSchema
	// returns early and leaves all slices empty. object always wraps the value in
	// a 1-element slice, so this guard cannot fire for non-array types.
	if len(result.Values) == 0 && len(result.Idx) == 0 &&
		len(result.Exists) == 0 && len(result.Anchors) == 0 {
		return nil, nil
	}

	configs := collectNestedIndexConfig("", prop.NestedProperties)

	var hasFilterable, hasSearchable, hasRangeable bool
	for _, cfg := range configs {
		hasFilterable = hasFilterable || cfg.filterable
		hasSearchable = hasSearchable || cfg.searchable
		hasRangeable = hasRangeable || cfg.rangeable
	}

	var numFilterable int
	values := make([]NestedValue, 0, len(result.Values))
	for _, ve := range result.Values {
		cfg := configs[ve.Path]
		if !cfg.hasAny() {
			continue
		}
		analyzed, err := a.analyzeNestedValue(ve, cfg)
		if err != nil {
			return nil, fmt.Errorf("analyze value at %q: %w", ve.Path, err)
		}
		if cfg.filterable {
			numFilterable += len(analyzed)
		}
		values = append(values, analyzed...)
	}

	return &NestedProperty{
		Name:               prop.Name,
		result:             result,
		values:             values,
		configs:            configs,
		numFilterable:      numFilterable,
		HasFilterableIndex: hasFilterable,
		HasSearchableIndex: hasSearchable,
		HasRangeableIndex:  hasRangeable,
	}, nil
}

// analyzeNestedValue converts a raw nested.ValueEntry into one or more
// NestedValue entries with analyzed byte representations. Text values
// may produce multiple entries (one per token); all entries carry the same Pos
// — a 2×uint32 copy per entry, semantically identical to the v1 path's alias.
func (a *Analyzer) analyzeNestedValue(ve nested.ValueEntry, cfg nestedIndexConfig) ([]NestedValue, error) {
	// TODO aliszka:nested_filtering verify whether ve.PropName (leaf nested
	// property name, e.g. "city") is the correct identifier for the text analyzer
	// lookup, or whether the top-level property name should be used instead.
	items, err := a.analyzeValue(ve.DataType, ve.Tokenization, ve.PropName, ve.TextAnalyzer, ve.Value)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return nil, nil
	}

	out := make([]NestedValue, len(items))
	for i, item := range items {
		out[i] = NestedValue{
			Path:               ve.Path,
			Data:               item.Data,
			Pos:                ve.Pos,
			HasFilterableIndex: cfg.filterable,
			HasSearchableIndex: cfg.searchable,
			HasRangeableIndex:  cfg.rangeable,
		}
	}
	return out, nil
}
