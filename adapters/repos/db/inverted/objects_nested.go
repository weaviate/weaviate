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

// Per-property index predicates — one function per index type, used both
// in the Has* helpers below and in collectNestedIndexConfig.

func isNestedFilterable(prop *models.NestedProperty) bool {
	if prop.IndexFilterable == nil {
		return true
	}
	return *prop.IndexFilterable
}

func isNestedSearchable(np *models.NestedProperty, dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		if np.IndexSearchable == nil {
			return true
		}
		return *np.IndexSearchable
	default:
		return false
	}
}

func isNestedRangeable(np *models.NestedProperty, dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeInt, schema.DataTypeIntArray,
		schema.DataTypeNumber, schema.DataTypeNumberArray,
		schema.DataTypeDate, schema.DataTypeDateArray:
		if np.IndexRangeFilters == nil {
			return false
		}
		return *np.IndexRangeFilters
	default:
		return false
	}
}

// Has* helpers — check whether any descendant of a top-level Property has a
// given index type enabled. Used to decide which buckets to create.

// HasNestedFilterableIndex returns true if any descendant NestedProperty
// of the given property has a filterable index. Used to decide whether
// the nested value bucket needs to be created.
func HasNestedFilterableIndex(prop *models.Property) bool {
	return slices.ContainsFunc(prop.NestedProperties, hasNestedFilterableRecursive)
}

func hasNestedFilterableRecursive(prop *models.NestedProperty) bool {
	return isNestedFilterable(prop) ||
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
	return isNestedSearchable(prop, dt) ||
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
	return isNestedRangeable(prop, dt) ||
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
	return isNestedFilterable(prop) ||
		isNestedSearchable(prop, dt) ||
		isNestedRangeable(prop, dt) ||
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
				filterable: isNestedFilterable(np),
				searchable: isNestedSearchable(np, dt),
				rangeable:  isNestedRangeable(np, dt),
			}
		}
		maps.Copy(configs, collectNestedIndexConfig(path, np.NestedProperties))
	}
	return configs
}

// analyzeNestedProp runs position assignment on a nested property value,
// then analyzes each leaf value into bytes suitable for indexing.
func (a *Analyzer) analyzeNestedProp(prop *models.Property, value any) (*NestedProperty, error) {
	if value == nil {
		return nil, nil
	}

	assignResult, err := nested.AssignPositions(prop, value)
	if err != nil {
		return nil, err
	}

	if len(assignResult.Values) == 0 && len(assignResult.Idx) == 0 && len(assignResult.Exists) == 0 {
		return nil, nil
	}

	indexConfigs := collectNestedIndexConfig("", prop.NestedProperties)

	var hasFilterable, hasSearchable, hasRangeable bool
	for _, cfg := range indexConfigs {
		hasFilterable = hasFilterable || cfg.filterable
		hasSearchable = hasSearchable || cfg.searchable
		hasRangeable = hasRangeable || cfg.rangeable
	}

	values := make([]NestedValue, 0, len(assignResult.Values))
	for _, pv := range assignResult.Values {
		cfg := indexConfigs[pv.Path]
		if !cfg.hasAny() {
			continue
		}
		analyzed, err := a.analyzeNestedValue(pv, cfg)
		if err != nil {
			return nil, fmt.Errorf("analyze value at %q: %w", pv.Path, err)
		}
		values = append(values, analyzed...)
	}

	idx := make([]NestedMeta, len(assignResult.Idx))
	for i, entry := range assignResult.Idx {
		idx[i] = NestedMeta{
			Path:      entry.Path,
			Index:     entry.Index,
			Positions: entry.Positions,
		}
	}

	exists := make([]NestedMeta, len(assignResult.Exists))
	for i, entry := range assignResult.Exists {
		exists[i] = NestedMeta{
			Path:      entry.Path,
			Index:     -1,
			Positions: entry.Positions,
		}
	}

	return &NestedProperty{
		Name:               prop.Name,
		Values:             values,
		Idx:                idx,
		Exists:             exists,
		HasFilterableIndex: hasFilterable,
		HasSearchableIndex: hasSearchable,
		HasRangeableIndex:  hasRangeable,
	}, nil
}

// analyzeNestedValue converts a raw positioned value into one or more
// NestedValue entries with analyzed byte representations. Text values
// may produce multiple entries (one per token).
func (a *Analyzer) analyzeNestedValue(pv nested.PositionedValue, cfg nestedIndexConfig) ([]NestedValue, error) {
	items, err := a.analyzeValue(pv.DataType, pv.Tokenization, pv.Value)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return nil, nil
	}

	out := make([]NestedValue, len(items))
	for i, item := range items {
		out[i] = NestedValue{
			Path:               pv.Path,
			Data:               item.Data,
			Positions:          pv.Positions,
			HasFilterableIndex: cfg.filterable,
			HasSearchableIndex: cfg.searchable,
			HasRangeableIndex:  cfg.rangeable,
		}
	}
	return out, nil
}
