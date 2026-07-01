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

	nested2 "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested2"
	"github.com/weaviate/weaviate/entities/models"
)

// analyzeNestedProp2 is the v2 counterpart to analyzeNestedProp. It calls
// nested2.AssignPositionsFromSchema (which requires a pre-built LevelSchema)
// and builds a NestedProperty2 whose Values hold PosRange references into the
// posArena rather than materialised []uint64 positions.
//
// Gating is identical to the v1 path:
//   - Values: gated at analyze time; entries with !cfg.hasAny() are dropped.
//   - Idx: ungated; all Result.Idx entries are read by the bucket-builder.
//   - Exists/Anchors: gated at build time via the iterator methods on
//     NestedProperty2; leaf paths with no index are skipped; root ("") and
//     intermediate array paths are always kept.
//
// Returns (nil, nil) for nil value, no-index schema, or an empty assign result
// (e.g. object[] with an empty array). Callers skip bucket-writes for nil.
func (a *Analyzer) analyzeNestedProp2(
	ls nested2.LevelSchema,
	prop *models.Property,
	value any,
) (*NestedProperty2, error) {
	if value == nil {
		return nil, nil
	}

	if !HasAnyNestedInvertedIndex(prop) {
		return nil, nil
	}

	result, err := nested2.AssignPositionsFromSchema(ls, prop, value)
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
	values := make([]NestedValue2, 0, len(result.Values))
	for _, ve := range result.Values {
		cfg := configs[ve.Path]
		if !cfg.hasAny() {
			continue
		}
		analyzed, err := a.analyzeNestedValue2(ve, cfg)
		if err != nil {
			return nil, fmt.Errorf("analyze value at %q: %w", ve.Path, err)
		}
		if cfg.filterable {
			numFilterable += len(analyzed)
		}
		values = append(values, analyzed...)
	}

	return &NestedProperty2{
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

// analyzeNestedValue2 converts a raw nested2.ValueEntry into one or more
// NestedValue2 entries with analyzed byte representations. Text values
// may produce multiple entries (one per token); all entries carry the same Pos
// — a 2×uint32 copy per entry, semantically identical to the v1 path's alias.
func (a *Analyzer) analyzeNestedValue2(ve nested2.ValueEntry, cfg nestedIndexConfig) ([]NestedValue2, error) {
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

	out := make([]NestedValue2, len(items))
	for i, item := range items {
		out[i] = NestedValue2{
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
