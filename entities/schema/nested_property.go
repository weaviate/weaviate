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

package schema

import "github.com/weaviate/weaviate/entities/models"

// Default values applied when a NestedProperty's index setting is unset
// (the pointer field is nil). Each index type has its own default:
//   - filterable defaults to true (matches flat-property behaviour: a
//     property is filterable unless explicitly disabled).
//   - searchable defaults to true (matches flat-property behaviour for
//     text types; ignored for non-text types).
//   - rangeable defaults to false (opt-in for numeric/date types to
//     avoid creating the rangeable bucket unless requested).
const (
	NestedIndexFilterableDefault = true
	NestedIndexSearchableDefault = true
	NestedIndexRangeableDefault  = false
)

// IsNestedFilterable reports whether the filterable index is enabled on
// the given nested property. Returns NestedIndexFilterableDefault when
// IndexFilterable is unset.
func IsNestedFilterable(np *models.NestedProperty) bool {
	if np.IndexFilterable == nil {
		return NestedIndexFilterableDefault
	}
	return *np.IndexFilterable
}

// IsNestedSearchable reports whether the searchable index is enabled on
// the given nested property for the given data type. The searchable index
// is only meaningful for text and text-array types; for all other types
// the result is false regardless of the flag. Returns
// NestedIndexSearchableDefault when IndexSearchable is unset on a text
// type.
func IsNestedSearchable(np *models.NestedProperty, dt DataType) bool {
	switch dt {
	case DataTypeText, DataTypeTextArray:
		if np.IndexSearchable == nil {
			return NestedIndexSearchableDefault
		}
		return *np.IndexSearchable
	default:
		return false
	}
}

// IsNestedRangeable reports whether the rangeable index is enabled on
// the given nested property for the given data type. The rangeable index
// is only meaningful for int/number/date types (and their array variants);
// for all other types the result is false regardless of the flag. Returns
// NestedIndexRangeableDefault when IndexRangeFilters is unset on a
// supported type.
func IsNestedRangeable(np *models.NestedProperty, dt DataType) bool {
	switch dt {
	case DataTypeInt, DataTypeIntArray,
		DataTypeNumber, DataTypeNumberArray,
		DataTypeDate, DataTypeDateArray:
		if np.IndexRangeFilters == nil {
			return NestedIndexRangeableDefault
		}
		return *np.IndexRangeFilters
	default:
		return false
	}
}
