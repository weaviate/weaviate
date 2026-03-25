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

package nested

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// PositionedValue represents a leaf value discovered during position
// assignment. Positions have docID=0; the caller ORs in the real docID.
type PositionedValue struct {
	Path         string          // dot-notation path, e.g. "addresses.city"
	Value        any             // raw value (caller analyzes based on DataType)
	DataType     schema.DataType // scalar data type for value analysis
	Tokenization string          // tokenization strategy (for text types)
	Positions    []uint64        // encoded positions with docID=0
}

// IdxEntry records which positions belong to a specific array element.
// Used for same-element verification in cross-sibling filter correlation.
type IdxEntry struct {
	Path      string   // array path, e.g. "addresses" or "owner.nicknames"
	Index     int      // 0-based element index
	Positions []uint64 // positions with docID=0
}

// ExistsEntry records which positions have a given property present.
// Used for IS NULL checks and ALL/NONE filter operators.
type ExistsEntry struct {
	Path      string   // property path; empty string for root-level exists
	Positions []uint64 // positions with docID=0
}

// AssignResult holds all positioned values and metadata produced by
// walking a nested property value.
type AssignResult struct {
	Values []PositionedValue
	Idx    []IdxEntry
	Exists []ExistsEntry
}

// AssignPositions walks a nested property value depth-first, assigns
// leaf positions according to the position assignment rules, and returns
// positioned values and metadata entries.
//
// The property must be of type object or object[]. For object type,
// the value is wrapped in a 1-element array with root_idx=1.
//
// All positions have docID=0; the caller ORs in the real docID during write.
func AssignPositions(prop *models.Property, value any) (*AssignResult, error) {
	if value == nil {
		return &AssignResult{}, nil
	}

	dt, ok := schema.AsNested(prop.DataType)
	if !ok {
		return nil, fmt.Errorf("property %q is not a nested type", prop.Name)
	}

	var elements []any
	switch dt {
	case schema.DataTypeObject:
		elements = []any{value}
	case schema.DataTypeObjectArray:
		arr, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("expected []any for object[] %q, got %T", prop.Name, value)
		}
		elements = arr
	}

	if len(elements) == 0 {
		return &AssignResult{}, nil
	}

	result := &AssignResult{}
	var allPositions []uint64

	for i, elem := range elements {
		rootIdx := uint16(i + 1)
		if int(rootIdx) >= MaxRoots {
			return nil, fmt.Errorf("element count %d exceeds maximum %d for property %q",
				i+1, MaxRoots-1, prop.Name)
		}

		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any for %q[%d], got %T",
				prop.Name, i, elem)
		}

		w := &walker{
			rootIdx: rootIdx,
			leafIdx: 1,
			result:  result,
		}

		elemPositions, err := w.walkObject("", elemMap, prop.NestedProperties)
		if err != nil {
			return nil, fmt.Errorf("walk element %d of %q: %w", i, prop.Name, err)
		}

		allPositions = append(allPositions, elemPositions...)
	}

	// Root-level _exists: all positions across all root elements
	result.Exists = append(result.Exists, ExistsEntry{
		Path:      "",
		Positions: allPositions,
	})

	return result, nil
}

type walker struct {
	rootIdx uint16
	leafIdx uint16
	result  *AssignResult
}

func (w *walker) nextLeaf() (uint64, error) {
	if int(w.leafIdx) >= MaxLeavesPerRoot {
		return 0, fmt.Errorf("leaf count %d exceeds maximum %d", w.leafIdx, MaxLeavesPerRoot-1)
	}
	pos := Encode(w.rootIdx, w.leafIdx, 0)
	w.leafIdx++
	return pos, nil
}

// walkObject processes a single object element depth-first and returns
// its positions. The positions are either inherited from descendant
// leaves (intermediate node) or a newly assigned leaf position (leaf node).
func (w *walker) walkObject(prefix string, obj map[string]any,
	nestedProps []*models.NestedProperty,
) ([]uint64, error) {
	var descendantPositions []uint64

	// Phase 1: Process nested objects/arrays and scalar arrays depth-first.
	// This must happen before scalar properties so leaf positions are
	// assigned in depth-first order.
	for _, np := range nestedProps {
		val, exists := obj[np.Name]
		if !exists || val == nil {
			continue
		}

		path := joinPath(prefix, np.Name)
		dt := schema.DataType(np.DataType[0])

		if schema.IsNested(dt) {
			childPos, err := w.walkNestedArray(path, dt, val, np.NestedProperties)
			if err != nil {
				return nil, err
			}
			descendantPositions = append(descendantPositions, childPos...)
		} else if schema.IsScalarArrayType(dt) {
			childPos, err := w.walkScalarArray(path, dt, val, np)
			if err != nil {
				return nil, err
			}
			descendantPositions = append(descendantPositions, childPos...)
		}
	}

	// Phase 2: Determine element positions.
	// - Has descendant leaves → intermediate: positions = union of descendants
	// - No descendant leaves → leaf: gets its own leaf_idx
	var elementPositions []uint64
	if len(descendantPositions) > 0 {
		elementPositions = descendantPositions
	} else {
		pos, err := w.nextLeaf()
		if err != nil {
			return nil, err
		}
		elementPositions = []uint64{pos}
	}

	// Phase 3: Process scalar properties. Scalars inherit ALL positions
	// of their parent element.
	for _, np := range nestedProps {
		val, exists := obj[np.Name]
		if !exists || val == nil {
			continue
		}

		path := joinPath(prefix, np.Name)
		dt := schema.DataType(np.DataType[0])

		if !schema.IsNested(dt) && !schema.IsScalarArrayType(dt) {
			w.result.Values = append(w.result.Values, PositionedValue{
				Path:         path,
				Value:        val,
				DataType:     dt,
				Tokenization: np.Tokenization,
				Positions:    elementPositions,
			})
			w.result.Exists = append(w.result.Exists, ExistsEntry{
				Path:      path,
				Positions: elementPositions,
			})
		}
	}

	return elementPositions, nil
}

// walkNestedArray processes a nested object or object[] property.
// For object type, the value is wrapped in a 1-element array.
func (w *walker) walkNestedArray(path string, dt schema.DataType,
	val any, nestedProps []*models.NestedProperty,
) ([]uint64, error) {
	var elements []any
	switch dt {
	case schema.DataTypeObject:
		elements = []any{val}
	case schema.DataTypeObjectArray:
		arr, ok := val.([]any)
		if !ok {
			return nil, fmt.Errorf("expected []any for %s, got %T", path, val)
		}
		elements = arr
	}

	var allPositions []uint64
	for i, elem := range elements {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map for %s[%d], got %T", path, i, elem)
		}

		elemPositions, err := w.walkObject(path, elemMap, nestedProps)
		if err != nil {
			return nil, err
		}

		w.result.Idx = append(w.result.Idx, IdxEntry{
			Path:      path,
			Index:     i,
			Positions: elemPositions,
		})

		allPositions = append(allPositions, elemPositions...)
	}

	if len(allPositions) > 0 {
		w.result.Exists = append(w.result.Exists, ExistsEntry{
			Path:      path,
			Positions: allPositions,
		})
	}

	return allPositions, nil
}

// walkScalarArray processes a scalar array property (text[], int[], etc.).
// Each element gets its own leaf position.
func (w *walker) walkScalarArray(path string, dt schema.DataType,
	val any, np *models.NestedProperty,
) ([]uint64, error) {
	scalarDT := schema.ScalarFromArrayType(dt)
	var allPositions []uint64

	// appendElem records a single array element directly — elem is assignable
	// to PositionedValue.Value (type any) without an intermediate []any copy.
	appendElem := func(i int, elem any) error {
		pos, err := w.nextLeaf()
		if err != nil {
			return fmt.Errorf("scalar array %s[%d]: %w", path, i, err)
		}
		positions := []uint64{pos}
		w.result.Values = append(w.result.Values, PositionedValue{
			Path:         path,
			Value:        elem,
			DataType:     scalarDT,
			Tokenization: np.Tokenization,
			Positions:    positions,
		})
		w.result.Idx = append(w.result.Idx, IdxEntry{
			Path:      path,
			Index:     i,
			Positions: positions,
		})
		allPositions = append(allPositions, pos)
		return nil
	}

	// Iterate directly over the concrete slice type — handles both the original
	// []any form and typed slices produced by JSON round-trips ([]string etc.).
	switch v := val.(type) {
	case []any:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	case []string:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	case []float64:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	case []bool:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("expected []any for %s, got %T", path, val)
	}

	if len(allPositions) == 0 {
		return nil, nil
	}

	w.result.Exists = append(w.result.Exists, ExistsEntry{
		Path:      path,
		Positions: allPositions,
	})

	return allPositions, nil
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}
