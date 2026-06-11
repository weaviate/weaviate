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
	"time"

	"github.com/google/uuid"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// PositionedValue represents a leaf value discovered during position
// assignment. Positions have docID=0; the caller ORs in the real docID.
type PositionedValue struct {
	Path         string                     // dot-notation path, e.g. "addresses.city"
	PropName     string                     // leaf property name, e.g. "city"
	Value        any                        // raw value (caller analyzes based on DataType)
	DataType     schema.DataType            // scalar data type for value analysis
	Tokenization string                     // tokenization strategy (for text types)
	TextAnalyzer *models.TextAnalyzerConfig // custom text analyzer config (may be nil)
	Positions    []uint64                   // encoded positions with docID=0
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

// AnchorEntry records the marker bit of a single element at a given path.
// Distinct from ExistsEntry: ExistsEntry stores the full elementPositions
// (self marker + descendants); AnchorEntry stores only the self marker.
type AnchorEntry struct {
	Path      string   // path of the element's containing collection; empty for root-level
	Positions []uint64 // marker positions with docID=0
}

// AssignResult holds all positioned values and metadata produced by
// walking a nested property value.
type AssignResult struct {
	Values  []PositionedValue
	Idx     []IdxEntry
	Exists  []ExistsEntry
	Anchors []AnchorEntry
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
	default:
		// Unreachable: dt is returned by AsNested which only returns members of
		// NestedDataTypes (DataTypeObject, DataTypeObjectArray).
		return nil, fmt.Errorf("property %q has unexpected data type %q", prop.Name, dt)
	}

	if len(elements) == 0 {
		return &AssignResult{}, nil
	}

	result := &AssignResult{}
	var allPositions []uint64

	for i, elem := range elements {
		if i+1 >= MaxRoots {
			return nil, fmt.Errorf("element count %d exceeds maximum %d for property %q",
				i+1, MaxRoots-1, prop.Name)
		}
		rootIdx := uint16(i + 1)

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

		// Root elements have no ancestors — the chain is empty. walkObject
		// returns this root's subtree selves (self + every descendant marker)
		// without any chain prefix; because the chain is empty here, those are
		// also this root's full elementPositions.
		elemPositions, err := w.walkObject("", elemMap, prop.NestedProperties, nil)
		if err != nil {
			return nil, fmt.Errorf("walk element %d of %q: %w", i, prop.Name, err)
		}

		// Root-level _idx entry: records which positions belong to root element i.
		// Required for arr[N] positional filtering (e.g. "addresses[1].city = X").
		result.Idx = append(result.Idx, IdxEntry{
			Path:      "",
			Index:     i,
			Positions: elemPositions,
		})

		allPositions = append(allPositions, elemPositions...)
	}

	// Root-level _exists: all positions across all root elements
	result.Exists = append(result.Exists, ExistsEntry{
		Path:      "",
		Positions: allPositions,
	})

	return result, nil
}

// walker is not goroutine-safe; each call to AssignPositions must use its own instance.
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

// walkObject processes a single object element depth-first.
//
// ancestorChain is the sequence of every enclosing element's self marker, in
// outer-to-inner order, visible to this element. Per the encoding rule an
// element's positions are `ancestorChain ∪ self ∪ descendant selves`; that
// is what we emit on this element's behalf for Value, Exists, and (via the
// caller) Idx entries.
//
// The returned slice contains this element's subtree selves only — the self
// marker followed by every descendant self marker, without any chain. Callers
// prepend their own chain when materializing full elementPositions for the
// emissions they own (walkNestedArray for Idx/Exists at the array path; the
// top-level AssignPositions for the root Idx/Exists).
func (w *walker) walkObject(prefix string, obj map[string]any,
	nestedProps []*models.NestedProperty, ancestorChain []uint64,
) ([]uint64, error) {
	// Phase 0: Allocate this element's marker BEFORE walking descendants so
	// DFS leaf assignment puts the parent's marker ahead of its children's.
	// Predecessor scans over a parent anchor bitmap can then lift each child
	// position to its owning parent.
	selfMarker, err := w.nextLeaf()
	if err != nil {
		return nil, err
	}
	w.result.Anchors = append(w.result.Anchors, AnchorEntry{
		Path:      prefix,
		Positions: []uint64{selfMarker},
	})

	// Chain visible to descendants extends our chain with our own marker.
	// Fresh allocation so the caller's slice cannot be aliased by ours.
	chainForChildren := make([]uint64, 0, len(ancestorChain)+1)
	chainForChildren = append(chainForChildren, ancestorChain...)
	chainForChildren = append(chainForChildren, selfMarker)

	// descendantSelves accumulates every marker emitted below this element
	// (children, grandchildren, …). Children return their own subtree selves
	// (self + theirs), which composes naturally.
	var descendantSelves []uint64

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
			childPos, err := w.walkNestedArray(path, dt, val, np.NestedProperties, chainForChildren)
			if err != nil {
				return nil, err
			}
			descendantSelves = append(descendantSelves, childPos...)
		} else if schema.IsScalarArrayType(dt) {
			childPos, err := w.walkScalarArray(path, dt, val, np, chainForChildren)
			if err != nil {
				return nil, err
			}
			descendantSelves = append(descendantSelves, childPos...)
		}
	}

	// Phase 2: elementPositions = ancestor chain + self + descendant selves.
	// This is the value we hand out as our own elementPositions for Phase 3
	// scalar emissions. The returned subtree-selves slice deliberately omits
	// the chain so the caller can prepend its own without duplication.
	elementPositions := make([]uint64, 0, len(ancestorChain)+1+len(descendantSelves))
	elementPositions = append(elementPositions, ancestorChain...)
	elementPositions = append(elementPositions, selfMarker)
	elementPositions = append(elementPositions, descendantSelves...)

	// Phase 3: Process scalar properties. Scalars inherit ALL positions of
	// their parent element — which already include the ancestor chain.
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
				PropName:     np.Name,
				Value:        val,
				DataType:     dt,
				Tokenization: np.Tokenization,
				TextAnalyzer: np.TextAnalyzer,
				Positions:    elementPositions,
			})
			w.result.Exists = append(w.result.Exists, ExistsEntry{
				Path:      path,
				Positions: elementPositions,
			})
		}
	}

	// Return subtree selves (self + descendant selves) without chain. The
	// caller (walkNestedArray or top-level AssignPositions) prepends its own
	// chain when building IdxEntry/ExistsEntry at the array level.
	subtreeSelves := make([]uint64, 0, 1+len(descendantSelves))
	subtreeSelves = append(subtreeSelves, selfMarker)
	subtreeSelves = append(subtreeSelves, descendantSelves...)
	return subtreeSelves, nil
}

// walkNestedArray processes a nested object or object[] property.
// For object type, the value is wrapped in a 1-element array.
//
// ancestorChain is the chain of self markers visible to each array element
// (i.e. the chain produced by the enclosing walkObject for its descendants).
// We prepend it to every per-element subtree to materialize full
// elementPositions for IdxEntry and ExistsEntry.
//
// The returned slice contains every element's subtree selves (no chain),
// suitable for the caller to splice into its own descendantSelves.
func (w *walker) walkNestedArray(path string, dt schema.DataType,
	val any, nestedProps []*models.NestedProperty, ancestorChain []uint64,
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
	default:
		// Unreachable: walkNestedArray is only called when IsNested(dt) is true,
		// which only holds for DataTypeObject and DataTypeObjectArray.
		return nil, fmt.Errorf("unexpected data type %q at path %q", dt, path)
	}

	var allSubtreeSelves []uint64
	for i, elem := range elements {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map for %s[%d], got %T", path, i, elem)
		}

		subtreeSelves, err := w.walkObject(path, elemMap, nestedProps, ancestorChain)
		if err != nil {
			return nil, err
		}

		// IdxEntry needs the K-th element's full elementPositions:
		// chain + (self + descendant selves).
		idxPositions := make([]uint64, 0, len(ancestorChain)+len(subtreeSelves))
		idxPositions = append(idxPositions, ancestorChain...)
		idxPositions = append(idxPositions, subtreeSelves...)

		w.result.Idx = append(w.result.Idx, IdxEntry{
			Path:      path,
			Index:     i,
			Positions: idxPositions,
		})

		allSubtreeSelves = append(allSubtreeSelves, subtreeSelves...)
	}

	if len(allSubtreeSelves) > 0 {
		// ExistsEntry covers the entire array under the shared chain.
		existsPositions := make([]uint64, 0, len(ancestorChain)+len(allSubtreeSelves))
		existsPositions = append(existsPositions, ancestorChain...)
		existsPositions = append(existsPositions, allSubtreeSelves...)
		w.result.Exists = append(w.result.Exists, ExistsEntry{
			Path:      path,
			Positions: existsPositions,
		})
	}

	return allSubtreeSelves, nil
}

// walkScalarArray processes a scalar array property (text[], int[], etc.).
// Each element gets its own leaf position.
//
// ancestorChain is the chain visible to each scalar element. Value and Idx
// entries carry the full elementPositions (chain + self); Anchor is exact
// (self only); ExistsEntry collects the chain plus every element's self.
//
// The returned slice contains every element's self marker (no chain) for the
// caller to splice into its own descendantSelves.
func (w *walker) walkScalarArray(path string, dt schema.DataType,
	val any, np *models.NestedProperty, ancestorChain []uint64,
) ([]uint64, error) {
	scalarDT := schema.ScalarFromArrayType(dt)
	var allSelves []uint64

	// appendElem records a single array element directly — elem is assignable
	// to PositionedValue.Value (type any) without an intermediate []any copy.
	appendElem := func(i int, elem any) error {
		pos, err := w.nextLeaf()
		if err != nil {
			return fmt.Errorf("scalar array %s[%d]: %w", path, i, err)
		}
		// elementPositions = chain + self. Fresh allocation so each entry's
		// Positions slice is independent of any shared backing array.
		elementPositions := make([]uint64, 0, len(ancestorChain)+1)
		elementPositions = append(elementPositions, ancestorChain...)
		elementPositions = append(elementPositions, pos)
		w.result.Values = append(w.result.Values, PositionedValue{
			Path:         path,
			PropName:     np.Name,
			Value:        elem,
			DataType:     scalarDT,
			Tokenization: np.Tokenization,
			TextAnalyzer: np.TextAnalyzer,
			Positions:    elementPositions,
		})
		w.result.Idx = append(w.result.Idx, IdxEntry{
			Path:      path,
			Index:     i,
			Positions: elementPositions,
		})
		w.result.Anchors = append(w.result.Anchors, AnchorEntry{
			Path:      path,
			Positions: []uint64{pos},
		})
		allSelves = append(allSelves, pos)
		return nil
	}

	// Iterate directly over the concrete slice type. Values arrive in different
	// forms depending on the write path:
	//   - API/JSON path: enrichSchemaTypes converts []interface{} to []string,
	//     []float64, or []bool based on the element type.
	//   - Internal paths (e.g. direct programmatic insertion without a JSON
	//     round-trip) may produce []int, []time.Time, or []uuid.UUID.
	// All cases are handled defensively so that walkScalarArray remains correct
	// regardless of how the caller obtained the value.
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
	case []int:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	case []time.Time:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	case []uuid.UUID:
		for i, elem := range v {
			if err := appendElem(i, elem); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("expected []any for %s, got %T", path, val)
	}

	if len(allSelves) == 0 {
		return nil, nil
	}

	// ExistsEntry for the scalar-array path covers the chain plus every
	// element's self marker — the union of all per-element positions.
	existsPositions := make([]uint64, 0, len(ancestorChain)+len(allSelves))
	existsPositions = append(existsPositions, ancestorChain...)
	existsPositions = append(existsPositions, allSelves...)
	w.result.Exists = append(w.result.Exists, ExistsEntry{
		Path:      path,
		Positions: existsPositions,
	})

	return allSelves, nil
}

// joinPath concatenates prefix + separator + name without the
// []string + strings.Join allocation that filnested.JoinPath incurs.
// Called once per NestedProperty per object during nested write
// analysis, so the per-call alloc adds up on heavy ingest.
func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + filnested.PathSep + name
}
