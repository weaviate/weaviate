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

// assignPositionsFn is the AssignPositions implementation used by fast-path
// tests (the single chokepoint is fastPathIndex.addDoc). Flip this variable
// to swap encodings without touching any test body:
//
//   - AssignPositions:            current rootIdx | leafIdx | docID layout
//   - assignPositionsLeafDocOnly: rootIdx=0 always, doc-global leafIdx
//
// If the fast-path suite passes under both backends, the rootIdx half of the
// position encoding is provably redundant for those code paths. The next
// step is then a real encoding swap (new Encode layout, new MaskRootLeaf,
// new LiftToAncestor using a docID-only bucket mask) — at which point this
// variable becomes one slot in a larger encodingBackend struct.
var assignPositionsFn func(prop *models.Property, value any) (*AssignResult, error) = AssignPositions

// Keep the alternate backend reachable so it is not reported as unused while it
// awaits being wired in as the active assignPositionsFn. Flip assignPositionsFn
// to assignPositionsLeafDocOnly to exercise it.
var _ = assignPositionsLeafDocOnly

// assignPositionsLeafDocOnly is a drop-in replacement for AssignPositions that
// proves the rootIdx half of the position encoding is redundant for the
// fast-path filtering pipeline. It produces the same AssignResult shape, but
// every position has rootIdx == 0 and leafIdx is assigned monotonically across
// the entire document (DFS over every top-level element, not restarting per
// root). With rootIdx pinned to 0:
//
//   - Encode(0, leafIdx, docID) collapses every top-level root of a doc into
//     a single position bucket — the docID alone.
//   - LiftToAncestor's bucket key (pos & zeroLeafBits) reduces to the docID
//     bits, so the floor query still picks the correct enclosing parent
//     provided leafIdx is monotone across all roots in DFS order. The shared
//     walker below guarantees that.
//   - MaskRootLeaf / MaskLeaf still zero the (already-zero) root bits, so no
//     reader needs to change.
//
// If the fast-path test suite passes under this backend, the 14 rootIdx bits
// in the current encoding are not load-bearing.
func assignPositionsLeafDocOnly(prop *models.Property, value any) (*AssignResult, error) {
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
		return nil, fmt.Errorf("property %q has unexpected data type %q", prop.Name, dt)
	}

	if len(elements) == 0 {
		return &AssignResult{}, nil
	}

	result := &AssignResult{}
	var allPositions []uint64

	// Single walker shared across all top-level elements. rootIdx stays 0;
	// leafIdx advances doc-globally so DFS marker order is preserved across
	// roots — that's what keeps LiftToAncestor correct under a docID-only
	// bucket mask.
	w := &walker{rootIdx: 0, leafIdx: 1, result: result}

	for i, elem := range elements {
		elemMap, ok := elem.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map[string]any for %q[%d], got %T",
				prop.Name, i, elem)
		}

		elemPositions, err := w.walkObject("", elemMap, prop.NestedProperties, nil)
		if err != nil {
			return nil, fmt.Errorf("walk element %d of %q: %w", i, prop.Name, err)
		}

		result.Idx = append(result.Idx, IdxEntry{
			Path:      "",
			Index:     i,
			Positions: elemPositions,
		})

		allPositions = append(allPositions, elemPositions...)
	}

	result.Exists = append(result.Exists, ExistsEntry{
		Path:      "",
		Positions: allPositions,
	})

	return result, nil
}
