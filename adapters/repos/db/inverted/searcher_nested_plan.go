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
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// nestedCorrelation describes which bitmap operation should combine two nested
// filter conditions to achieve correct same-element semantics.
type nestedCorrelation int

const (
	// directAnd: positions are naturally aligned (scalar siblings at the same
	// element level, or ancestor/descendant). Standard bitmap AND on full 64-bit
	// values gives correct results.
	directAnd nestedCorrelation = iota

	// maskLeafAnd: paths are in different subtrees of a single object node with
	// no shared intermediate object[] array. Zero the leaf bits (keep root+docID)
	// then AND to align on document identity.
	maskLeafAnd

	// idxLoopAnd: paths are in different subtrees of a shared object[] array.
	// Must iterate _idx.{lcaPath}[N] to verify both conditions fall under the
	// same array element.
	idxLoopAnd
)

// ---------------------------------------------------------------------------
// Resolution plan
// ---------------------------------------------------------------------------

// resolutionPlan is a recursive tree that describes how to combine a set of
// nested filter conditions to achieve correct same-element semantics.
//
// Leaf nodes (groups == nil) hold a flat list of relative paths that share a
// common LCA and are combined with op. Interior maskLeafAnd nodes hold
// sub-plans for each first-level sub-tree group.
//
// Examples:
//
//	addresses.city + addresses.postcode + cars.make + cars.colors
//	→ maskLeafAnd
//	    ├── directAnd  [addresses.city, addresses.postcode]
//	    └── directAnd  [cars.make, cars.colors]
//
//	addresses.city + addresses.postcode + cars.tires.width + cars.accessories.type
//	→ maskLeafAnd
//	    ├── directAnd           [addresses.city, addresses.postcode]
//	    └── idxLoopAnd("cars")
//	            ├── directAnd   [cars.tires.width]
//	            └── directAnd   [cars.accessories.type]
//
//	cars.tires.width + cars.accessories.type + cars.accessories.color
//	→ idxLoopAnd("cars")
//	    ├── directAnd   [cars.tires.width]
//	    └── directAnd   [cars.accessories.type, cars.accessories.color]
type resolutionPlan struct {
	op      nestedCorrelation // operation to apply at this node
	lcaPath string            // set for idxLoopAnd: LCA array path (relative)
	groups  []*resolutionPlan // set for maskLeafAnd interior OR idxLoopAnd interior:
	//                           one sub-plan per pathRem sub-tree beneath the LCA
	paths []string // set for leaf nodes (no sub-groups): full relative paths
}

// resolutionPlanBuilder constructs a resolutionPlan for a set of relative
// nested filter paths. dt and props describe the root property's schema.
//
// Paths are split to segments exactly once and all internal work is done in
// segment space. The schema context is threaded downward so each recursive
// level starts from its LCA's schema rather than re-walking from the root.
//
// Algorithm:
//  1. Group paths by first segment, preserving order.
//  2. Multiple groups → interior maskLeafAnd; classify each group.
//  3. Single group → classifyLeaf:
//     a. Strip ancestor paths (pathRem == empty): their bitmaps are supersets of
//        all siblings and are no-ops in any AND — exclude from further logic.
//     b. Binary pair where one remainder is a direct scalar at the LCA level
//        (inherits all parent positions, superset of siblings) → directAnd
//     c. No remainder passes through a sub-array → directAnd
//     d/e. At least one remainder has a sub-array → recurse with stripped segs
//        and LCA-level schema:
//          · LCA is intermediate object[] → idxLoopAnd(lcaPath) with groups
//          · LCA is plain object or root object[] → maskLeafAnd with groups
type resolutionPlanBuilder struct {
	dt    schema.DataType
	props []*models.NestedProperty
}

// newResolutionPlanBuilder returns a resolutionPlanBuilder for the given root
// property schema. Call build on the result to produce a resolutionPlan.
func newResolutionPlanBuilder(dt schema.DataType, props []*models.NestedProperty) *resolutionPlanBuilder {
	return &resolutionPlanBuilder{dt: dt, props: props}
}

// build is the entry point. Returns an error if paths is empty.
func (b *resolutionPlanBuilder) build(paths []string) (*resolutionPlan, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("resolutionPlanBuilder: no paths provided")
	}
	if len(paths) == 1 {
		return &resolutionPlan{op: directAnd, paths: paths}, nil
	}
	pathsSegs := make([][]string, len(paths))
	for i, p := range paths {
		pathsSegs[i] = strings.Split(p, ".")
	}
	return b.planFromSegs(pathsSegs, paths, b.dt, b.props)
}

// planFromSegs groups paths by first segment and dispatches to classifyLeaf.
// pathsSegs[i] is the pre-split form of paths[i]; dt/props is the schema at
// the current nesting level.
func (b *resolutionPlanBuilder) planFromSegs(
	pathsSegs [][]string,
	paths []string,
	dt schema.DataType,
	props []*models.NestedProperty,
) (*resolutionPlan, error) {
	if len(pathsSegs) <= 1 {
		return &resolutionPlan{op: directAnd, paths: paths}, nil
	}

	seen := map[string]bool{}
	order := []string{}
	byFirst := map[string][]int{}
	for i, pathSegs := range pathsSegs {
		first := pathSegs[0]
		if !seen[first] {
			seen[first] = true
			order = append(order, first)
		}
		byFirst[first] = append(byFirst[first], i)
	}

	if len(order) > 1 {
		subPlans := make([]*resolutionPlan, 0, len(order))
		for _, first := range order {
			idxs := byFirst[first]
			groupPathSegs := make([][]string, len(idxs))
			groupPaths := make([]string, len(idxs))
			for j, idx := range idxs {
				groupPathSegs[j] = pathsSegs[idx]
				groupPaths[j] = paths[idx]
			}
			subPlan, err := b.classifyLeaf(groupPathSegs, groupPaths, dt, props)
			if err != nil {
				return nil, err
			}
			subPlans = append(subPlans, subPlan)
		}
		return &resolutionPlan{op: maskLeafAnd, groups: subPlans}, nil
	}

	return b.classifyLeaf(pathsSegs, paths, dt, props)
}

// classifyLeaf classifies a set of pre-split paths that all share a common
// first segment by computing their LCA and applying the decision rules.
func (b *resolutionPlanBuilder) classifyLeaf(
	pathsSegs [][]string,
	paths []string,
	dt schema.DataType,
	props []*models.NestedProperty,
) (*resolutionPlan, error) {
	if len(pathsSegs) == 0 {
		return nil, fmt.Errorf("resolutionPlanBuilder: no paths provided")
	}
	if len(pathsSegs) == 1 {
		return &resolutionPlan{op: directAnd, paths: paths}, nil
	}

	lcaLen := len(pathsSegs[0])
	for _, ps := range pathsSegs[1:] {
		i := 0
		for i < lcaLen && i < len(ps) && pathsSegs[0][i] == ps[i] {
			i++
		}
		lcaLen = i
	}

	// 3a: exclude ancestor paths — they are supersets of all siblings (no-ops).
	var activePathSegs [][]string
	var activePaths []string
	for i, pathSegs := range pathsSegs {
		if len(pathSegs) > lcaLen {
			activePathSegs = append(activePathSegs, pathSegs)
			activePaths = append(activePaths, paths[i])
		}
	}
	if len(activePaths) <= 1 {
		return &resolutionPlan{op: directAnd, paths: activePaths}, nil
	}

	lcaPathSegs := pathsSegs[0][:lcaLen]
	lcaDT, lcaProps := b.nodeTypeAndProps(lcaPathSegs, dt, props)
	if !schema.IsNested(lcaDT) {
		return nil, fmt.Errorf("resolutionPlanBuilder: LCA node %q is not object or object[], got %q",
			strings.Join(lcaPathSegs, "."), lcaDT)
	}

	activePathRems := make([][]string, len(activePathSegs))
	for i, pathSegs := range activePathSegs {
		activePathRems[i] = pathSegs[lcaLen:]
	}

	// 3b/3c: directAnd suffices when positions are naturally aligned.
	if b.isDirectAndEligible(activePathRems, lcaProps) {
		return &resolutionPlan{op: directAnd, paths: activePaths}, nil
	}

	// 3d/3e: element-level correlation required.
	innerPlan, err := b.planFromSegs(activePathRems, activePaths, lcaDT, lcaProps)
	if err != nil {
		return nil, err
	}

	if lcaDT == schema.DataTypeObjectArray {
		if lcaPath := strings.Join(lcaPathSegs, "."); lcaPath != "" {
			return &resolutionPlan{op: idxLoopAnd, lcaPath: lcaPath, groups: b.extractGroups(innerPlan)}, nil
		}
	}
	return &resolutionPlan{op: maskLeafAnd, groups: b.extractGroups(innerPlan)}, nil
}

// isDirectAndEligible returns true when a plain bitmap AND gives correct
// same-element results without an idx loop:
//   - 3b: binary pair where one remainder is a direct scalar at the LCA level
//   - 3c: no remainder passes through a sub-array
func (b *resolutionPlanBuilder) isDirectAndEligible(pathRems [][]string, lcaProps []*models.NestedProperty) bool {
	if len(pathRems) == 2 {
		if b.isScalarAtLevel(pathRems[0], lcaProps) || b.isScalarAtLevel(pathRems[1], lcaProps) {
			return true
		}
	}
	for _, pathRem := range pathRems {
		if b.containsObjectArray(pathRem, lcaProps) {
			return false
		}
	}
	return true
}

// extractGroups lifts the sub-groups of an interior maskLeafAnd node so they
// can be used directly as groups of the outer operation, or wraps a single
// plan as a one-element slice.
func (b *resolutionPlanBuilder) extractGroups(plan *resolutionPlan) []*resolutionPlan {
	if plan.op == maskLeafAnd && len(plan.groups) > 0 {
		return plan.groups
	}
	return []*resolutionPlan{plan}
}

// nodeTypeAndProps walks pathSegs through the nested schema and returns the
// DataType and NestedProperties at that node. An empty slice returns (dt, props).
func (b *resolutionPlanBuilder) nodeTypeAndProps(
	pathSegs []string,
	dt schema.DataType,
	props []*models.NestedProperty,
) (schema.DataType, []*models.NestedProperty) {
	for _, pathSeg := range pathSegs {
		np := findNestedPropByName(props, pathSeg)
		if np == nil {
			return dt, props
		}
		dt = schema.DataType(np.DataType[0])
		props = np.NestedProperties
	}
	return dt, props
}


// isScalarAtLevel returns true if pathSegs is a single segment resolving to a
// non-array, non-nested scalar property. Such a property inherits ALL leaf
// positions of its parent element and is a superset of any sibling's positions.
func (b *resolutionPlanBuilder) isScalarAtLevel(pathSegs []string, props []*models.NestedProperty) bool {
	if len(pathSegs) != 1 {
		return false
	}
	np := findNestedPropByName(props, pathSegs[0])
	if np == nil {
		return false
	}
	dt := schema.DataType(np.DataType[0])
	return !schema.IsNested(dt) && !schema.IsScalarArrayType(dt)
}

// containsObjectArray returns true if any segment in pathSegs resolves to a
// DataTypeObjectArray property — the path passes through a nested array that
// introduces element-level positions.
func (b *resolutionPlanBuilder) containsObjectArray(pathSegs []string, props []*models.NestedProperty) bool {
	for _, pathSeg := range pathSegs {
		np := findNestedPropByName(props, pathSeg)
		if np == nil {
			return false
		}
		if schema.DataType(np.DataType[0]) == schema.DataTypeObjectArray {
			return true
		}
		props = np.NestedProperties
	}
	return false
}
