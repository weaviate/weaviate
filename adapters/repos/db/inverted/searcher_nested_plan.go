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

// pathRelationship holds the result of nestedPathsRelationship.
type pathRelationship struct {
	kind    nestedCorrelation
	lcaPath string // set for idxLoopAnd: dot-notation path of the LCA array
}

// nestedPathsRelationship classifies the correlation between N relative nested
// filter paths (stripped of the root property name) and returns which bitmap
// operation should be used to combine them.
//
// rootDT is the DataType of the root property (DataTypeObject or
// DataTypeObjectArray). rootProps are its NestedProperties.
//
// Decision rules applied to the group LCA (longest common prefix of all paths):
//  1. Any path is an ancestor of the others (empty remaining) → directAnd
//  2. Any path is a direct scalar at the LCA level (inherits all positions,
//     superset of siblings) → directAnd
//  3. No path goes through a sub-array (all scalar siblings at LCA level,
//     identical positions) → directAnd
//  4. At least one path has a sub-array:
//     – LCA is intermediate object[] → idxLoopAnd (leaf_idx encodes element
//     identity and is zeroed by MaskLeafPositions)
//     – LCA is single object or root object[] → maskLeafAnd
func nestedPathsRelationship(
	paths []string,
	rootDT schema.DataType,
	rootProps []*models.NestedProperty,
) (pathRelationship, error) {
	if len(paths) <= 1 {
		// Zero or one condition — no correlation needed.
		return pathRelationship{kind: directAnd}, nil
	}

	// Compute the group LCA as the longest common prefix of all paths.
	lcaSegs := strings.Split(paths[0], ".")
	for _, p := range paths[1:] {
		segs := strings.Split(p, ".")
		i := 0
		for i < len(lcaSegs) && i < len(segs) && lcaSegs[i] == segs[i] {
			i++
		}
		lcaSegs = lcaSegs[:i]
	}

	// Compute remaining segments for each path after the LCA.
	rems := make([][]string, len(paths))
	for i, p := range paths {
		segs := strings.Split(p, ".")
		rems[i] = segs[len(lcaSegs):]
		// Empty remaining means this path IS the LCA — ancestor of all others.
		if len(rems[i]) == 0 {
			return pathRelationship{kind: directAnd}, nil
		}
	}

	lcaDT, lcaProps := nodeTypeAndProps(lcaSegs, rootDT, rootProps)

	if !schema.IsNested(lcaDT) {
		return pathRelationship{}, fmt.Errorf("nested path correlation requires object or object[] property, got %q", lcaDT)
	}

	// Direct AND: for a binary pair, a scalar property at the LCA level inherits
	// ALL leaf positions of its parent element (superset of the other's positions).
	// This shortcut does NOT extend to N>2: a scalar is a superset of each
	// sibling individually, but ANDing two non-scalar siblings may produce empty
	// results even when a document satisfies all conditions in the same element.
	// Example: make + colors + accessories.type on a Tesla car — direct AND of
	// {l4..l9} ∩ {l9} ∩ {l7} = {} even though all three match the same car.
	if len(rems) == 2 {
		if isScalarAtLevel(rems[0], lcaProps) || isScalarAtLevel(rems[1], lcaProps) {
			return pathRelationship{kind: directAnd}, nil
		}
	}

	// Direct AND: no path goes through a sub-array — all are scalar siblings at
	// the same element level with identical positions.
	allSimple := true
	for _, rem := range rems {
		if containsObjectArray(rem, lcaProps) {
			allSimple = false
			break
		}
	}
	if allSimple {
		return pathRelationship{kind: directAnd}, nil
	}

	// At least one path descends into a further sub-array. For intermediate
	// object[] arrays the leaf_idx encodes element identity and is zeroed by
	// MaskLeafPositions, so the _idx loop is required. At the root level or
	// under a single object, root_idx or docID alignment is sufficient.
	lcaPath := strings.Join(lcaSegs, ".")
	if lcaDT == schema.DataTypeObjectArray && lcaPath != "" {
		return pathRelationship{kind: idxLoopAnd, lcaPath: lcaPath}, nil
	}
	return pathRelationship{kind: maskLeafAnd}, nil
}

// nodeTypeAndProps walks segs through the nested schema and returns the
// DataType and NestedProperties at the node identified by segs. For an empty
// segs slice it returns (rootDT, rootProps) — the root.
func nodeTypeAndProps(
	segs []string,
	rootDT schema.DataType,
	rootProps []*models.NestedProperty,
) (schema.DataType, []*models.NestedProperty) {
	dt := rootDT
	props := rootProps
	for _, seg := range segs {
		np := findNestedPropByName(props, seg)
		if np == nil {
			return dt, props
		}
		dt = schema.DataType(np.DataType[0])
		props = np.NestedProperties
	}
	return dt, props
}

// findNestedPropByName returns the NestedProperty with the given name, or nil.
func findNestedPropByName(props []*models.NestedProperty, name string) *models.NestedProperty {
	for _, np := range props {
		if np.Name == name {
			return np
		}
	}
	return nil
}

// isScalarAtLevel returns true if segs is a single segment that resolves to a
// non-array, non-nested scalar property (text, int, number, bool, date, uuid).
// Such a property inherits ALL leaf positions of its parent element, making it
// a superset of any sibling's positions and safe for Direct AND.
func isScalarAtLevel(segs []string, props []*models.NestedProperty) bool {
	if len(segs) != 1 {
		return false
	}
	np := findNestedPropByName(props, segs[0])
	if np == nil {
		return false
	}
	dt := schema.DataType(np.DataType[0])
	return !schema.IsNested(dt) && !schema.IsScalarArrayType(dt)
}

// containsObjectArray returns true if any segment in segs resolves to a
// DataTypeObjectArray property in the given schema level — indicating the path
// passes through a nested array that introduces element-level positions.
func containsObjectArray(segs []string, props []*models.NestedProperty) bool {
	for _, seg := range segs {
		np := findNestedPropByName(props, seg)
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
//	    ├── directAnd      [addresses.city, addresses.postcode]
//	    └── idxLoopAnd("cars") [cars.tires.width, cars.accessories.type]
//
//	cars.tires.width + cars.colors + cars.accessories.type
//	→ idxLoopAnd("cars") [cars.tires.width, cars.colors, cars.accessories.type]
type resolutionPlan struct {
	op      nestedCorrelation // operation to apply at this node
	lcaPath string            // set for idxLoopAnd: LCA array path (relative)
	groups  []*resolutionPlan // set for maskLeafAnd: one sub-plan per sub-tree
	paths   []string          // set for leaf nodes: full relative paths
}

// buildResolutionPlan constructs a resolutionPlan for the given relative nested
// paths (stripped of the root property name). rootDT and rootProps describe the
// root property's schema. The same rootDT/rootProps are used for ALL recursive
// calls so that nestedPathsRelationship always operates in the correct
// position-encoding context.
//
// Algorithm:
//  1. Group paths by first segment. If there are multiple groups the operation
//     is always maskLeafAnd — nestedPathsRelationship is not needed.
//  2. For a single group, call nestedPathsRelationship to determine whether it
//     is directAnd or idxLoopAnd.
func buildResolutionPlan(
	paths []string,
	rootDT schema.DataType,
	rootProps []*models.NestedProperty,
) (*resolutionPlan, error) {
	if len(paths) <= 1 {
		return &resolutionPlan{op: directAnd, paths: paths}, nil
	}

	// Group paths by first segment, preserving insertion order.
	seen := map[string]bool{}
	order := []string{}
	byFirst := map[string][]string{}
	for _, p := range paths {
		first, _, _ := strings.Cut(p, ".")
		if !seen[first] {
			seen[first] = true
			order = append(order, first)
		}
		byFirst[first] = append(byFirst[first], p)
	}

	if len(order) > 1 {
		// Paths span multiple sub-trees → always maskLeafAnd.
		// nestedPathsRelationship is not needed: cross-subtree divergence always
		// requires MaskLeafPositions AND to align on root+docID.
		subPlans := make([]*resolutionPlan, 0, len(order))
		for _, first := range order {
			subPlan, err := buildResolutionPlan(byFirst[first], rootDT, rootProps)
			if err != nil {
				return nil, err
			}
			subPlans = append(subPlans, subPlan)
		}
		return &resolutionPlan{op: maskLeafAnd, groups: subPlans}, nil
	}

	// Single group: all paths share the same first segment.
	// Use nestedPathsRelationship to determine the operation.
	rel, err := nestedPathsRelationship(paths, rootDT, rootProps)
	if err != nil {
		return nil, err
	}

	switch rel.kind {
	case directAnd:
		return &resolutionPlan{op: directAnd, paths: paths}, nil
	case idxLoopAnd:
		return &resolutionPlan{op: idxLoopAnd, lcaPath: rel.lcaPath, paths: paths}, nil
	case maskLeafAnd:
		// maskLeafAnd within a single group means paths diverge under a sub-object
		// node. Return a leaf maskLeafAnd — the executor will apply MaskLeafPositions
		// AND across the paths.
		return &resolutionPlan{op: maskLeafAnd, paths: paths}, nil
	}

	return nil, fmt.Errorf("buildResolutionPlan: unhandled correlation kind %d", rel.kind)
}
