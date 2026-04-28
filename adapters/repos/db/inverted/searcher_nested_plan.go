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
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// groupOp describes how the condition bitmaps within a conditionGroup are combined.
type groupOp int

const (
	// groupAndAll: all conditions are single and positions are naturally aligned
	// (no masked results, no further object[] in any path rem beyond the group
	// LCA). Raw AndAll on full 64-bit positions gives correct results because
	// leaf bits encode same-element identity — e.g. two scalars in the same car
	// share the same leaf, two scalars in different cars do not.
	groupAndAll groupOp = iota

	// groupAndAllMaskLeaf: the group LCA is the root object[] itself (lcaPath=="")
	// and at least one path has masked combinePositions output (multiple
	// independents or tokens+independent). root_idx already encodes element
	// identity so AndAllMaskLeaf gives correct same-root-element semantics
	// without an idx loop.
	groupAndAllMaskLeaf

	// groupRunIdxLoop: the group LCA is an intermediate object[] (lcaPath!="").
	// Must iterate _idx.{lcaPath}[N] entries to verify all conditions land in
	// the same array element.
	groupRunIdxLoop
)

// conditionGroup is one set of relPaths that share a common ObjectArray LCA
// and are combined with a single operation.
type conditionGroup struct {
	op      groupOp
	lcaPath string   // non-empty for groupRunIdxLoop: the intermediate object[] LCA
	paths   []string // relPaths whose condition bitmaps participate in this group
}

// executionPlan describes how the executor should combine condition bitmaps.
// The plan builder is the single authority that decides the strategy; the
// executor's job is only to carry it out.
type executionPlan struct {
	// groups is the ordered list of condition groups for the normal (positive
	// conditions present) execution path.
	groups []conditionGroup
	// useRootAnchor is set when all conditions are IsNull=true and there are no
	// positive conditions. The executor must use the root-level _exists bitmap
	// as the element universe and subtract the IsNull=true excludes from it at
	// raw (leaf) level to preserve same-element semantics.
	useRootAnchor bool
}

// executionPlanBuilder groups relPaths by their common ObjectArray LCA and
// assigns each group the appropriate combining operation.
// Only the root property schema (props) is stored on the struct — it is
// invariant for the lifetime of the builder. conditionCounts (per-path token
// and independent counts) is query-specific and passed to build() directly.
type executionPlanBuilder struct {
	props []*models.NestedProperty
}

// newExecutionPlanBuilder returns a builder for the given root property schema.
func newExecutionPlanBuilder(props []*models.NestedProperty) *executionPlanBuilder {
	return &executionPlanBuilder{props: props}
}

// build is the entry point. counts maps each relPath to [tokens, independents]
// so the builder can determine which paths produce leaf-masked results.
// When paths is empty (all conditions are IsNull=true), the plan has
// useRootAnchor=true and no groups — the executor uses the root _exists bitmap.
func (b *executionPlanBuilder) build(paths []string, counts conditionCounts) (executionPlan, error) {
	switch len(paths) {
	case 0:
		return executionPlan{useRootAnchor: true}, nil
	case 1:
		return executionPlan{groups: []conditionGroup{b.singlePathGroup(paths[0], counts)}}, nil
	default:
		return b.groupPaths(paths, counts), nil
	}
}

// groupPaths groups paths by their first segment, computes the common
// ObjectArray LCA for each first-segment group, and assigns the combining op.
func (b *executionPlanBuilder) groupPaths(paths []string, counts conditionCounts) executionPlan {
	type rawGroup struct {
		segs  [][]string
		paths []string
	}
	seen := map[string]bool{}
	order := []string{}
	byFirst := map[string]*rawGroup{}

	for _, p := range paths {
		segs := filnested.SplitPath(p)
		first := segs[0]
		if !seen[first] {
			seen[first] = true
			order = append(order, first)
			byFirst[first] = &rawGroup{}
		}
		byFirst[first].segs = append(byFirst[first].segs, segs)
		byFirst[first].paths = append(byFirst[first].paths, p)
	}

	groups := make([]conditionGroup, 0, len(order))
	for _, first := range order {
		g := byFirst[first]
		lcaPath := b.commonObjectArrayLCA(g.segs, g.paths)
		op := b.determineGroupOp(g.paths, lcaPath, counts)
		groups = append(groups, conditionGroup{op: op, lcaPath: lcaPath, paths: g.paths})
	}
	return executionPlan{groups: groups}
}

// singlePathGroup builds a conditionGroup for a single relPath.
func (b *executionPlanBuilder) singlePathGroup(path string, counts conditionCounts) conditionGroup {
	lcaPath := b.lastIntermediateObjectArray(path)
	op := b.determineGroupOp([]string{path}, lcaPath, counts)
	return conditionGroup{op: op, lcaPath: lcaPath, paths: []string{path}}
}

// commonObjectArrayLCA returns the deepest ObjectArray in the common prefix of
// all segs. For a single path it delegates to lastIntermediateObjectArray.
func (b *executionPlanBuilder) commonObjectArrayLCA(segs [][]string, paths []string) string {
	if len(paths) == 1 {
		return b.lastIntermediateObjectArray(paths[0])
	}
	// Find the common prefix length across all segs.
	lcaLen := len(segs[0])
	for _, s := range segs[1:] {
		i := 0
		for i < lcaLen && i < len(s) && segs[0][i] == s[i] {
			i++
		}
		lcaLen = i
	}
	if lcaLen == 0 {
		return ""
	}
	return b.lastIntermediateObjectArray(filnested.JoinPath(segs[0][:lcaLen]))
}

// determineGroupOp picks the combining operation for a group:
//
//  1. Any masked path (tokens+independent or multiple independents) → leaf bits
//     cannot be used for same-element detection via raw AndAll.
//     - lcaPath!="" → groupRunIdxLoop (idx entries verify same intermediate element)
//     - lcaPath==""  → groupAndAllMaskLeaf (root_idx encodes element identity)
//
//  2. All single conditions but some path passes through a further object[]
//     beyond the group LCA → leaf positions are not naturally aligned.
//     Same routing as above.
//
//  3. All single conditions, no further object[] in any rem → positions are
//     naturally aligned; raw AndAll on leaf-precise positions is sufficient.
//
// Why groupAndAllMaskLeaf is never safe when lcaPath!="":
//
// root_idx always encodes the ROOT property's element index; leaf_idx is a
// depth-first counter within that root element. Masking leaf bits collapses
// all positions within the same root element to {root_idx, leaf=0, docID},
// regardless of which intermediate array element they came from.
//
// Example — garages (root object[]), cars (intermediate object[]):
//
//	garages[0].cars[0].tags[0] = "german" → {root=1, leaf=1, doc=D}
//	garages[0].cars[1].tags[0] = "electric" → {root=1, leaf=2, doc=D}
//	MaskLeaf both → {root=1, leaf=0, doc=D} for each
//	AndAllMaskLeaf → {root=1, leaf=0, doc=D}  ← false positive
//
// The two tags are in different cars, but the same garages element. Masking
// the leaf loses the cars-level distinction entirely. Only the idx loop can
// verify that all conditions land within the same intermediate element, so
// groupRunIdxLoop is required for any lcaPath!="".
func (b *executionPlanBuilder) determineGroupOp(paths []string, lcaPath string, counts conditionCounts) groupOp {
	for _, p := range paths {
		if counts.isMasked(p) || b.lastIntermediateObjectArray(p) != lcaPath {
			if lcaPath != "" {
				return groupRunIdxLoop
			}
			return groupAndAllMaskLeaf
		}
	}
	return groupAndAll
}

// lastIntermediateObjectArray walks the nested property schema along path and
// returns the dot-notation prefix up to and including the LAST (deepest)
// segment whose DataType is DataTypeObjectArray, or "" if none exists.
//
// Examples:
//
//	"cars.tags"         (cars=object[], tags=text[])                   → "cars"
//	"garages.cars.tags" (garages=object[], cars=object[], tags=text[]) → "garages.cars"
//	"make"              (text, no intermediate array)                  → ""
func (b *executionPlanBuilder) lastIntermediateObjectArray(path string) string {
	segs := filnested.SplitPath(path)
	props := b.props
	last := ""
	for i, seg := range segs {
		np := filnested.FindNestedProp(props, seg)
		if np == nil {
			return last
		}
		dt := schema.DataType(np.DataType[0])
		if dt == schema.DataTypeObjectArray {
			last = filnested.JoinPath(segs[:i+1])
		}
		if !schema.IsNested(dt) {
			return last
		}
		props = np.NestedProperties
	}
	return last
}

// conditionCounts maps each relPath to its [tokens, independents] condition counts.
type conditionCounts map[string][2]int

// isMasked reports whether the conditions for path will produce a leaf-masked
// bitmap from the combining step: independents>1 OR (tokens>0 AND independents>0).
func (c conditionCounts) isMasked(path string) bool {
	tok, ind := c[path][0], c[path][1]
	return ind > 1 || (tok > 0 && ind > 0)
}
