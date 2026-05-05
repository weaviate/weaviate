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
	"sort"
	"strings"

	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// recPlanNode is the recursive plan tree returned by recPlanBuilder. The two
// concrete shapes describe how the executor should combine condition bitmaps:
//
//   - recGroupNode collects conditions that share a common ObjectArray LCA at
//     lcaPath. here = conditions whose path terminates at the LCA (no further
//     object[] segment). subs = recursive plans for sub-paths grouped by their
//     next-deeper object[] segment.
//
//   - recSplitNode dispatches an arr[N] constraint at lcaPath. Each branch
//     pins to a single index value and contains the plan for the items that
//     carry that constraint at this scope (plus any unconstrained items that
//     are merged in when the branch is the only constrained bucket).
type recPlanNode interface {
	isRecPlanNode()
	describeLines() []string
}

type recGroupNode struct {
	lcaPath string
	here    []*propValuePair
	subs    []recPlanNode
}

type recSplitNode struct {
	lcaPath  string
	branches []recSplitBranch
}

type recSplitBranch struct {
	index int
	plan  recPlanNode
}

func (*recGroupNode) isRecPlanNode() {}
func (*recSplitNode) isRecPlanNode() {}

// recPlanBuilder builds a recPlanNode tree from the children of an
// isCorrelated AND. Only the root property's nested schema is required; all
// per-condition state lives on the propValuePair children.
type recPlanBuilder struct {
	props []*models.NestedProperty
}

func newRecPlanBuilder(props []*models.NestedProperty) *recPlanBuilder {
	return &recPlanBuilder{props: props}
}

// build is the entry point. children are the conditions of an isCorrelated
// AND. The returned plan starts from the root property scope ("").
func (b *recPlanBuilder) build(children []*propValuePair) recPlanNode {
	return b.buildPlan(children, "")
}

// buildPlan dispatches an arr[N] constraint at scope when needed, otherwise
// recurses into a group at scope.
//
// Bucketing rules at a single scope:
//   - 0 constrained buckets → no split needed; buildGroup at scope.
//   - 1 constrained bucket  → 1-branch split, with unconstrained items merged
//     into the single branch.
//   - ≥2 constrained buckets → pure split, one branch per index. Validation
//     rejects unconstrained items mixed with multiple constrained buckets;
//     until validation lands, any unconstrained items in this configuration
//     are dropped on the floor here (TODO aliszka:nested_filtering — wire up
//     validation in entities/filters/filters_validator.go).
func (b *recPlanBuilder) buildPlan(items []*propValuePair, scope string) recPlanNode {
	var unconstrained []*propValuePair
	constrained := map[int][]*propValuePair{}
	var keyOrder []int

	for _, it := range items {
		idx, ok := constraintAtScope(scope, childArrayIndices(it))
		if !ok {
			unconstrained = append(unconstrained, it)
			continue
		}
		if _, exists := constrained[idx]; !exists {
			keyOrder = append(keyOrder, idx)
		}
		constrained[idx] = append(constrained[idx], it)
	}

	if len(constrained) == 0 {
		return b.buildGroup(items, scope)
	}
	sort.Ints(keyOrder)
	if len(constrained) == 1 {
		idx := keyOrder[0]
		merged := append([]*propValuePair{}, constrained[idx]...)
		merged = append(merged, unconstrained...)
		return &recSplitNode{
			lcaPath: scope,
			branches: []recSplitBranch{{
				index: idx,
				plan:  b.buildGroup(merged, scope),
			}},
		}
	}

	branches := make([]recSplitBranch, 0, len(keyOrder))
	for _, idx := range keyOrder {
		branches = append(branches, recSplitBranch{
			index: idx,
			plan:  b.buildGroup(constrained[idx], scope),
		})
	}
	return &recSplitNode{lcaPath: scope, branches: branches}
}

// buildGroup partitions items at scope into here (terminating at the scope
// LCA) and one sub-plan per next-deeper ObjectArray. An empty here with a
// single sub collapses to the sub directly so trees stay tight.
func (b *recPlanBuilder) buildGroup(items []*propValuePair, scope string) recPlanNode {
	var here []*propValuePair
	subsByPath := map[string][]*propValuePair{}
	for _, it := range items {
		rp := childRelPath(it)
		if b.lastIntermediateObjectArray(rp) == scope {
			here = append(here, it)
			continue
		}
		next := b.nextObjectArrayAfter(scope, rp)
		subsByPath[next] = append(subsByPath[next], it)
	}

	sort.Slice(here, func(i, j int) bool {
		return childRelPath(here[i]) < childRelPath(here[j])
	})

	nextPaths := make([]string, 0, len(subsByPath))
	for p := range subsByPath {
		nextPaths = append(nextPaths, p)
	}
	sort.Strings(nextPaths)

	subs := make([]recPlanNode, 0, len(nextPaths))
	for _, p := range nextPaths {
		subs = append(subs, b.buildPlan(subsByPath[p], p))
	}

	if len(here) == 0 && len(subs) == 1 && !needsWrappingGroup(scope, subs[0]) {
		return subs[0]
	}
	return &recGroupNode{lcaPath: scope, here: here, subs: subs}
}

// needsWrappingGroup reports whether the wrapping GROUP at scope must be kept
// even when there are no here conditions and only a single sub. The wrapping
// GROUP's per-element loop (runIdxLoopRecursive at intermediate scope) is what
// enforces same-element semantics around constructs that need an outer scope:
//
//   - Multi-branch SPLIT — collapsing the wrapper away would let the SPLIT
//     combiner AND branches at rootDoc only, losing the "same element at the
//     LCA above the conflict" guarantee.
//   - GROUP whose subtree contains a non-root multi-sub GROUP — that deeper
//     GROUP uses runIdxLoopRecursive (the flat path bails on multi-sub) and
//     iterates `_idx.{lca}[K]` keys that aggregate across all parents at the
//     intermediate level. Without the wrapping GROUP at the parent's scope
//     to provide parentScope, the K bucket conflates physical instances at
//     the same K under different parents (case-3 same-K-different-parent bug).
//
// Returns false at root scope: root-level same-element is handled implicitly
// by root_idx in the position encoding, no outer loop needed.
func needsWrappingGroup(scope string, sub recPlanNode) bool {
	if scope == "" {
		return false
	}
	switch n := sub.(type) {
	case *recSplitNode:
		return len(n.branches) > 1
	case *recGroupNode:
		return groupSubtreeNeedsOuterScope(n)
	}
	return false
}

// groupSubtreeNeedsOuterScope reports whether g (or any descendant
// recGroupNode in g's subtree) will use runIdxLoopRecursive — which requires
// the immediate ancestor's _idx scope to disambiguate same-K-different-parent
// physical instances. A non-root recGroupNode uses runIdxLoopRecursive when:
//
//   - It has ≥2 subs (the flat raw-AndAll path bails on multi-sub since
//     sibling sub-arrays produce conditions at distinct leaves with no
//     inheritance bridge).
//   - It has duplicate here paths (same-path values land at distinct leaves
//     per walkScalarArray, so canUseRawAndAll is false and the flat path
//     bails). Same-element semantics for these requires per-element _idx
//     iteration scoped by the outer parent.
func groupSubtreeNeedsOuterScope(g *recGroupNode) bool {
	if g.lcaPath != "" && (len(g.subs) >= 2 || hasDuplicateHerePaths(g)) {
		return true
	}
	for _, sub := range g.subs {
		if grp, ok := sub.(*recGroupNode); ok {
			if groupSubtreeNeedsOuterScope(grp) {
				return true
			}
		}
		// recSplitNode descendants handle their own scoping at their level
		// via needsWrappingGroup invocations during their own buildPlan.
	}
	return false
}

// hasDuplicateHerePaths reports whether two or more here entries in g share
// the same childRelPath. Each text[]/scalar-array value lives at a distinct
// leaf assigned by walkScalarArray, so duplicate-path filters cannot match
// at the same leaf via raw AndAll — they need per-element evaluation.
func hasDuplicateHerePaths(g *recGroupNode) bool {
	if len(g.here) < 2 {
		return false
	}
	seen := make(map[string]struct{}, len(g.here))
	for _, leaf := range g.here {
		path := childRelPath(leaf)
		if _, dup := seen[path]; dup {
			return true
		}
		seen[path] = struct{}{}
	}
	return false
}

// constraintAtScope returns the arr[N] index whose RelPath == scope, if any.
// arr[N] entries are unique by RelPath (each indexed array can appear at most
// once in a single path), so the first match is the only match.
func constraintAtScope(scope string, indices arrayIndices) (int, bool) {
	for _, idx := range indices {
		if idx.RelPath == scope {
			return idx.Index, true
		}
	}
	return 0, false
}

// lastIntermediateObjectArray walks the nested schema along path and returns
// the deepest object[] segment, or "" if none exists. Mirrors the helper in
// the older executionPlanBuilder so both planners agree on LCA semantics.
func (b *recPlanBuilder) lastIntermediateObjectArray(path string) string {
	if path == "" {
		return ""
	}
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

// nextObjectArrayAfter returns the first object[] segment in path that is
// strictly deeper than scope, or "" if none exists. With scope="garages" and
// path="garages.cars.tires.width" it returns "garages.cars".
func (b *recPlanBuilder) nextObjectArrayAfter(scope, path string) string {
	segs := filnested.SplitPath(path)
	var startIdx int
	if scope != "" {
		startIdx = len(filnested.SplitPath(scope))
	}
	props := b.props
	for i, seg := range segs {
		np := filnested.FindNestedProp(props, seg)
		if np == nil {
			return ""
		}
		dt := schema.DataType(np.DataType[0])
		if i >= startIdx && dt == schema.DataTypeObjectArray {
			return filnested.JoinPath(segs[:i+1])
		}
		if !schema.IsNested(dt) {
			return ""
		}
		props = np.NestedProperties
	}
	return ""
}

// collectPlanLCAs walks node and returns the set of recGroupNode lcaPaths.
// Used by recExecutor to decide which excludes are consumed inside groups
// (per §8.5) and which fall through to the rootDoc subtraction in execute().
// recSplitNode lcaPaths are not collected — splits don't apply excludes; their
// branches recurse into recGroupNode children which contribute lcaPaths.
func collectPlanLCAs(node recPlanNode) map[string]struct{} {
	out := map[string]struct{}{}
	var walk func(n recPlanNode)
	walk = func(n recPlanNode) {
		switch t := n.(type) {
		case *recGroupNode:
			out[t.lcaPath] = struct{}{}
			for _, sub := range t.subs {
				walk(sub)
			}
		case *recSplitNode:
			for _, br := range t.branches {
				walk(br.plan)
			}
		}
	}
	if node != nil {
		walk(node)
	}
	return out
}

// describePlan renders the plan as a multi-line string for structural unit
// tests. Each level adds two spaces of indent; a node's first line carries
// the node kind and lcaPath, with subsequent lines listing here paths and
// nested subs/branches.
func describePlan(node recPlanNode) string {
	return strings.Join(node.describeLines(), "\n")
}

func (g *recGroupNode) describeLines() []string {
	lines := []string{fmt.Sprintf("GROUP lcaPath=%q", g.lcaPath)}
	paths := make([]string, len(g.here))
	for i, c := range g.here {
		paths[i] = childRelPath(c)
	}
	lines = append(lines, fmt.Sprintf("  here=[%s]", strings.Join(paths, ", ")))
	if len(g.subs) == 0 {
		lines = append(lines, "  subs=[]")
		return lines
	}
	lines = append(lines, "  subs:")
	for _, s := range g.subs {
		for _, l := range s.describeLines() {
			lines = append(lines, "    "+l)
		}
	}
	return lines
}

func (s *recSplitNode) describeLines() []string {
	lines := []string{
		fmt.Sprintf("SPLIT lcaPath=%q", s.lcaPath),
		"  branches:",
	}
	for _, br := range s.branches {
		lines = append(lines, fmt.Sprintf("    index=%d", br.index))
		for _, l := range br.plan.describeLines() {
			lines = append(lines, "      "+l)
		}
	}
	return lines
}
