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

	"github.com/weaviate/weaviate/entities/filters"
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
	// lcaPath returns the node's logical LCA scope. Used by parents to compute
	// their own LCA (e.g. recOrNode's deepest-common-LCA across children) and
	// for plan-shape tests.
	lcaPath() string
}

type recGroupNode struct {
	lca  string
	here []*propValuePair
	subs []recPlanNode
}

type recSplitNode struct {
	lca      string
	branches []recSplitBranch
}

type recSplitBranch struct {
	index int
	plan  recPlanNode
}

// recOrNode is a position-level OR over arbitrary recPlanNode children.
// Each child evaluates raw at its own lcaPath; the OR returns the raw
// union. The lcaPath field is the deepest common ancestor of the
// children's lcaPaths and serves as the OR result's logical scope for
// parent-side combining. Not yet produced by the planner in production
// (step 5 dispatch refactor will enable that); validated by unit tests.
type recOrNode struct {
	lca      string
	children []recPlanNode
}

// recNotNode inverts its operand at the operand's natural LCA against
// the per-LCA universe (`_exists.{lca}`). pins, when non-empty, restrict
// the universe to the specified arr[N] slice — for example, pins for
// `NOT cars.tires[1].width=205` are `[{path:"tires", idx:1}]`, so the
// inversion universe is `_exists.cars.tires ∩ _idx.cars.tires[1]`.
// Not yet produced by the planner in production.
type recNotNode struct {
	lca     string
	pins    arrayIndices
	operand recPlanNode
}

func (*recGroupNode) isRecPlanNode() {}
func (*recSplitNode) isRecPlanNode() {}
func (*recOrNode) isRecPlanNode()    {}
func (*recNotNode) isRecPlanNode()   {}

func (g *recGroupNode) lcaPath() string { return g.lca }
func (s *recSplitNode) lcaPath() string { return s.lca }
func (o *recOrNode) lcaPath() string    { return o.lca }
func (n *recNotNode) lcaPath() string   { return n.lca }

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
			lca: scope,
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
	return &recSplitNode{lca: scope, branches: branches}
}

// buildGroup partitions items at scope into here (terminating at the scope
// LCA), one sub-plan per next-deeper ObjectArray (for leaf items), and one
// sub-plan per OR/NOT operator item (planned recursively from its natural
// scope). An empty here with a single sub collapses to the sub directly so
// trees stay tight.
func (b *recPlanBuilder) buildGroup(items []*propValuePair, scope string) recPlanNode {
	var here []*propValuePair
	subsByPath := map[string][]*propValuePair{}
	var operatorSubs []recPlanNode
	for _, it := range items {
		switch it.operator {
		case filters.OperatorOr:
			operatorSubs = append(operatorSubs, b.buildOr(it))
			continue
		case filters.OperatorNot:
			operatorSubs = append(operatorSubs, b.buildNot(it))
			continue
		default:
			// Fall through to leaf-or-correlated-AND path below.
		}
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

	subs := make([]recPlanNode, 0, len(nextPaths)+len(operatorSubs))
	for _, p := range nextPaths {
		subs = append(subs, b.buildPlan(subsByPath[p], p))
	}
	// Operator subs (OR/NOT) appear after path-grouped subs. Stable order
	// preserves the user-written filter sequence for plan-shape tests.
	subs = append(subs, operatorSubs...)

	if len(here) == 0 && len(subs) == 1 && !b.needsWrappingGroup(scope, subs[0]) {
		return subs[0]
	}
	return &recGroupNode{lca: scope, here: here, subs: subs}
}

// buildOr plans each child of an OR operator at its own natural scope and
// wraps the children in a recOrNode whose lca is the deepest common
// ancestor of the children's lcas. Filter validation rejects OR with zero
// children; we assert defensively to surface validation gaps loudly. See
// step 7 (validation) in the position-level eval implementation plan.
func (b *recPlanBuilder) buildOr(orPv *propValuePair) recPlanNode {
	if len(orPv.children) == 0 {
		panic("recPlanBuilder.buildOr: OR with zero children — filter validation gap")
	}
	childPlans := make([]recPlanNode, 0, len(orPv.children))
	childLCAs := make([]string, 0, len(orPv.children))
	for _, c := range orPv.children {
		plan := b.planSingleItem(c)
		childPlans = append(childPlans, plan)
		childLCAs = append(childLCAs, plan.lcaPath())
	}
	return &recOrNode{lca: deepestCommonLCA(childLCAs), children: childPlans}
}

// buildNot plans the single operand of a NOT operator and wraps it in a
// recNotNode whose lca is the operand's natural LCA. arr[N] pins are
// lifted from the operand for universe restriction during evaluation —
// today this is best-effort: leaf operands' arrayIndices are propagated
// directly, and compound operands (e.g. NOT(A AND B)) leave pins empty
// (universe unrestricted; correct but loose).
//
// Filter validation rejects NOT with !=1 children; we assert defensively.
// See step 7 (validation) for the broader operator-arity check.
func (b *recPlanBuilder) buildNot(notPv *propValuePair) recPlanNode {
	if len(notPv.children) != 1 {
		panic(fmt.Sprintf("recPlanBuilder.buildNot: NOT with %d children — filter validation gap", len(notPv.children)))
	}
	operand := notPv.children[0]
	plan := b.planSingleItem(operand)
	return &recNotNode{
		lca:     plan.lcaPath(),
		pins:    liftArrayIndicesFromOperand(operand),
		operand: plan,
	}
}

// planSingleItem plans one item from the root scope. Used by buildOr /
// buildNot to plan their child operands without inheriting the parent's
// scope (each operand's plan must reflect its own path's natural scope).
func (b *recPlanBuilder) planSingleItem(it *propValuePair) recPlanNode {
	return b.buildPlan([]*propValuePair{it}, "")
}

// liftArrayIndicesFromOperand extracts arr[N] pins from a NOT operand
// for universe restriction. For a leaf operand the pins come from the
// operand's nested.arrayIndices directly. For a compound operand
// (sub-operator like AND/OR/NOT), pin handling is deferred — returning
// empty falls back to an unrestricted universe at evaluation time, which
// is correct but may evaluate over more positions than strictly needed.
//
// TODO aliszka:nested_filtering: extend lift to compound operands when
// all leaves share a common pin (e.g. NOT(cars[1].make=tesla AND
// cars[1].color=red) could lift pins=[{path:"", idx:1}]). Today leaves
// this as the unrestricted universe.
func liftArrayIndicesFromOperand(pv *propValuePair) arrayIndices {
	if pv.nested.isNested {
		return pv.nested.arrayIndices
	}
	return nil
}

// deepestCommonLCA returns the longest dot-segment-aligned common prefix
// of the provided LCA paths. Used by buildOr to compute its lca from
// children's lcas. Returns "" if the list is empty, no common prefix
// exists, or any input is "".
//
// Examples:
//
//	deepestCommonLCA(["cars.tires", "cars"])                = "cars"
//	deepestCommonLCA(["cars.tires", "cars.accessories"])   = "cars"
//	deepestCommonLCA(["cars.tires", "garages.cars"])       = ""
//	deepestCommonLCA(["cars.tires", "cars.tires"])         = "cars.tires"
//	deepestCommonLCA(["cars.tires.brand", "cars", "cars.colors"]) = "cars"
func deepestCommonLCA(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	segs := filnested.SplitPath(paths[0])
	for _, p := range paths[1:] {
		other := filnested.SplitPath(p)
		// Truncate segs to the common prefix with other.
		limit := len(segs)
		if len(other) < limit {
			limit = len(other)
		}
		i := 0
		for i < limit && segs[i] == other[i] {
			i++
		}
		segs = segs[:i]
		if len(segs) == 0 {
			return ""
		}
	}
	return filnested.JoinPath(segs)
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
func (b *recPlanBuilder) needsWrappingGroup(scope string, sub recPlanNode) bool {
	if scope == "" {
		return false
	}
	switch n := sub.(type) {
	case *recSplitNode:
		if len(n.branches) > 1 {
			return true
		}
		// 1-branch SPLIT pins to one K at its lcaPath but doesn't disambiguate
		// that K across different parents at the same level. If the branch's
		// plan itself needs outer scope (e.g. its descendants dispatch through
		// runIdxLoopRecursive), the parent-level wrap must be kept so the
		// outer iteration provides per-parent disambiguation.
		return b.needsWrappingGroup(scope, n.branches[0].plan)
	case *recGroupNode:
		return b.groupSubtreeNeedsOuterScope(n)
	}
	return false
}

// groupSubtreeNeedsOuterScope reports whether g (or any descendant
// recGroupNode in g's subtree) will use runIdxLoopRecursive — which requires
// the immediate ancestor's _idx scope to disambiguate same-K-different-parent
// physical instances. A non-root recGroupNode uses runIdxLoopRecursive when
// neither canUseRawAndAll nor collectFlatSubtree succeeds:
//
//   - It has ≥2 subs (the flat raw-AndAll path bails on multi-sub since
//     sibling sub-arrays produce conditions at distinct leaves with no
//     inheritance bridge).
//   - It has duplicate here paths (same-path values land at distinct leaves
//     per walkScalarArray, so canUseRawAndAll is false and the flat path
//     bails). Same-element semantics for these requires per-element _idx
//     iteration scoped by the outer parent.
//   - It has ≥2 scalar-array (text[], int[], …) here paths — two scalar
//     arrays never share a leaf even within the same parent element, so
//     canUseRawAndAll bails and collectFlatSubtree bails on scalar-array.
//   - It has ≥1 scalar-array here AND ≥1 sub — collectFlatSubtree bails on
//     scalar-array, canUseRawAndAll bails on subs.
func (b *recPlanBuilder) groupSubtreeNeedsOuterScope(g *recGroupNode) bool {
	if g.lca != "" && (len(g.subs) >= 2 || hasDuplicateHerePaths(g) || b.groupNeedsIdxLoop(g)) {
		return true
	}
	for _, sub := range g.subs {
		if grp, ok := sub.(*recGroupNode); ok {
			if b.groupSubtreeNeedsOuterScope(grp) {
				return true
			}
		}
		// recSplitNode descendants handle their own scoping at their level
		// via needsWrappingGroup invocations during their own buildPlan.
	}
	return false
}

// groupNeedsIdxLoop reports whether g (considered in isolation, not its
// descendants) will dispatch to runIdxLoopRecursive due to scalar-array
// terminals among its here paths. Two cases:
//
//   - ≥2 scalar-array here paths: canUseRawAndAll bails (after counting
//     scalar-arrays); collectFlatSubtree bails on the first scalar-array.
//   - ≥1 scalar-array here AND ≥1 sub: canUseRawAndAll bails on subs;
//     collectFlatSubtree bails on the scalar-array.
//
// A single scalar-array here with no subs goes through canUseRawAndAll's raw
// AndAll path (works via Phase-3 inheritance with regular scalars) — no idx
// loop, no outer scope needed.
func (b *recPlanBuilder) groupNeedsIdxLoop(g *recGroupNode) bool {
	scalarArrays := 0
	for _, leaf := range g.here {
		if pathTerminalIsScalarArray(b.props, childRelPath(leaf)) {
			scalarArrays++
			if scalarArrays >= 2 {
				return true
			}
		}
	}
	return scalarArrays >= 1 && len(g.subs) >= 1
}

// pathTerminalIsScalarArray reports whether path's terminal property in the
// given nested schema is a scalar-array type (text[], int[], number[],
// boolean[], date[], uuid[]). Shared by recPlanBuilder and recExecutor so
// planning and execution stay in sync on scalar-array detection.
func pathTerminalIsScalarArray(props []*models.NestedProperty, path string) bool {
	if path == "" {
		return false
	}
	segs := filnested.SplitPath(path)
	for i, seg := range segs {
		np := filnested.FindNestedProp(props, seg)
		if np == nil {
			return false
		}
		dt := schema.DataType(np.DataType[0])
		if i == len(segs)-1 {
			return schema.IsScalarArrayType(dt)
		}
		if !schema.IsNested(dt) {
			return false
		}
		props = np.NestedProperties
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
			out[t.lca] = struct{}{}
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
	lines := []string{fmt.Sprintf("GROUP lcaPath=%q", g.lca)}
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
		fmt.Sprintf("SPLIT lcaPath=%q", s.lca),
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

func (o *recOrNode) describeLines() []string {
	lines := []string{
		fmt.Sprintf("OR lcaPath=%q", o.lca),
		"  children:",
	}
	for _, c := range o.children {
		for _, l := range c.describeLines() {
			lines = append(lines, "    "+l)
		}
	}
	return lines
}

func (n *recNotNode) describeLines() []string {
	pinStrs := make([]string, 0, len(n.pins))
	for _, p := range n.pins {
		pinStrs = append(pinStrs, fmt.Sprintf("%s[%d]", p.RelPath, p.Index))
	}
	lines := []string{
		fmt.Sprintf("NOT lcaPath=%q pins=[%s]", n.lca, strings.Join(pinStrs, ",")),
		"  operand:",
	}
	for _, l := range n.operand.describeLines() {
		lines = append(lines, "    "+l)
	}
	return lines
}
