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
	"context"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nestedlegacy"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

// recGroupInput is the normalized form fed to the recursive plan + executor.
// Tokenization wrappers have been collapsed into single virtual leaves; IsNull
// leaves are materialized as strict-existential positives at their operand
// LCA (correlated-AND IsNull alignment).
type recGroupInput struct {
	// positives are the *propValuePair entries the recursive planner consumes
	// as logical leaves. Each entry has a corresponding rawsByCond bitmap.
	positives []*propValuePair
	// rawsByCond maps each positive to its raw position bitmap. For a
	// tokenization wrapper the bitmap is the AndAll of all token bitmaps.
	// For an IsNull=true leaf the bitmap is _exists.{operandLCA} AndNot
	// _exists.{relPath} (strict-existential at the operand LCA).
	rawsByCond map[*propValuePair]*sroar.Bitmap
	// releases holds cleanup callbacks for every bitmap acquired by the
	// normalizer (raw positions, AndAll temporaries). The caller must invoke
	// them after the executor has finished.
	releases []func()
}

// recBitmapFetcher abstracts the bitmap acquisition step so the normalizer can
// be exercised in tests with synthetic bitmaps. The production implementation
// reads from the Searcher's value and meta buckets via fetchNestedPositions /
// fetchNestedExistsPositions.
type recBitmapFetcher interface {
	// fetchValue returns the raw position bitmap for a value-equals leaf,
	// already restricted by any arr[N] indices on the leaf.
	fetchValue(ctx context.Context, leaf *propValuePair) (*sroar.Bitmap, func(), error)
	// fetchExists returns the raw _exists.{relPath} bitmap, restricted by any
	// arr[N] indices on the leaf. Used for IsNull=false (positive existence
	// leaf) and as the operand half of strict-existential IsNull=true.
	fetchExists(leaf *propValuePair) (*sroar.Bitmap, func(), error)
	// fetchExistsAtPath returns the raw _exists.{path} bitmap, restricted by
	// the leaf's arr[N] indices. Used to materialize strict-existential IsNull
	// inside correlated AND: caller AndNots _exists.{relPath} from
	// _exists.{operandLCA} to obtain "∃ LCA-element without the operand."
	fetchExistsAtPath(leaf *propValuePair, path string) (*sroar.Bitmap, func(), error)
}

// normalizeRecGroup walks the group's children and builds the recursive input.
// It is decoupled from the Searcher via recBitmapFetcher so unit tests can
// inject synthetic bitmaps. Token wrappers are AndAll-collapsed; IsNull=false
// becomes a positive existence leaf; IsNull=true is materialized as a
// strict-existential positive at the operand's LCA (correlated-AND IsNull alignment).
//
// The two tokenization patterns are both handled:
//
//   - Outer pv carries childrenFromTokenization=true: every direct child is a
//     token of the same value at the same path. They are AndAll'd into one
//     virtual leaf bitmap and the first child stands in as the planner key.
//
//   - Inner child wraps tokens (child.nested.isNested == false, grandchildren
//     are tokens): the wrapper is the planner key and rawsByCond[wrapper] is
//     the AndAll of grandchildren bitmaps.
//
// The function takes ownership of every bitmap it acquires and emits matching
// release callbacks via input.releases. On any error every acquired bitmap is
// released before returning.
func normalizeRecGroup(
	ctx context.Context,
	pv *propValuePair,
	children []*propValuePair,
	fetcher recBitmapFetcher,
	bitmapOps *invnested.BitmapOps,
	maxConcurrency int,
) (*recGroupInput, error) {
	input := &recGroupInput{
		rawsByCond: make(map[*propValuePair]*sroar.Bitmap, len(children)),
	}
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range input.releases {
				rel()
			}
		}
	}()

	// Pattern 1: outer pv is a tokenization wrapper. All direct children are
	// tokens of the same value. AndAll them into a single virtual leaf.
	if pv.nested.childrenFromTokenization {
		if len(children) == 0 {
			return nil, fmt.Errorf("normalizeRecGroup: tokenization wrapper has no children")
		}
		combined, releases, err := fetchAndAndAllTokens(ctx, children, fetcher, bitmapOps, maxConcurrency)
		if err != nil {
			return nil, err
		}
		input.releases = append(input.releases, releases...)
		input.positives = append(input.positives, children[0])
		input.rawsByCond[children[0]] = combined
		succeeded = true
		return input, nil
	}

	// Pattern 2 (and the mainstream non-tokenized case): each child is one of
	//   - direct nested leaf (isNested): routeDirectLeaf.
	//   - tokenization wrapper (childrenFromTokenization=true): AndAll the
	//     token grandchildren into one virtual leaf bitmap.
	//   - operator subtree (AND/OR/NOT containing nested leaves) introduced
	//     by recursive same-root grouping: recursively pre-fetch a value
	//     bitmap for every nested leaf inside the subtree. The operator pvp
	//     itself is added to positives so the planner sees it and builds
	//     recOrNode / recNotNode / nested recGroupNode as needed.
	for _, child := range children {
		if child.nested.isNested {
			if err := routeDirectLeaf(ctx, child, fetcher, bitmapOps, input); err != nil {
				return nil, err
			}
			continue
		}
		if len(child.children) == 0 {
			return nil, fmt.Errorf("normalizeRecGroup: non-nested child %p has no grandchildren", child)
		}
		if child.nested.childrenFromTokenization {
			combined, releases, err := fetchAndAndAllTokens(ctx, child.children, fetcher, bitmapOps, maxConcurrency)
			if err != nil {
				return nil, err
			}
			input.releases = append(input.releases, releases...)
			input.positives = append(input.positives, child)
			input.rawsByCond[child] = combined
			continue
		}
		switch child.operator {
		case filters.OperatorAnd, filters.OperatorOr, filters.OperatorNot,
			filters.ContainsAll, filters.ContainsAny:
			// ContainsAll / ContainsAny are AND / OR aliases on nested
			// paths (first-class-operator approach); same fetch path as OperatorAnd/Or — the
			// planner reads them as operator subtrees and either flattens
			// (ContainsAll → AND) or plans an OR scope (ContainsAny).
			if err := fetchOperatorSubtreeBitmaps(ctx, child, fetcher, bitmapOps, maxConcurrency, input); err != nil {
				return nil, err
			}
			input.positives = append(input.positives, child)
		case filters.ContainsNone:
			// First-class ContainsNone inside a correlated AND. Materialize
			// the strict-existential result (universe at operand path AndNot
			// OR-of-value-bitmaps) as a raw position bitmap, stored in
			// rawsByCond keyed by the ContainsNone pvp. The planner places
			// the pvp at its natural LCA (via childRelPath → operand path)
			// and the executor correlates it with sibling leaves at root
			// scope via the existing same-element AndAll path.
			if err := materializeNestedContainsNone(ctx, child, fetcher, bitmapOps, maxConcurrency, input); err != nil {
				return nil, err
			}
			input.positives = append(input.positives, child)
		default:
			return nil, fmt.Errorf("normalizeRecGroup: unsupported non-nested child operator %q", child.operator.Name())
		}
	}

	succeeded = true
	return input, nil
}

// materializeNestedContainsNone computes a ContainsNone leaf's strict-
// existential bitmap for use inside a correlated AND or operator subtree.
// Mirrors fetchNestedContainsNone's algorithm but builds the result inline
// against the recBitmapFetcher abstraction so it composes with the rest of
// normalizeRecGroup. Children may be value leaves or tokenization wrappers
// (multi-token text values).
func materializeNestedContainsNone(
	ctx context.Context,
	pv *propValuePair,
	fetcher recBitmapFetcher,
	bitmapOps *invnested.BitmapOps,
	maxConcurrency int,
	input *recGroupInput,
) error {
	if len(pv.children) == 0 {
		return fmt.Errorf("materializeNestedContainsNone: no values for %q", pv.prop)
	}

	// Operand: OR over each forbidden value's position bitmap. Multi-token
	// text values AndAll their tokens at the leaf level (same as standalone
	// fetchNestedContainsNone) before contributing to the OR.
	perValue := make([]*sroar.Bitmap, 0, len(pv.children))
	for _, child := range pv.children {
		if child.nested.childrenFromTokenization {
			combined, releases, err := fetchAndAndAllTokens(ctx, child.children, fetcher, bitmapOps, maxConcurrency)
			if err != nil {
				return fmt.Errorf("materializeNestedContainsNone: tokens for %q: %w", pv.prop, err)
			}
			input.releases = append(input.releases, releases...)
			perValue = append(perValue, combined)
			continue
		}
		bm, rel, err := fetcher.fetchValue(ctx, child)
		if err != nil {
			return fmt.Errorf("materializeNestedContainsNone: fetch value for %q: %w", pv.prop, err)
		}
		input.releases = append(input.releases, rel)
		perValue = append(perValue, bm)
	}

	var operand *sroar.Bitmap
	if len(perValue) == 1 {
		operand = perValue[0]
	} else {
		var operandRel func()
		operand, operandRel = bitmapOps.OrAll(perValue, maxConcurrency)
		input.releases = append(input.releases, operandRel)
	}

	// Universe at the operand's scalar-array scope, restricted by any
	// arr[N] pins carried on the ContainsNone pvp itself.
	universe, universeRel, err := fetcher.fetchExistsAtPath(pv, pv.nested.relPath)
	if err != nil {
		return fmt.Errorf("materializeNestedContainsNone: fetch exists at %q: %w", pv.nested.relPath, err)
	}
	input.releases = append(input.releases, universeRel)

	result, resultRel := bitmapOps.AndNot(universe, operand, maxConcurrency)
	input.releases = append(input.releases, resultRel)
	input.rawsByCond[pv] = result
	return nil
}

// routeDirectLeaf classifies a single isNested child and appends to the right
// slice on input. IsNull=true → strict-existential positive (∃ LCA-element
// without operand, materialized as _exists.{operandLCA} AndNot
// _exists.{relPath}); IsNull=false → positive existence leaf; everything else
// → positive value leaf.
//
// The strict-existential materialization aligns correlated-AND IsNull with
// the standalone fetchNestedIsNull path: docs whose operand LCA is empty for
// the pinned scope no longer match vacuously — they must have at least one
// element at LCA where the operand is missing.
func routeDirectLeaf(ctx context.Context, leaf *propValuePair, fetcher recBitmapFetcher, bitmapOps *invnested.BitmapOps, input *recGroupInput) error {
	if leaf.operator == filters.OperatorIsNull {
		input.positives = append(input.positives, leaf)
		return materializeNestedIsNull(leaf, fetcher, bitmapOps, input)
	}

	bm, rel, err := fetcher.fetchValue(ctx, leaf)
	if err != nil {
		return fmt.Errorf("normalizeRecGroup: fetch value for %q: %w", leaf.nested.relPath, err)
	}
	input.releases = append(input.releases, rel)
	input.positives = append(input.positives, leaf)
	input.rawsByCond[leaf] = bm
	return nil
}

// materializeNestedIsNull computes the IsNull leaf's strict-existential
// bitmap (when IsNull=true) or positive existence bitmap (when IsNull=false)
// and stores it in input.rawsByCond keyed by the leaf. Used by both
// routeDirectLeaf (direct child of correlated AND) and
// fetchOperatorSubtreeBitmaps (IsNull buried inside an OR/NOT/inner-AND
// subtree). Does NOT touch input.positives — the caller decides whether the
// leaf is itself a planner entry.
//
// IsNull=true is materialized as _exists.{operandLCA} AndNot _exists.{relPath}
// — a strict-existential bitmap at the operand's natural LCA marking elements
// that have the field absent. IsNull=false reduces to the operand's own
// _exists.{relPath} bitmap (the field is present at those positions).
//
// Phase-3 inheritance makes the resulting bitmap compose correctly through
// OR / NOT siblings at the same operand LCA: scalars within a parent element
// share their parent's element-position encoding, so the existential bitmap
// and value-leaf bitmaps live at the same per-element leaf positions.
func materializeNestedIsNull(
	leaf *propValuePair,
	fetcher recBitmapFetcher,
	bitmapOps *invnested.BitmapOps,
	input *recGroupInput,
) error {
	bm, rel, err := fetcher.fetchExists(leaf)
	if err != nil {
		return fmt.Errorf("normalizeRecGroup: fetch exists for %q: %w", leaf.nested.relPath, err)
	}
	input.releases = append(input.releases, rel)
	isAbsent := len(leaf.value) > 0 && leaf.value[0] == 0x01
	if !isAbsent {
		input.rawsByCond[leaf] = bm
		return nil
	}
	lca, err := leaf.isNullOperandLCA()
	if err != nil {
		return fmt.Errorf("normalizeRecGroup: compute LCA for IsNull %q: %w", leaf.nested.relPath, err)
	}
	lcaBm, lcaRel, err := fetcher.fetchExistsAtPath(leaf, lca)
	if err != nil {
		return fmt.Errorf("normalizeRecGroup: fetch exists at LCA %q for IsNull on %q: %w", lca, leaf.nested.relPath, err)
	}
	input.releases = append(input.releases, lcaRel)
	existential, existRel := bitmapOps.AndNot(lcaBm, bm, concurrency.SROAR_MERGE)
	input.releases = append(input.releases, existRel)
	input.rawsByCond[leaf] = existential
	return nil
}

// fetchOperatorSubtreeBitmaps walks an AND/OR/NOT subtree rooted at node and
// pre-fetches a raw position bitmap for every nested leaf it reaches. Bitmaps
// land in input.rawsByCond keyed by the leaf pvp; the operator nodes
// themselves don't go to rawsByCond — the planner walks them via
// buildOr/buildNot/buildGroup and reads bitmaps for the leaves at evaluation.
//
// Tokenization wrappers (childrenFromTokenization=true) inside the subtree are
// treated as virtual leaves: their tokens are ANDed once into a combined
// bitmap and stored in rawsByCond keyed by the wrapper. This mirrors
// normalizeRecGroup Pattern 2 for direct AND-wrapped tokenization wrappers,
// so the planner sees the same shape regardless of how deeply the wrapper is
// nested under operator subtrees.
//
// IsNull leaves are materialized via materializeNestedIsNull (same path
// routeDirectLeaf uses), so the resulting bitmap composes with sibling
// value leaves through OR / NOT via Phase-3 inheritance.
func fetchOperatorSubtreeBitmaps(
	ctx context.Context,
	node *propValuePair,
	fetcher recBitmapFetcher,
	bitmapOps *invnested.BitmapOps,
	maxConcurrency int,
	input *recGroupInput,
) error {
	if node.nested.isNested {
		if node.operator == filters.OperatorIsNull {
			return materializeNestedIsNull(node, fetcher, bitmapOps, input)
		}
		bm, rel, err := fetcher.fetchValue(ctx, node)
		if err != nil {
			return fmt.Errorf("normalizeRecGroup: fetch value for %q: %w", node.nested.relPath, err)
		}
		input.releases = append(input.releases, rel)
		input.rawsByCond[node] = bm
		return nil
	}
	if node.nested.childrenFromTokenization {
		combined, releases, err := fetchAndAndAllTokens(ctx, node.children, fetcher, bitmapOps, maxConcurrency)
		if err != nil {
			return fmt.Errorf("normalizeRecGroup: combine tokenization wrapper inside operator subtree: %w", err)
		}
		input.releases = append(input.releases, releases...)
		input.rawsByCond[node] = combined
		return nil
	}
	switch node.operator {
	case filters.OperatorAnd, filters.OperatorOr, filters.OperatorNot,
		filters.ContainsAll, filters.ContainsAny:
		// ContainsAll / ContainsAny treated as AND / OR aliases (first-class-operator approach).
		for _, child := range node.children {
			if err := fetchOperatorSubtreeBitmaps(ctx, child, fetcher, bitmapOps, maxConcurrency, input); err != nil {
				return err
			}
		}
		return nil
	case filters.ContainsNone:
		// First-class ContainsNone inside an operator subtree (OR/NOT/AND).
		// Materialize the strict-existential bitmap inline, same as the
		// correlated-AND child case in normalizeRecGroup.
		return materializeNestedContainsNone(ctx, node, fetcher, bitmapOps, maxConcurrency, input)
	default:
		return fmt.Errorf("normalizeRecGroup: unsupported node in operator subtree (operator=%q, isNested=%v)", node.operator.Name(), node.nested.isNested)
	}
}

// fetchAndAndAllTokens fetches every token's raw position bitmap and ANDs them
// into a single bitmap (all tokens must share the same leaf position). When
// there is exactly one token the input bitmap is returned unmodified — its
// release callback is preserved in the returned slice.
func fetchAndAndAllTokens(
	ctx context.Context,
	tokens []*propValuePair,
	fetcher recBitmapFetcher,
	bitmapOps *invnested.BitmapOps,
	maxConcurrency int,
) (*sroar.Bitmap, []func(), error) {
	var releases []func()
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range releases {
				rel()
			}
		}
	}()

	bitmaps := make([]*sroar.Bitmap, 0, len(tokens))
	for _, tok := range tokens {
		bm, rel, err := fetcher.fetchValue(ctx, tok)
		if err != nil {
			return nil, nil, fmt.Errorf("fetchAndAndAllTokens: fetch token %q: %w", tok.nested.relPath, err)
		}
		releases = append(releases, rel)
		bitmaps = append(bitmaps, bm)
	}
	if len(bitmaps) == 1 {
		succeeded = true
		return bitmaps[0], releases, nil
	}
	combined, combinedRel := bitmapOps.AndAll(bitmaps, maxConcurrency)
	releases = append(releases, combinedRel)
	succeeded = true
	return combined, releases, nil
}

// searcherBitmapFetcher adapts a Searcher to recBitmapFetcher. It is the
// production implementation used by buildRecGroupExecutor.
type searcherBitmapFetcher struct {
	pv *propValuePair
	s  *Searcher
}

func (f *searcherBitmapFetcher) fetchValue(ctx context.Context, leaf *propValuePair) (*sroar.Bitmap, func(), error) {
	dbm, err := leaf.fetchNestedPositions(ctx, f.s)
	if err != nil {
		return nil, nil, err
	}
	return dbm.docIDs, dbm.release, nil
}

func (f *searcherBitmapFetcher) fetchExists(leaf *propValuePair) (*sroar.Bitmap, func(), error) {
	return leaf.fetchNestedExistsPositions(f.s)
}

func (f *searcherBitmapFetcher) fetchExistsAtPath(leaf *propValuePair, path string) (*sroar.Bitmap, func(), error) {
	metaBucket := f.s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(leaf.prop))
	if metaBucket == nil {
		return nil, nil, fmt.Errorf("fetchExistsAtPath: meta bucket for %q not found", leaf.prop)
	}
	positions, release, err := metaBucket.RoaringSetGet(invnested.ExistsKey(path))
	if err != nil {
		return nil, nil, fmt.Errorf("fetchExistsAtPath: read exists key %q for %q: %w", path, leaf.prop, err)
	}
	if len(leaf.nested.arrayIndices) == 0 {
		return positions, release, nil
	}
	dbm := &docBitmap{docIDs: positions, release: release}
	restricted, err := leaf.restrictByNestedIdx(f.s, dbm)
	if err != nil {
		return nil, nil, err
	}
	return restricted.docIDs, restricted.release, nil
}

// buildRecGroupExecutor fetches raw positions for the group's children,
// normalizes tokenization / IsNull, builds the recursive plan from the
// resulting positives, and returns a ready-to-use recExecutor. The returned
// releases must be invoked after the executor is done.
func (pv *propValuePair) buildRecGroupExecutor(
	ctx context.Context,
	s *Searcher,
	children []*propValuePair,
) (recPlanNode, *recExecutor, []func(), error) {
	rootProp, err := schema.GetPropertyByName(pv.Class, pv.prop)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("buildRecGroupExecutor: root property %q not found: %w", pv.prop, err)
	}
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, nil, nil, fmt.Errorf("buildRecGroupExecutor: meta bucket for %q not found", pv.prop)
	}

	fetcher := &searcherBitmapFetcher{pv: pv, s: s}
	input, err := normalizeRecGroup(ctx, pv, children, fetcher, s.nestedBitmapOps, concurrency.SROAR_MERGE)
	if err != nil {
		return nil, nil, nil, err
	}

	builder := newRecPlanBuilder(rootProp.NestedProperties)

	if len(input.positives) == 0 {
		return nil, nil, nil, fmt.Errorf("buildRecGroupExecutor: no positives for prop %q (post-Phase-6.5 IsNull is materialized as a positive)", pv.prop)
	}

	// Dispatch on outer operator. Wrapping for all shapes is decided
	// at extraction time by groupNestedSubtrees; here we just plant
	// the right plan node.
	//   AND / ContainsAll → recGroupNode (same-element correlation).
	//   OR / ContainsAny  → recOrNode (union at deepest common LCA).
	//   NOT               → recNotNode (invert at operand LCA).
	var plan recPlanNode
	switch pv.operator {
	case filters.OperatorOr, filters.ContainsAny:
		plan = builder.buildOrAtScope(pv, "")
	case filters.OperatorNot:
		plan = builder.buildNotAtScope(pv, "")
	default:
		plan = builder.build(input.positives)
	}

	exec := newRecExecutor(input.rawsByCond, metaBucket, s.nestedBitmapOps, concurrency.SROAR_MERGE).
		withProps(rootProp.NestedProperties)

	return plan, exec, input.releases, nil
}

// Compile-time check that searcherBitmapFetcher satisfies recBitmapFetcher.
var _ recBitmapFetcher = (*searcherBitmapFetcher)(nil)
