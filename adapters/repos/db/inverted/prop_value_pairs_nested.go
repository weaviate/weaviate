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

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/schema"
)

// nestedInfo groups fields that are only relevant for nested (object/object[])
// property filters. The zero value represents a non-nested node.
//
// isNested and isWithinRootSubtree are mutually exclusive — a node is either a leaf
// or a group, never both:
//
// When isNested is set (leaf node):
//   - prop on the parent propValuePair is the top-level property name
//   - value is the bare encoded value (without prefix)
//   - relPath is the dot-notation path relative to prop
//     (e.g. "city" or "owner.firstname")
//   - the result bitmap contains positions that are stripped to docIDs
//
// When isWithinRootSubtree is set (AND group node):
//   - prop on the parent propValuePair is the root property name
//   - all children require position-aware same-element resolution
//   - childrenFromTokenization marks a compound AND from multi-token text;
//     its children are tokens that must share the same leaf position
type nestedInfo struct {
	isNested                 bool
	relPath                  string
	isWithinRootSubtree      bool
	childrenFromTokenization bool
	arrayIndices             arrayIndices
}

// arrayIndices holds positional constraints from arr[N] filter syntax.
// Each entry restricts matching positions to the specified array element.
// Multiple entries support multi-level indexing (e.g. cars[1].tags[2]).
type arrayIndices []filnested.ArrayIndex

// fetchNestedDocIDs resolves a value filter on a nested property, returning
// docID-only results. It fetches raw positions, applies any arr[N] index
// constraints, then strips position bits to extract plain docIDs.
//
// No limit parameter: the underlying position bitmap holds (root|leaf|docID)
// entries, so a cursor cardinality limit applied during the bucket read would
// short-circuit on position count rather than docID count and return fewer
// docs than the caller asked for. Limits are enforced at the docID-bitmap
// layer by the iterator clamp (DocIDsLimited → LimitedIterator,
// Objects → objectsByDocID).
func (pv *propValuePair) fetchNestedDocIDs(ctx context.Context, s *Searcher) (*docBitmap, error) {
	positions, err := pv.fetchNestedPositions(ctx, s)
	if err != nil {
		return nil, err
	}
	defer positions.release()

	dbm := &docBitmap{isDenyList: positions.isDenyList}
	dbm.docIDs, dbm.release = s.nestedBitmapOps.MaskRootLeaf(positions.docIDs)
	return dbm, nil
}

// fetchNestedIsNull resolves an IsNull filter for a nested property when it
// stands alone (top-level filter or a single-clause OR/NOT child). For IsNull
// inside a correlated AND see routeDirectLeaf in prop_value_pairs_nested_recursive.go
// — both paths produce the same strict-existential semantics now.
//
// Two routing rules:
//
//  1. IS NULL on the nested root prop itself (relPath="") is a doc-level
//     question — "doc has no addresses anywhere" — and does not have a
//     per-element interpretation. Returns the prop's _exists positions
//     stripped to docIDs with isDenyList=true, so the outer machinery
//     inverts at the doc universe.
//
//  2. IS NULL on a sub-property (relPath != "") follows per-element inversion
//     (scope-aware IsNull): strict-existential per-element at the operand's natural
//     LCA. Materialized as universe (_exists.{operandLCA}) AndNot
//     operand (_exists.{relPath}); both pin-restricted; result is the
//     positive bitmap at the operand's LCA, then stripped to docIDs.
//     Returns isDenyList=false. Vacuous case (no elements at LCA for a
//     doc) → empty result → doc does not match. The correlated-AND path
//     applies the same rule inline via fetchExistsAtPath.
//
// IS NOT NULL is the existential dual in both cases: returns an
// allowlist of positions where the field exists.
//
// Pre-Phase-6 universal-at-docID semantics for sub-property IS NULL is
// no longer expressible implicitly. Until explicit ANY/ALL/NONE
// quantifiers ship, the closest universal-style query remains
// `<prop> IS NULL` (IsNull on the array property itself, rule 1
// above — "no <prop> at all" at the root scope).
func (pv *propValuePair) fetchNestedIsNull(s *Searcher) (*docBitmap, error) {
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, errors.Errorf("nested IsNull: meta bucket for %q not found — is it indexed?", pv.prop)
	}

	// pv.value is a little-endian bool: 0x01 = true (IS NULL).
	isNullTrue := len(pv.value) > 0 && pv.value[0] == 0x01

	// Read operand: positions where the field exists at pv.nested.relPath.
	operandRaw, operandRel, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, fmt.Errorf("nested IsNull: read exists key for %q: %w", pv.prop, err)
	}
	operand, err := pv.restrictByNestedIdx(s, &docBitmap{docIDs: operandRaw, release: operandRel})
	if err != nil {
		return nil, err
	}
	defer operand.release()

	// IS NOT NULL — existential allowlist of positions where the field
	// exists, stripped to docIDs. Identical behavior for relPath="" (doc
	// has the prop) and relPath!="" (∃ element with the field).
	if !isNullTrue {
		docIDs, docIDsRel := s.nestedBitmapOps.MaskRootLeaf(operand.docIDs)
		return &docBitmap{docIDs: docIDs, release: docIDsRel}, nil
	}

	// IS NULL on the array prop itself — doc-level question. Return
	// denylist so the outer machinery inverts at doc universe.
	if pv.nested.relPath == "" {
		docIDs, docIDsRel := s.nestedBitmapOps.MaskRootLeaf(operand.docIDs)
		return &docBitmap{docIDs: docIDs, release: docIDsRel, isDenyList: true}, nil
	}

	// IS NULL on sub-property — materialize positive at operand's LCA:
	// universe AndNot operand.
	lca, err := pv.isNullOperandLCA()
	if err != nil {
		return nil, err
	}

	universeRaw, universeRel, err := metaBucket.RoaringSetGet(invnested.ExistsKey(lca))
	if err != nil {
		return nil, fmt.Errorf("nested IsNull: read exists key for LCA %q: %w", lca, err)
	}
	universe, err := pv.restrictByNestedIdx(s, &docBitmap{docIDs: universeRaw, release: universeRel})
	if err != nil {
		return nil, err
	}
	defer universe.release()

	positive, positiveRel := s.nestedBitmapOps.AndNot(universe.docIDs, operand.docIDs, concurrency.SROAR_MERGE)
	defer positiveRel()

	docIDs, docIDsRel := s.nestedBitmapOps.MaskRootLeaf(positive)
	return &docBitmap{docIDs: docIDs, release: docIDsRel}, nil
}

// fetchNestedContainsNone resolves a ContainsNone filter on a nested
// scalar-array path (text[], int[], etc.) or single-value scalar. Strict
// existential semantics: a doc matches iff ∃ a position at the operand's
// path whose value is NOT in the listed set.
//
// Implementation: read `_exists.{relPath}` as the universe (restricted by
// any arr[N] pins on this pv); compute operand = OR over each value's
// bitmap (with multi-token text values AndAll'd through their tokenization
// wrapper child); strict-existential = universe AndNot operand; strip to
// docIDs.
//
// The universe is the operand's own scalar-array path — `_exists.tags` for
// `country.tags`, never `_exists.""`. This means sibling-branch leaves
// (e.g. `cities`) and phantom leaves from empty containers cannot leak
// through the AndNot.
func (pv *propValuePair) fetchNestedContainsNone(ctx context.Context, s *Searcher) (*docBitmap, error) {
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, errors.Errorf("nested ContainsNone: meta bucket for %q not found", pv.prop)
	}
	if len(pv.children) == 0 {
		return nil, errors.Errorf("nested ContainsNone: no values for %q", pv.prop)
	}

	operand, operandRel, err := pv.collectNestedContainsOperand(ctx, s)
	if err != nil {
		return nil, err
	}
	defer operandRel()

	universeRaw, universeRel, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, fmt.Errorf("nested ContainsNone: read exists key for %q: %w", pv.nested.relPath, err)
	}
	universe, err := pv.restrictByNestedIdx(s, &docBitmap{docIDs: universeRaw, release: universeRel})
	if err != nil {
		return nil, err
	}
	defer universe.release()

	positive, positiveRel := s.nestedBitmapOps.AndNot(universe.docIDs, operand, concurrency.SROAR_MERGE)
	defer positiveRel()

	docIDs, docIDsRel := s.nestedBitmapOps.MaskRootLeaf(positive)
	return &docBitmap{docIDs: docIDs, release: docIDsRel}, nil
}

// collectNestedContainsOperand fetches each ContainsNone child's position
// bitmap and OR-s them. Children come from extractContains and are either:
//
//   - Single-token value leaves (`isNested=true`, `operator=Equal`): one
//     fetchNestedPositions read.
//   - Tokenization wrappers (`childrenFromTokenization=true`): AndAll over
//     grandchildren tokens so all tokens land on the same leaf (multi-token
//     text value semantics).
//
// Returns a borrowed bitmap and a release that cleans up all intermediates.
func (pv *propValuePair) collectNestedContainsOperand(ctx context.Context, s *Searcher) (*sroar.Bitmap, func(), error) {
	releases := make([]func(), 0, len(pv.children))
	cleanup := func() {
		for _, rel := range releases {
			rel()
		}
	}
	succeeded := false
	defer func() {
		if !succeeded {
			cleanup()
		}
	}()

	perValue := make([]*sroar.Bitmap, 0, len(pv.children))
	for _, child := range pv.children {
		bm, rel, err := pv.fetchContainsChildBitmap(ctx, s, child)
		if err != nil {
			return nil, nil, err
		}
		releases = append(releases, rel)
		perValue = append(perValue, bm)
	}

	if len(perValue) == 1 {
		succeeded = true
		return perValue[0], cleanup, nil
	}
	or, orRel := s.nestedBitmapOps.OrAll(perValue, concurrency.SROAR_MERGE)
	releases = append(releases, orRel)
	succeeded = true
	return or, cleanup, nil
}

// fetchContainsChildBitmap returns the position bitmap for one ContainsNone
// child value. Multi-token values (childrenFromTokenization=true) AndAll
// their grandchildren tokens so all tokens of the same value land on the
// same leaf.
func (pv *propValuePair) fetchContainsChildBitmap(ctx context.Context, s *Searcher, child *propValuePair) (*sroar.Bitmap, func(), error) {
	if child.nested.childrenFromTokenization {
		if len(child.children) == 0 {
			return nil, nil, errors.Errorf("nested ContainsNone: tokenization wrapper with no tokens for %q", pv.prop)
		}
		tokenBitmaps := make([]*sroar.Bitmap, 0, len(child.children))
		releases := make([]func(), 0, len(child.children))
		succeeded := false
		defer func() {
			if !succeeded {
				for _, rel := range releases {
					rel()
				}
			}
		}()
		for _, gc := range child.children {
			dbm, err := gc.fetchNestedPositions(ctx, s)
			if err != nil {
				return nil, nil, err
			}
			tokenBitmaps = append(tokenBitmaps, dbm.docIDs)
			releases = append(releases, dbm.release)
		}
		if len(tokenBitmaps) == 1 {
			succeeded = true
			return tokenBitmaps[0], releases[0], nil
		}
		anded, andedRel := s.nestedBitmapOps.AndAll(tokenBitmaps, concurrency.SROAR_MERGE)
		releases = append(releases, andedRel)
		cleanup := func() {
			for _, rel := range releases {
				rel()
			}
		}
		succeeded = true
		return anded, cleanup, nil
	}
	dbm, err := child.fetchNestedPositions(ctx, s)
	if err != nil {
		return nil, nil, err
	}
	return dbm.docIDs, dbm.release, nil
}

// isNullOperandLCA returns the operand's natural LCA for an IS NULL filter on
// a sub-property (pv.nested.relPath != ""). Result is the deepest object[]
// segment strictly above relPath, or "" if the field is at the top level of
// the nested root.
//
// Examples (under typical schema):
//
//	relPath="cars.make"          → "cars"            (parent object[] segment)
//	relPath="cars.tires.width"   → "cars.tires"      (parent object[] segment)
//	relPath="cars"               → ""                (no enclosing object[])
//	relPath="cars.tires"         → "cars"            (parent object[] segment)
//
// Caller must ensure relPath != "" — relPath="" is handled by the doc-level
// branch in fetchNestedIsNull and never reaches this helper.
func (pv *propValuePair) isNullOperandLCA() (string, error) {
	segs := filnested.SplitPath(pv.nested.relPath)
	if len(segs) <= 1 {
		// Field is at the top-level of the nested root — parent is root.
		// Class is not needed for this case.
		return "", nil
	}
	if pv.Class == nil {
		return "", fmt.Errorf("nested IsNull: class is nil for prop %q (cannot resolve operand LCA)", pv.prop)
	}
	rootProp, err := schema.GetPropertyByName(pv.Class, pv.prop)
	if err != nil {
		return "", fmt.Errorf("nested IsNull: root property %q not found: %w", pv.prop, err)
	}
	parentPath := filnested.JoinPath(segs[:len(segs)-1])
	return lastIntermediateObjectArrayInProps(rootProp.NestedProperties, parentPath), nil
}

// fetchNestedPositions fetches the raw position bitmap for a nested value
// filter and applies any arr[N] index constraints. Positions are not stripped
// to docIDs — callers that need docIDs use fetchNestedDocIDs instead; the
// correlated resolution path (resolveNestedSubtree) uses this directly.
//
// No limit parameter: positions (root|leaf|docID) don't map 1:1 to docs, so a
// cursor-level limit would short-circuit on position cardinality and produce
// wrong results once any matching doc contributes multiple positions. The
// bucket read is always unlimited; docID-count limits apply post-MaskRootLeaf
// at the iterator boundary.
//
// NotEqual returns a denylist position bitmap from readFromBucket. We
// materialize the positive bitmap here at the leaf's natural LCA: positions
// where the property is indexed AND-NOT the denylist set. This yields the
// same positive bitmap that NOT(Equal) would compute via buildNotAtScope,
// so NotEqual is semantically equivalent to NOT(Equal) at every nesting
// level (existential per-element). Lazy: the universe is only loaded when
// isDenyList is set. Rangeable indices (future) that return positive
// bitmaps directly skip the materialization.
func (pv *propValuePair) fetchNestedPositions(ctx context.Context, s *Searcher) (*docBitmap, error) {
	raw, err := pv.readFromBucket(ctx, s, 0)
	if err != nil {
		return nil, err
	}
	raw, err = pv.restrictByNestedIdx(s, raw)
	if err != nil {
		return nil, err
	}
	if !raw.isDenyList {
		return raw, nil
	}
	// Materialize NotEqual at LCA. Defers are panic-proof: even if
	// fetchNestedExistsPositions or AndNot panic, raw and universe are
	// released on unwind.
	defer raw.release()
	universe, universeRel, err := pv.fetchNestedExistsPositions(s)
	if err != nil {
		return nil, fmt.Errorf("materialize NotEqual at LCA for %q: %w", pv.nested.relPath, err)
	}
	defer universeRel()
	positive, posRel := s.nestedBitmapOps.AndNot(universe, raw.docIDs, concurrency.SROAR_MERGE)
	return &docBitmap{docIDs: positive, release: posRel, isDenyList: false}, nil
}

// restrictByNestedIdx restricts a position bitmap to the specific array
// elements recorded in pv.nested.arrayIndices. For each constraint it reads
// IdxKey(relPath, index) from the nested metadata bucket and ANDs it into
// the accumulator using mergeBitmapsAndOrWithDenyList, which selects the more
// efficient accumulator (smaller for AND) and releases the discarded buffer.
// Returns the (potentially swapped) accumulator on success. On error the
// accumulator is released internally and nil is returned.
// No-op when arrayIndices is empty.
func (pv *propValuePair) restrictByNestedIdx(s *Searcher, positions *docBitmap) (*docBitmap, error) {
	if len(pv.nested.arrayIndices) == 0 {
		return positions, nil
	}
	// Release the current accumulator on any non-success exit (error or panic).
	// The closure reads positions at call time, so it always releases the latest
	// accumulator even after mergeBitmapsAndOrWithDenyList swaps it.
	succeeded := false
	defer func() {
		if !succeeded {
			positions.release()
		}
	}()

	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, fmt.Errorf("nested [N] filter: meta bucket for %q not found — is it indexed?", pv.prop)
	}
	var keyBuf [invnested.IdxKeySize]byte
	for _, ai := range pv.nested.arrayIndices {
		positionsIdx, release, err := metaBucket.RoaringSetGet(invnested.IdxKeyToBuf(ai.RelPath, ai.Index, keyBuf[:]))
		if err != nil {
			return nil, fmt.Errorf("nested [N] filter: read idx key for %q[%d]: %w", ai.RelPath, ai.Index, err)
		}
		dbmIdx := &docBitmap{docIDs: positionsIdx, release: release}
		positions = mergeBitmapsAndOrWithDenyList(positions, dbmIdx, filters.OperatorAnd)
	}
	succeeded = true
	return positions, nil
}

// fetchNestedExistsPositions reads the raw position bitmap for a nested property's
// _exists entry from the meta bucket. Unlike fetchNestedIsNull it does NOT apply
// MaskRootLeaf — the caller receives element-level positions suitable for use in
// the correlated resolver (either as an include or an exclude/AndNot bitmap).
// If the filter carries arr[N] constraints (e.g. "cars[1].make IS NULL"),
// restrictByNestedIdx is applied so that only the specified element's positions
// are returned.
func (pv *propValuePair) fetchNestedExistsPositions(s *Searcher) (*sroar.Bitmap, func(), error) {
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, nil, fmt.Errorf("nested IsNull: meta bucket for %q not found — is it indexed?", pv.prop)
	}
	positions, release, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, nil, fmt.Errorf("nested IsNull: read exists key for %q: %w", pv.prop, err)
	}
	if len(pv.nested.arrayIndices) == 0 {
		return positions, release, nil
	}
	// Restrict to the specific array element(s) indicated by arr[N] constraints.
	dbm := &docBitmap{docIDs: positions, release: release}
	restricted, err := pv.restrictByNestedIdx(s, dbm)
	if err != nil {
		return nil, nil, err
	}
	return restricted.docIDs, restricted.release, nil
}

// resolveNestedSubtree resolves a nested correlated subtree using
// position-aware semantics. The outer operator dispatches:
//
//   - AND: children are partitioned by arr[N] compatibility, then resolved
//     as same-element AND within each group; multi-group ANDs combine via
//     docID-level AND (or root+docID AND under intermediate-LCA conflicts).
//   - OR / NOT: arr[N] partitioning is skipped — the planner handles pins
//     per-child inside the recOrNode / recNotNode (each child's own
//     buildPlan emits recSplitNode for pinned operands; buildNotAtScope
//     lifts pins via liftArrayIndicesFromOperand).
//
// TODO aliszka:nested_filtering reject filters mixing conflicting explicit
// intermediate arr[N] constraints with unconstrained conditions at the same
// level in filter validation. Until that lands, unconstrained items in that
// shape are silently dropped during plan construction.
//
// No user limit reaches this path: leaf reads go through fetchNestedPositions
// which has no limit parameter — positions don't map 1:1 to docs, so the
// bucket cursor would short-circuit on position cardinality and produce
// wrong results. The user limit applies post-MaskRootLeaf at the iterator
// clamp (objectsByDocID / LimitedIterator) in the outer caller.
func (pv *propValuePair) resolveNestedSubtree(ctx context.Context, s *Searcher) (*docBitmap, error) {
	if pv.operator == filters.OperatorOr || pv.operator == filters.OperatorNot ||
		pv.operator == filters.ContainsAny {
		// Single-group fast path: the planner's buildOrAtScope /
		// buildNotAtScope handle per-child arr[N] pins internally;
		// top-level partitioning would fan out into single-child groups
		// and lose leaf bitmap references. ContainsAny is an OR alias
		// under first-class-operator approach — same path as OperatorOr.
		return pv.resolveNestedSubtreeGroup(ctx, s, pv.children)
	}

	groups, allRootConstrained := groupChildrenByArrayIndicesKey(pv.children)
	switch len(groups) {
	case 0:
		return nil, fmt.Errorf("nested subtree: no condition groups for %q", pv.prop)
	case 1:
		return pv.resolveNestedSubtreeGroup(ctx, s, groups[0])
	}

	if allRootConstrained {
		// All groups explicitly target different root elements — independent.
		return pv.resolveMultiGroupDocIDLevelAnd(ctx, s, groups)
	}

	// Groups have conflicting arr[N] constraints at an intermediate level.
	// Resolve each group to root+docID positions and AND them so all conditions
	// must be satisfied by the same root element.
	return pv.resolveMultiGroupRootDocIDAnd(ctx, s, groups)
}

// resolveMultiGroupDocIDLevelAnd resolves each group independently and AND's the
// plain docID results. Correct when all groups explicitly target different root
// elements (allGroupsRootConstrained).
func (pv *propValuePair) resolveMultiGroupDocIDLevelAnd(ctx context.Context, s *Searcher, groups [][]*propValuePair) (*docBitmap, error) {
	dbm, err := pv.resolveNestedSubtreeGroup(ctx, s, groups[0])
	if err != nil {
		return nil, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			dbm.release()
		}
	}()
	for _, group := range groups[1:] {
		groupDbm, err := pv.resolveNestedSubtreeGroup(ctx, s, group)
		if err != nil {
			return nil, err
		}
		dbm = mergeBitmapsAndOrWithDenyList(dbm, groupDbm, filters.OperatorAnd)
	}
	succeeded = true
	return dbm, nil
}

// resolveMultiGroupRootDocIDAnd resolves each group to a raw position bitmap
// and combines them via CrossLeafCopresenceAll so all conditions must land in
// the same root element. Applied when groups have conflicting intermediate
// constraints or no common intermediate LCA — different groups' leaves are
// disjoint by construction, so plain raw AndAll would give ∅. The leaf-zeroed
// (root, doc) co-presence check across groups keeps positions whose root
// element contains a passing element in every group.
func (pv *propValuePair) resolveMultiGroupRootDocIDAnd(ctx context.Context, s *Searcher, groups [][]*propValuePair) (*docBitmap, error) {
	rawBitmaps := make([]*sroar.Bitmap, 0, len(groups))
	releases := make([]func(), 0, len(groups))
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range releases {
				rel()
			}
		}
	}()

	for _, group := range groups {
		raw, rel, err := pv.resolveGroupRaw(ctx, s, group)
		if err != nil {
			return nil, err
		}
		rawBitmaps = append(rawBitmaps, raw)
		releases = append(releases, rel)
	}

	combined, combinedRel := s.nestedBitmapOps.CrossLeafCopresenceAll(rawBitmaps)
	docIDs, docRelease := s.nestedBitmapOps.MaskRootLeaf(combined)
	combinedRel()
	succeeded = true
	for _, rel := range releases {
		rel()
	}
	return &docBitmap{docIDs: docIDs, release: docRelease}, nil
}

// resolveNestedSubtreeGroup resolves a single set of children using
// position-aware same-element correlation through the recursive plan + executor.
// The raw position bitmap returned by the executor is stripped to docIDs via
// MaskRootLeaf.
func (pv *propValuePair) resolveNestedSubtreeGroup(ctx context.Context, s *Searcher, children []*propValuePair) (*docBitmap, error) {
	raw, rawRelease, err := pv.resolveGroupRaw(ctx, s, children)
	if err != nil {
		return nil, err
	}
	defer rawRelease()
	docIDs, release := s.nestedBitmapOps.MaskRootLeaf(raw)
	return &docBitmap{docIDs: docIDs, release: release}, nil
}

// resolveGroupRaw resolves children to a raw position bitmap through the
// recursive plan + executor. Used directly by resolveMultiGroupRootDocIDAnd
// to combine groups via CrossLeafCopresenceAll before stripping to docIDs,
// and indirectly by resolveNestedSubtreeGroup which strips immediately.
//
// Invariant: callers must apply MaskRootLeaf to the returned bitmap before
// it reaches any docID-counting code (cardinality checks, iterator clamps,
// sort). Raw positions are (root|leaf|docID) tuples; treating their
// cardinality as a doc count yields wrong results once a single doc
// contributes multiple positions.
func (pv *propValuePair) resolveGroupRaw(ctx context.Context, s *Searcher, children []*propValuePair) (*sroar.Bitmap, func(), error) {
	plan, executor, releases, err := pv.buildRecGroupExecutor(ctx, s, children)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		for _, rel := range releases {
			rel()
		}
	}()

	// TODO aliszka:nested_filtering concurrency.SROAR_MERGE is a fixed budget;
	// consider deriving it from the request context or shard-level config.
	raw, rawRelease, err := executor.execute(ctx, plan)
	if err != nil {
		return nil, nil, fmt.Errorf("nested subtree: execute for %q: %w", pv.prop, err)
	}
	return raw, rawRelease, nil
}

// ---------------------------------------------------------------------------
// Grouping helpers
// ---------------------------------------------------------------------------

// childRelPath returns the relative path of a child, handling both direct leaf
// conditions and tokenization compound AND children. OR/NOT operator items
// have no single representative path — their structure is planned recursively;
// returning "" defers placement to the OR/NOT-aware dispatch in buildGroup.
func childRelPath(child *propValuePair) string {
	if child.nested.isNested {
		return child.nested.relPath
	}
	if child.operator == filters.OperatorOr || child.operator == filters.OperatorNot {
		return ""
	}
	if len(child.children) > 0 {
		return child.children[0].nested.relPath
	}
	return ""
}

// childArrayIndices returns the arr[N] constraints of a child. OR/NOT
// operator items are not subject to outer arr[N] dispatch — their pins are
// internal (lifted into recNotNode for universe restriction, or carried by
// each OR child's own plan). Returning nil keeps the outer planner from
// wrapping an OR/NOT in a SPLIT based on inner-leaf pins.
func childArrayIndices(child *propValuePair) arrayIndices {
	if child.nested.isNested {
		return child.nested.arrayIndices
	}
	if child.operator == filters.OperatorOr || child.operator == filters.OperatorNot {
		return nil
	}
	if len(child.children) > 0 {
		return child.children[0].nested.arrayIndices
	}
	return nil
}

// compatibleConstraints returns true when arr[N] sets a and b can be resolved
// in the same compatibility group. Conflicts at the root RelPath ("") force
// separate groups (so each root element is resolved independently). Conflicts
// at intermediate RelPaths are tolerated: the planner builds a SPLIT-at-LCA
// shape that enforces same-element semantics at the deepest unconstrained
// ancestor above the conflict.
func compatibleConstraints(a, b arrayIndices) bool {
	aMap := make(map[string]int, len(a))
	for _, ai := range a {
		aMap[ai.RelPath] = ai.Index
	}
	for _, bi := range b {
		if aIdx, ok := aMap[bi.RelPath]; ok && aIdx != bi.Index && bi.RelPath == "" {
			return false
		}
	}
	return true
}

// groupChildrenByArrayIndicesKey partitions children into groups where all
// members have compatible arr[N] constraints (see compatibleConstraints: only
// RelPath="" conflicts split groups; intermediate-level conflicts are tolerated
// and handled via SPLIT-at-LCA in the recursive planner).
//
// Conditions without arr[N] constraints (empty arrayIndices) are trivially
// compatible with everything, so all-unconstrained filters always produce a
// single group — preserving the existing plan-builder behaviour. Conditions
// with conflicting constraints (e.g. garages[1].x AND garages[2].y) are placed
// in separate groups and resolved independently.
//
// The returned bool is true when len(groups)>1 AND every constrained condition
// has its outermost arr[N] at RelPath="" (root-level explicit indices). The
// caller uses this to select docID-level AND without a second pass over groups.
func groupChildrenByArrayIndicesKey(children []*propValuePair) ([][]*propValuePair, bool) {
	var groups [][]*propValuePair
	// allRootConstrained tracks whether every constrained condition targets a
	// root-level element (RelPath=""). Unconstrained conditions (no arr[N]) are
	// neutral and do not affect this flag.
	allRootConstrained := true

	for _, child := range children {
		indices := childArrayIndices(child)

		if len(indices) > 0 && indices[0].RelPath != "" {
			allRootConstrained = false
		}

		added := false
		for i, group := range groups {
			ok := true
			for _, member := range group {
				if !compatibleConstraints(childArrayIndices(member), indices) {
					ok = false
					break
				}
			}
			if ok {
				groups[i] = append(groups[i], child)
				added = true
				break
			}
		}
		if !added {
			groups = append(groups, []*propValuePair{child})
		}
	}

	return groups, len(groups) > 1 && allRootConstrained
}
