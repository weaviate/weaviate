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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
)

// nestedInfo groups fields that are only relevant for nested (object/object[])
// property filters. The zero value represents a non-nested node.
//
// isNested and isCorrelated are mutually exclusive — a node is either a leaf
// or a group, never both:
//
// When isNested is set (leaf node):
//   - prop on the parent propValuePair is the top-level property name
//   - value is the bare encoded value (without prefix)
//   - relPath is the dot-notation path relative to prop
//     (e.g. "city" or "owner.firstname")
//   - the result bitmap contains positions that are stripped to docIDs
//
// When isCorrelated is set (AND group node):
//   - prop on the parent propValuePair is the root property name
//   - all children require position-aware same-element resolution
//   - childrenFromTokenization marks a compound AND from multi-token text;
//     its children are tokens that must share the same leaf position
type nestedInfo struct {
	isNested                 bool
	relPath                  string
	isCorrelated             bool
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
func (pv *propValuePair) fetchNestedDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	positions, err := pv.fetchNestedPositions(ctx, s, limit)
	if err != nil {
		return nil, err
	}
	defer positions.release()

	dbm := &docBitmap{isDenyList: positions.isDenyList}
	dbm.docIDs, dbm.release = s.nestedBitmapOps.MaskRootLeaf(positions.docIDs)
	return dbm, nil
}

// fetchNestedIsNull resolves an IsNull filter for a nested property by reading
// the existence bitmap from the metadata bucket. relPath="" checks root-level
// existence (e.g. "addresses IsNull"); a non-empty relPath checks sub-property
// existence (e.g. "addresses.city IsNull").
//
// IsNull=false (property exists) → allowlist of matching docIDs.
// IsNull=true  (property absent) → denylist (complement) of matching docIDs.
func (pv *propValuePair) fetchNestedIsNull(s *Searcher) (*docBitmap, error) {
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, errors.Errorf("nested IsNull: meta bucket for %q not found — is it indexed?", pv.prop)
	}

	positionsExists, release, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, fmt.Errorf("nested IsNull: read exists key for %q: %w", pv.prop, err)
	}
	dbmExists, err := pv.restrictByNestedIdx(s, &docBitmap{docIDs: positionsExists, release: release})
	if err != nil {
		return nil, err // restrictByNestedIdx released the bitmap on error
	}
	defer dbmExists.release()

	// pv.value is a little-endian bool: 0x01 = true (IsNull — property absent → denylist).
	dbm := &docBitmap{isDenyList: len(pv.value) > 0 && pv.value[0] == 0x01}
	dbm.docIDs, dbm.release = s.nestedBitmapOps.MaskRootLeaf(dbmExists.docIDs)
	return dbm, nil
}

// fetchNestedPositions fetches the raw position bitmap for a nested value
// filter and applies any arr[N] index constraints. Positions are not stripped
// to docIDs — callers that need docIDs use fetchNestedDocIDs instead; the
// correlated resolution path (resolveNestedCorrelated) uses this directly.
func (pv *propValuePair) fetchNestedPositions(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	raw, err := pv.readFromBucket(ctx, s, limit)
	if err != nil {
		return nil, err
	}
	return pv.restrictByNestedIdx(s, raw)
}

// restrictByNestedIdx restricts a position bitmap to the specific array
// elements recorded in pv.nested.arrayIndices. For each constraint it reads
// IdxKey(relPath, index) from the nested metadata bucket and ANDs it into
// the accumulator using mergeBitmapsAndOrWithDenyList, which selects the more
// efficient accumulator (smaller for AND) and releases the discarded buffer.
// Returns the (potentially swapped) accumulator. On error the current
// accumulator is returned unchanged — the caller is responsible for releasing it.
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

// resolveNestedCorrelated resolves a correlated AND filter using position-aware
// same-element semantics. Children are grouped by arr[N] compatibility, then:
//
//   - Single group: resolved directly via resolveNestedCorrelatedGroup.
//   - All groups root-constrained (all first arr[N] have RelPath=""): the user
//     is explicitly querying different root elements → docID-level AND.
//   - Otherwise: resolve each group to root+docID positions and AND them so all
//     conditions must land in the same root element.
//
// TODO aliszka:nested_filtering reject filters mixing conflicting explicit
// intermediate arr[N] constraints with unconstrained conditions at the same
// level in filter validation. Until that lands, unconstrained items in that
// shape are silently dropped during plan construction.
func (pv *propValuePair) resolveNestedCorrelated(ctx context.Context, s *Searcher) (*docBitmap, error) {
	groups, allRootConstrained := groupChildrenByArrayIndicesKey(pv.children)
	switch len(groups) {
	case 0:
		return nil, fmt.Errorf("nested correlated AND: no condition groups for %q", pv.prop)
	case 1:
		return pv.resolveNestedCorrelatedGroup(ctx, s, groups[0])
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
	dbm, err := pv.resolveNestedCorrelatedGroup(ctx, s, groups[0])
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
		groupDbm, err := pv.resolveNestedCorrelatedGroup(ctx, s, group)
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

// resolveNestedCorrelatedGroup resolves a single set of children using
// position-aware same-element correlation through the recursive plan + executor.
// The raw position bitmap returned by the executor is stripped to docIDs via
// MaskRootLeaf.
func (pv *propValuePair) resolveNestedCorrelatedGroup(ctx context.Context, s *Searcher, children []*propValuePair) (*docBitmap, error) {
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
// and indirectly by resolveNestedCorrelatedGroup which strips immediately.
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
		return nil, nil, fmt.Errorf("nested correlated AND: execute for %q: %w", pv.prop, err)
	}
	return raw, rawRelease, nil
}

// fetchRootAnchor returns the bitmap of element positions used as the starting
// universe when all conditions in a correlated group are IsNull=true (no positive
// anchor). It reads _exists."" from the meta bucket and, if any child carries
// arr[N] constraints, restricts it to only the specified element positions via
// restrictByNestedIdx — so that "garages[1].make IS NULL" starts from garage[1]
// positions only, not all garages.
func (pv *propValuePair) fetchRootAnchor(s *Searcher, metaBucket *lsmkv.Bucket, children []*propValuePair) (*sroar.Bitmap, func(), error) {
	rootPositions, rootRelease, err := metaBucket.RoaringSetGet(invnested.ExistsKey(""))
	if err != nil {
		return nil, nil, err
	}

	// Find arr[N] constraints from any leaf child (all children in the group share the same key).
	var arrayIndices []filnested.ArrayIndex
	for _, child := range children {
		if child.nested.isNested && len(child.nested.arrayIndices) > 0 {
			arrayIndices = child.nested.arrayIndices
			break
		}
		for _, gc := range child.children {
			if len(gc.nested.arrayIndices) > 0 {
				arrayIndices = gc.nested.arrayIndices
				break
			}
		}
		if len(arrayIndices) > 0 {
			break
		}
	}

	if len(arrayIndices) == 0 {
		return rootPositions, rootRelease, nil
	}

	// Apply arr[N] restriction using a temporary propValuePair that carries only
	// the array indices — restrictByNestedIdx reads _idx.{relPath}[N] entries.
	tempPvp := &propValuePair{
		prop:   pv.prop,
		nested: nestedInfo{arrayIndices: arrayIndices},
		Class:  pv.Class,
	}
	dbm := &docBitmap{docIDs: rootPositions, release: rootRelease}
	restricted, err := tempPvp.restrictByNestedIdx(s, dbm)
	if err != nil {
		return nil, nil, err
	}
	return restricted.docIDs, restricted.release, nil
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
