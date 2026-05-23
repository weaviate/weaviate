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
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/schema"
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

// groupKey returns a string that uniquely identifies the set of arr[N] constraints.
// Two conditions with the same groupKey are safe to combine in a correlated AND
// (same-element semantics); different keys require independent resolution.
func (a arrayIndices) groupKey() string {
	if len(a) == 0 {
		return ""
	}
	var key strings.Builder
	for _, ai := range a {
		fmt.Fprintf(&key, "%s[%d]", ai.RelPath, ai.Index)
	}
	return key.String()
}

// fetchNestedDocIDs resolves a value filter on a nested property, returning
// docID-only results. It fetches raw positions, applies any arr[N] index
// constraints, then strips position bits to extract plain docIDs.
func (pv *propValuePair) fetchNestedDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	dbm, err := pv.fetchNestedPositions(ctx, s, limit)
	if err != nil {
		return nil, err
	}
	dbm.docIDs = invnested.MaskRootLeaf(dbm.docIDs)
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

	positions, release, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, fmt.Errorf("nested IsNull: read exists key for %q: %w", pv.prop, err)
	}
	defer release()

	if err := pv.restrictByNestedIdx(s, positions); err != nil {
		return nil, err
	}

	dbm := newDocBitmap()
	dbm.docIDs = invnested.MaskRootLeaf(positions)
	// pv.value is a little-endian bool: 0x01 = true (IsNull — property absent → denylist).
	dbm.isDenyList = len(pv.value) > 0 && pv.value[0] == 0x01
	return &dbm, nil
}

// fetchNestedPositions fetches the raw position bitmap for a nested value
// filter and applies any arr[N] index constraints. Positions are not stripped
// to docIDs — callers that need docIDs use fetchNestedDocIDs instead; the
// correlated resolution path (resolveNestedCorrelated) uses this directly.
func (pv *propValuePair) fetchNestedPositions(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	dbm, err := pv.readFromBucket(ctx, s, limit)
	if err != nil {
		return nil, err
	}
	if err := pv.restrictByNestedIdx(s, dbm.docIDs); err != nil {
		return nil, err
	}
	return dbm, nil
}

// restrictByNestedIdx restricts a position bitmap to the specific array
// elements recorded in pv.nested.arrayIndices. For each constraint it reads
// IdxKey(relPath, index) from the nested metadata bucket and ANDs it into
// the bitmap in place. No-op when arrayIndices is empty.
func (pv *propValuePair) restrictByNestedIdx(s *Searcher, positions *sroar.Bitmap) error {
	if len(pv.nested.arrayIndices) == 0 {
		return nil
	}
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return fmt.Errorf("nested [N] filter: meta bucket for %q not found — is it indexed?", pv.prop)
	}
	for _, ai := range pv.nested.arrayIndices {
		idxPositions, release, err := metaBucket.RoaringSetGet(invnested.IdxKey(ai.RelPath, ai.Index))
		if err != nil {
			return fmt.Errorf("nested [N] filter: read idx key for %q[%d]: %w", ai.RelPath, ai.Index, err)
		}
		positions.And(idxPositions)
		release()
	}
	return nil
}

// positionBitmaps groups pre-fetched raw position bitmaps for a single nested path,
// split by origin so the executor can apply the correct combining strategy:
//   - tokens: from childrenFromTokenization compound ANDs (multi-token text);
//     combined with AndAll — all tokens must share the same leaf position.
//   - independent: from direct leaf conditions (e.g. scalar array values);
//     combined with MaskLeafAndAll when there are multiple — values may be at
//     different leaf positions within the same parent element.
type positionBitmaps struct {
	tokens      []*sroar.Bitmap
	independent []*sroar.Bitmap
}

// resolveNestedCorrelated resolves one prop group using position-aware
// correlation. It builds a resolutionPlan for the group's paths, pre-computes
// raw position bitmaps, and executes the plan to enforce same-element semantics.
//
// When children carry conflicting arr[N] constraints (e.g. cars[1].X and
// cars[0].Y), they are partitioned by their arrayIndices key and each partition
// is resolved independently. The per-partition results are ANDed at docID level
// so that a document is returned only when it satisfies all partitions — without
// incorrectly requiring conditions from different explicit elements to land in
// the same element.
func (pv *propValuePair) resolveNestedCorrelated(ctx context.Context, s *Searcher) (*docBitmap, error) {
	groups := groupChildrenByArrayIndicesKey(pv.children)
	switch len(groups) {
	case 0:
		return nil, fmt.Errorf("nested correlated AND: no condition groups for %q", pv.prop)
	case 1:
		return pv.resolveNestedCorrelatedGroup(ctx, s, groups[0])
	default:
		// Multiple groups with conflicting arr[N] constraints: resolve each group
		// independently using same-element semantics, then AND the docID results.
		first, err := pv.resolveNestedCorrelatedGroup(ctx, s, groups[0])
		if err != nil {
			return nil, err
		}
		combined := first.docIDs
		for _, group := range groups[1:] {
			dbm, err := pv.resolveNestedCorrelatedGroup(ctx, s, group)
			if err != nil {
				return nil, err
			}
			combined.And(dbm.docIDs)
		}
		result := newDocBitmap()
		result.docIDs = combined
		return &result, nil
	}
}

// resolveNestedCorrelatedGroup resolves a single set of children using
// position-aware same-element correlation. All children must have compatible
// arrayIndices (same arr[N] constraints at every level).
func (pv *propValuePair) resolveNestedCorrelatedGroup(ctx context.Context, s *Searcher, children []*propValuePair) (*docBitmap, error) {
	// Build positionsByPath: fetch raw position bitmaps per pvp and route into
	// the correct bucket slot based on origin (token vs independent).
	positionsByPath := make(map[string]*positionBitmaps, len(children))
	paths := make([]string, 0, len(children))

	fetchAndRoute := func(leaf *propValuePair, isToken bool) error {
		dbm, err := leaf.fetchNestedPositions(ctx, s, 0)
		if err != nil {
			return err
		}
		path := leaf.nested.relPath
		if _, exists := positionsByPath[path]; !exists {
			positionsByPath[path] = &positionBitmaps{}
			paths = append(paths, path)
		}
		if isToken {
			positionsByPath[path].tokens = append(positionsByPath[path].tokens, dbm.docIDs)
		} else {
			positionsByPath[path].independent = append(positionsByPath[path].independent, dbm.docIDs)
		}
		return nil
	}

	for _, child := range children {
		if child.nested.isNested {
			// When pv itself is a tokenization compound AND, its children are tokens
			// of the same value and must share the same leaf position → route as tokens.
			if err := fetchAndRoute(child, pv.nested.childrenFromTokenization); err != nil {
				return nil, fmt.Errorf("nested correlated AND: fetch bitmap for %q: %w", child.nested.relPath, err)
			}
		} else {
			// Tokenization compound AND child: grandchildren are tokens of the same value.
			for _, gc := range child.children {
				if err := fetchAndRoute(gc, true); err != nil {
					return nil, fmt.Errorf("nested correlated AND: fetch bitmap for %q: %w", gc.nested.relPath, err)
				}
			}
		}
	}

	// Find the root property's schema so the plan builder can locate intermediate
	// object[] arrays and determine same-element semantics for each group.
	rootProp, err := schema.GetPropertyByName(pv.Class, pv.prop)
	if err != nil {
		return nil, fmt.Errorf("nested correlated AND: root property %q not found in schema: %w", pv.prop, err)
	}

	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, fmt.Errorf("nested correlated AND: meta bucket for %q not found — is it indexed?", pv.prop)
	}

	// Compute per-path condition counts (tokens, independents) for the plan builder.
	counts := make(conditionCounts, len(positionsByPath))
	for path, positions := range positionsByPath {
		counts[path] = [2]int{len(positions.tokens), len(positions.independent)}
	}

	plan, err := newExecutionPlanBuilder(rootProp.NestedProperties).build(paths, counts)
	if err != nil {
		return nil, fmt.Errorf("nested correlated AND: build plan for %q: %w", pv.prop, err)
	}

	docIDs, err := newPlanExecutor(plan, positionsByPath, metaBucket).execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("nested correlated AND: execute for %q: %w", pv.prop, err)
	}

	dbm := newDocBitmap()
	dbm.docIDs = docIDs
	return &dbm, nil
}

// groupChildrenByArrayIndicesKey partitions the children of a correlated AND
// node by their arr[N] constraint key. Children with the same key can be
// combined with same-element semantics; children with different keys must be
// resolved independently. A single group is returned when all children are
// compatible (the common case when no arr[N] constraints are used).
func groupChildrenByArrayIndicesKey(children []*propValuePair) [][]*propValuePair {
	seen := map[string]int{} // key → index into groups
	var groups [][]*propValuePair

	for _, child := range children {
		var key string
		if child.nested.isNested {
			key = child.nested.arrayIndices.groupKey()
		} else if len(child.children) > 0 {
			// Tokenization compound AND: all grandchildren share the same arrayIndices.
			key = child.children[0].nested.arrayIndices.groupKey()
		}
		if idx, ok := seen[key]; ok {
			groups[idx] = append(groups[idx], child)
		} else {
			seen[key] = len(groups)
			groups = append(groups, []*propValuePair{child})
		}
	}

	return groups
}
