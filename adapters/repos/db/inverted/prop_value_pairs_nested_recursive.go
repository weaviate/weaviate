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
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
)

// recGroupInput is the normalized form fed to the recursive plan + executor.
// Tokenization wrappers have been collapsed into single virtual leaves, IsNull
// has been split into positives (existence) and excludes (absence), and the
// rootAnchor seed is set when the group has only excludes.
type recGroupInput struct {
	// positives are the *propValuePair entries the recursive planner consumes
	// as logical leaves. Each entry has a corresponding rawsByCond bitmap.
	positives []*propValuePair
	// rawsByCond maps each positive to its raw position bitmap. For a
	// tokenization wrapper the bitmap is the AndAll of all token bitmaps.
	rawsByCond map[*propValuePair]*sroar.Bitmap
	// excludePositions holds raw _exists.{path} bitmaps for IsNull=true leaves.
	// excludeLeaves[i] is the leaf the i-th bitmap was fetched for; its relPath
	// drives the deepest-object[]-LCA computation in buildRecGroupExecutor.
	excludePositions []*sroar.Bitmap
	excludeLeaves    []*propValuePair
	// rootAnchor is the seed bitmap for the no-positive case. Set when
	// positives is empty and excludePositions is non-empty.
	rootAnchor *sroar.Bitmap
	// releases holds cleanup callbacks for every bitmap acquired by the
	// normalizer (raw positions, AndAll temporaries, anchor reads). The caller
	// must invoke them after the executor has finished.
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
	// arr[N] indices on the leaf. Used for both IsNull=false (positive leaf)
	// and IsNull=true (exclude bitmap).
	fetchExists(leaf *propValuePair) (*sroar.Bitmap, func(), error)
	// fetchRootAnchor returns the _exists."" bitmap restricted by any arr[N]
	// indices visible across the whole group (the first arr[N] found on any
	// leaf or grandchild). Used as the seed when the group has no positives.
	fetchRootAnchor(children []*propValuePair) (*sroar.Bitmap, func(), error)
}

// normalizeRecGroup walks the group's children and builds the recursive input.
// It is decoupled from the Searcher via recBitmapFetcher so unit tests can
// inject synthetic bitmaps. Token wrappers are AndAll-collapsed; IsNull=false
// becomes a positive existence leaf; IsNull=true is appended to excludes.
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

	// Pattern 2 (and the mainstream non-tokenized case): each child is either a
	// direct leaf (isNested) or a tokenization wrapper containing token leaves.
	for _, child := range children {
		if child.nested.isNested {
			if err := routeDirectLeaf(ctx, child, fetcher, input); err != nil {
				return nil, err
			}
			continue
		}
		if len(child.children) == 0 {
			return nil, fmt.Errorf("normalizeRecGroup: non-nested child %p has no grandchildren", child)
		}
		combined, releases, err := fetchAndAndAllTokens(ctx, child.children, fetcher, bitmapOps, maxConcurrency)
		if err != nil {
			return nil, err
		}
		input.releases = append(input.releases, releases...)
		input.positives = append(input.positives, child)
		input.rawsByCond[child] = combined
	}

	if len(input.positives) == 0 && len(input.excludePositions) > 0 {
		anchor, anchorRel, err := fetcher.fetchRootAnchor(children)
		if err != nil {
			return nil, fmt.Errorf("normalizeRecGroup: fetch root anchor: %w", err)
		}
		input.releases = append(input.releases, anchorRel)
		input.rootAnchor = anchor
	}

	succeeded = true
	return input, nil
}

// routeDirectLeaf classifies a single isNested child and appends to the right
// slice on input. IsNull=true → excludes; IsNull=false → positive existence
// leaf; everything else → positive value leaf.
func routeDirectLeaf(ctx context.Context, leaf *propValuePair, fetcher recBitmapFetcher, input *recGroupInput) error {
	if leaf.operator == filters.OperatorIsNull {
		bm, rel, err := fetcher.fetchExists(leaf)
		if err != nil {
			return fmt.Errorf("normalizeRecGroup: fetch exists for %q: %w", leaf.nested.relPath, err)
		}
		input.releases = append(input.releases, rel)
		isAbsent := len(leaf.value) > 0 && leaf.value[0] == 0x01
		if isAbsent {
			input.excludePositions = append(input.excludePositions, bm)
			input.excludeLeaves = append(input.excludeLeaves, leaf)
			return nil
		}
		input.positives = append(input.positives, leaf)
		input.rawsByCond[leaf] = bm
		return nil
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
	dbm, err := leaf.fetchNestedPositions(ctx, f.s, 0)
	if err != nil {
		return nil, nil, err
	}
	return dbm.docIDs, dbm.release, nil
}

func (f *searcherBitmapFetcher) fetchExists(leaf *propValuePair) (*sroar.Bitmap, func(), error) {
	return leaf.fetchNestedExistsPositions(f.s)
}

func (f *searcherBitmapFetcher) fetchRootAnchor(children []*propValuePair) (*sroar.Bitmap, func(), error) {
	metaBucket := f.s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(f.pv.prop))
	if metaBucket == nil {
		return nil, nil, fmt.Errorf("fetchRootAnchor: meta bucket for %q not found", f.pv.prop)
	}
	return f.pv.fetchRootAnchor(f.s, metaBucket, children)
}

// buildRecGroupExecutor fetches raw positions for the group's children,
// normalizes tokenization / IsNull, builds the recursive plan from the
// resulting positives, and returns a ready-to-use recExecutor. The returned
// releases must be invoked after the executor is done. plan is nil when the
// group has no positives — execute() then takes the rootAnchor path.
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

	var plan recPlanNode
	if len(input.positives) > 0 {
		plan = builder.build(input.positives)
	}

	excludes := make([]recExclude, 0, len(input.excludePositions))
	for i, bm := range input.excludePositions {
		var lcaPath string
		if i < len(input.excludeLeaves) {
			lcaPath = builder.lastIntermediateObjectArray(childRelPath(input.excludeLeaves[i]))
		}
		excludes = append(excludes, recExclude{bitmap: bm, lcaPath: lcaPath})
	}

	exec := newRecExecutor(input.rawsByCond, metaBucket, s.nestedBitmapOps, concurrency.SROAR_MERGE).
		withExcludes(excludes).
		withPlanLCAs(collectPlanLCAs(plan)).
		withRootAnchor(input.rootAnchor).
		withProps(rootProp.NestedProperties)

	return plan, exec, input.releases, nil
}

// Compile-time check that searcherBitmapFetcher satisfies recBitmapFetcher.
var _ recBitmapFetcher = (*searcherBitmapFetcher)(nil)
