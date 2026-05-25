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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

// fakeRecFetcher is a synthetic recBitmapFetcher used to drive normalizeRecGroup
// without a Searcher / buckets. It serves bitmaps keyed by leaf identity and
// records every fetch + every release invocation so tests can assert lifecycle.
type fakeRecFetcher struct {
	valueByLeaf  map[*propValuePair]*sroar.Bitmap
	existsByLeaf map[*propValuePair]*sroar.Bitmap
	rootAnchor   *sroar.Bitmap

	valueErr      error
	existsErr     error
	rootAnchorErr error
	failOnLeaf    *propValuePair // when set, fetchValue/fetchExists for this leaf returns errors.New("fake fetch error")

	valueCalls      int
	existsCalls     int
	rootAnchorCalls int
	releaseCalls    int
}

func (f *fakeRecFetcher) fetchValue(_ context.Context, leaf *propValuePair) (*sroar.Bitmap, func(), error) {
	f.valueCalls++
	if f.failOnLeaf == leaf {
		return nil, nil, errors.New("fake fetch error")
	}
	if f.valueErr != nil {
		return nil, nil, f.valueErr
	}
	bm, ok := f.valueByLeaf[leaf]
	if !ok {
		return nil, nil, fmt.Errorf("fakeRecFetcher: no value bitmap for leaf %p", leaf)
	}
	return bm, func() { f.releaseCalls++ }, nil
}

func (f *fakeRecFetcher) fetchExists(leaf *propValuePair) (*sroar.Bitmap, func(), error) {
	f.existsCalls++
	if f.failOnLeaf == leaf {
		return nil, nil, errors.New("fake fetch error")
	}
	if f.existsErr != nil {
		return nil, nil, f.existsErr
	}
	bm, ok := f.existsByLeaf[leaf]
	if !ok {
		return nil, nil, fmt.Errorf("fakeRecFetcher: no exists bitmap for leaf %p", leaf)
	}
	return bm, func() { f.releaseCalls++ }, nil
}

func (f *fakeRecFetcher) fetchRootAnchor(_ []*propValuePair) (*sroar.Bitmap, func(), error) {
	f.rootAnchorCalls++
	if f.rootAnchorErr != nil {
		return nil, nil, f.rootAnchorErr
	}
	if f.rootAnchor == nil {
		return nil, nil, fmt.Errorf("fakeRecFetcher: rootAnchor not configured")
	}
	return f.rootAnchor, func() { f.releaseCalls++ }, nil
}

// --- helpers --------------------------------------------------------------

func newFakeRecFetcher() *fakeRecFetcher {
	return &fakeRecFetcher{
		valueByLeaf:  map[*propValuePair]*sroar.Bitmap{},
		existsByLeaf: map[*propValuePair]*sroar.Bitmap{},
	}
}

func bitmapWith(values ...uint64) *sroar.Bitmap {
	bm := sroar.NewBitmap()
	for _, v := range values {
		bm.Set(v)
	}
	return bm
}

func newTestBitmapOps() *invnested.BitmapOps {
	return invnested.NewBitmapOps(roaringset.NewBitmapBufPoolNoop())
}

func valueLeaf(relPath, term string) *propValuePair {
	return &propValuePair{
		prop:     "addresses",
		value:    []byte(term),
		operator: filters.OperatorEqual,
		nested:   nestedInfo{isNested: true, relPath: relPath},
	}
}

func isNullLeaf(relPath string, isNullTrue bool) *propValuePair {
	var val byte
	if isNullTrue {
		val = 0x01
	}
	return &propValuePair{
		prop:     "addresses",
		value:    []byte{val},
		operator: filters.OperatorIsNull,
		nested:   nestedInfo{isNested: true, relPath: relPath},
	}
}

// tokenWrapperOuter builds a top-level pv with childrenFromTokenization=true,
// modeling Pattern 1: every direct child is a token of the same value.
func tokenWrapperOuter(tokens ...*propValuePair) *propValuePair {
	return &propValuePair{
		prop:     "addresses",
		operator: filters.OperatorAnd,
		nested:   nestedInfo{isCorrelated: true, childrenFromTokenization: true},
		children: tokens,
	}
}

// tokenWrapperInner builds a non-nested wrapper child whose grandchildren are
// tokens of a single value at the same path. Models Pattern 2.
func tokenWrapperInner(tokens ...*propValuePair) *propValuePair {
	return &propValuePair{
		prop:     "addresses",
		operator: filters.OperatorAnd,
		// isNested defaults to false → routed through the wrapper branch.
		children: tokens,
	}
}

// outerCorrelated builds the standard correlated-AND wrapper pv (not a tokenization wrapper).
func outerCorrelated(children ...*propValuePair) *propValuePair {
	return &propValuePair{
		prop:     "addresses",
		operator: filters.OperatorAnd,
		nested:   nestedInfo{isCorrelated: true},
		children: children,
	}
}

// --- tests ----------------------------------------------------------------

// TestNormalizeRecGroup exercises normalizeRecGroup directly through a synthetic
// recBitmapFetcher. Each sub-test asserts the shape of the resulting recGroupInput
// (positives, rawsByCond identity, excludes, rootAnchor) plus the fetcher call
// counts so we can detect missed or duplicate acquisitions.
func TestNormalizeRecGroup(t *testing.T) {
	ctx := context.Background()
	ops := newTestBitmapOps()

	t.Run("pattern1_outer_tokenization_collapses_to_single_positive", func(t *testing.T) {
		t1 := valueLeaf("city", "new")
		t2 := valueLeaf("city", "york")
		t3 := valueLeaf("city", "city")
		pv := tokenWrapperOuter(t1, t2, t3)

		f := newFakeRecFetcher()
		f.valueByLeaf[t1] = bitmapWith(1, 2, 3)
		f.valueByLeaf[t2] = bitmapWith(2, 3, 4)
		f.valueByLeaf[t3] = bitmapWith(3, 4, 5)

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		require.Len(t, input.positives, 1, "tokenization wrapper produces a single positive")
		assert.Same(t, t1, input.positives[0], "first child stands in as planner key")
		require.Contains(t, input.rawsByCond, t1)
		assert.Equal(t, []uint64{3}, input.rawsByCond[t1].ToArray(), "AndAll of all tokens")
		assert.Empty(t, input.excludePositions)
		assert.Nil(t, input.rootAnchor)

		assert.Equal(t, 3, f.valueCalls)
		assert.Equal(t, 0, f.existsCalls)
		assert.Equal(t, 0, f.rootAnchorCalls)

		// Releases: one per fetch + one for the AndAll combined bitmap.
		require.Len(t, input.releases, 4)
		for _, rel := range input.releases {
			rel()
		}
		assert.Equal(t, 3, f.releaseCalls, "fetcher releases invoked")
	})

	t.Run("pattern1_single_token_returns_bitmap_unmodified", func(t *testing.T) {
		t1 := valueLeaf("city", "berlin")
		pv := tokenWrapperOuter(t1)

		f := newFakeRecFetcher()
		raw := bitmapWith(10, 11, 12)
		f.valueByLeaf[t1] = raw

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		require.Len(t, input.positives, 1)
		assert.Same(t, t1, input.positives[0])
		assert.Same(t, raw, input.rawsByCond[t1], "single-token path skips AndAll, returns raw bitmap")
		assert.Empty(t, input.excludePositions)
		assert.Nil(t, input.rootAnchor)

		require.Len(t, input.releases, 1, "no AndAll release for single token")
	})

	t.Run("pattern1_empty_children_errors", func(t *testing.T) {
		pv := tokenWrapperOuter()
		f := newFakeRecFetcher()

		_, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "tokenization wrapper has no children")
		assert.Equal(t, 0, f.valueCalls)
	})

	t.Run("pattern2_inner_wrapper_collapses_grandchildren", func(t *testing.T) {
		t1 := valueLeaf("city", "new")
		t2 := valueLeaf("city", "york")
		wrapper := tokenWrapperInner(t1, t2)
		// pv is a normal correlated AND with a tokenization wrapper child plus
		// a sibling value leaf at the same path.
		postcode := valueLeaf("postcode", "10115")
		pv := outerCorrelated(wrapper, postcode)

		f := newFakeRecFetcher()
		f.valueByLeaf[t1] = bitmapWith(1, 2, 3)
		f.valueByLeaf[t2] = bitmapWith(2, 3, 4)
		f.valueByLeaf[postcode] = bitmapWith(2, 3)

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		require.Len(t, input.positives, 2)
		assert.Same(t, wrapper, input.positives[0], "wrapper stands in for tokens")
		assert.Same(t, postcode, input.positives[1])
		assert.Equal(t, []uint64{2, 3}, input.rawsByCond[wrapper].ToArray(), "AndAll of grandchildren tokens")
		assert.Equal(t, []uint64{2, 3}, input.rawsByCond[postcode].ToArray())
		assert.Empty(t, input.excludePositions)
		assert.Nil(t, input.rootAnchor)

		assert.Equal(t, 3, f.valueCalls, "2 tokens + 1 sibling leaf")
	})

	t.Run("pattern2_wrapper_with_no_grandchildren_errors", func(t *testing.T) {
		empty := tokenWrapperInner() // non-nested, zero children
		pv := outerCorrelated(empty)

		f := newFakeRecFetcher()
		_, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no grandchildren")
	})

	t.Run("direct_value_leaf_becomes_positive", func(t *testing.T) {
		leaf := valueLeaf("city", "berlin")
		pv := outerCorrelated(leaf)

		f := newFakeRecFetcher()
		raw := bitmapWith(7, 8, 9)
		f.valueByLeaf[leaf] = raw

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		require.Len(t, input.positives, 1)
		assert.Same(t, leaf, input.positives[0])
		assert.Same(t, raw, input.rawsByCond[leaf])
		assert.Empty(t, input.excludePositions)
		assert.Nil(t, input.rootAnchor)
		assert.Equal(t, 1, f.valueCalls)
		assert.Equal(t, 0, f.existsCalls)
	})

	t.Run("direct_isnull_false_uses_exists_as_positive", func(t *testing.T) {
		leaf := isNullLeaf("city", false)
		pv := outerCorrelated(leaf)

		f := newFakeRecFetcher()
		raw := bitmapWith(20, 21)
		f.existsByLeaf[leaf] = raw

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		require.Len(t, input.positives, 1)
		assert.Same(t, leaf, input.positives[0])
		assert.Same(t, raw, input.rawsByCond[leaf])
		assert.Empty(t, input.excludePositions)
		assert.Nil(t, input.rootAnchor)
		assert.Equal(t, 0, f.valueCalls, "fetchValue not called for IsNull=false")
		assert.Equal(t, 1, f.existsCalls)
		assert.Equal(t, 0, f.rootAnchorCalls, "no anchor needed when a positive exists")
	})

	t.Run("direct_isnull_true_routes_to_excludes_and_skips_anchor_if_positives_present", func(t *testing.T) {
		val := valueLeaf("postcode", "10115")
		isNull := isNullLeaf("city", true)
		pv := outerCorrelated(val, isNull)

		f := newFakeRecFetcher()
		valBM := bitmapWith(30, 31)
		nullBM := bitmapWith(31)
		f.valueByLeaf[val] = valBM
		f.existsByLeaf[isNull] = nullBM

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		require.Len(t, input.positives, 1)
		assert.Same(t, val, input.positives[0])
		assert.Same(t, valBM, input.rawsByCond[val])
		require.Len(t, input.excludePositions, 1)
		assert.Same(t, nullBM, input.excludePositions[0])
		assert.Nil(t, input.rootAnchor, "rootAnchor stays nil when a positive is present")
		assert.Equal(t, 0, f.rootAnchorCalls)
	})

	t.Run("only_isnull_true_triggers_root_anchor_fetch", func(t *testing.T) {
		isNull := isNullLeaf("city", true)
		pv := outerCorrelated(isNull)

		f := newFakeRecFetcher()
		nullBM := bitmapWith(40)
		anchorBM := bitmapWith(40, 41, 42)
		f.existsByLeaf[isNull] = nullBM
		f.rootAnchor = anchorBM

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		assert.Empty(t, input.positives)
		require.Len(t, input.excludePositions, 1)
		assert.Same(t, nullBM, input.excludePositions[0])
		require.NotNil(t, input.rootAnchor)
		assert.Same(t, anchorBM, input.rootAnchor)
		assert.Equal(t, 1, f.rootAnchorCalls)
	})

	t.Run("multiple_isnull_true_only_fetches_root_anchor_once", func(t *testing.T) {
		n1 := isNullLeaf("city", true)
		n2 := isNullLeaf("postcode", true)
		pv := outerCorrelated(n1, n2)

		f := newFakeRecFetcher()
		f.existsByLeaf[n1] = bitmapWith(50)
		f.existsByLeaf[n2] = bitmapWith(51)
		f.rootAnchor = bitmapWith(50, 51, 52)

		input, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.NoError(t, err)

		assert.Empty(t, input.positives)
		assert.Len(t, input.excludePositions, 2)
		assert.NotNil(t, input.rootAnchor)
		assert.Equal(t, 1, f.rootAnchorCalls, "anchor fetched exactly once for the whole group")
	})

	t.Run("error_in_value_fetch_releases_already_acquired_bitmaps", func(t *testing.T) {
		first := valueLeaf("city", "berlin")
		second := valueLeaf("postcode", "10115")
		pv := outerCorrelated(first, second)

		f := newFakeRecFetcher()
		f.valueByLeaf[first] = bitmapWith(60, 61)
		f.failOnLeaf = second

		_, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fetch value for")
		assert.Equal(t, 1, f.releaseCalls,
			"release for the successfully fetched first leaf must be invoked when a later fetch fails")
	})

	t.Run("error_in_exists_fetch_releases_already_acquired_bitmaps", func(t *testing.T) {
		val := valueLeaf("city", "berlin")
		nullLeaf := isNullLeaf("postcode", false)
		pv := outerCorrelated(val, nullLeaf)

		f := newFakeRecFetcher()
		f.valueByLeaf[val] = bitmapWith(70)
		f.failOnLeaf = nullLeaf

		_, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fetch exists for")
		assert.Equal(t, 1, f.releaseCalls)
	})

	t.Run("error_in_root_anchor_fetch_releases_exclude_bitmaps", func(t *testing.T) {
		isNull := isNullLeaf("city", true)
		pv := outerCorrelated(isNull)

		f := newFakeRecFetcher()
		f.existsByLeaf[isNull] = bitmapWith(80)
		f.rootAnchorErr = errors.New("anchor unavailable")

		_, err := normalizeRecGroup(ctx, pv, pv.children, f, ops, concurrency.SROAR_MERGE)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fetch root anchor")
		assert.Equal(t, 1, f.releaseCalls, "exclude bitmap released after anchor fetch fails")
	})
}
