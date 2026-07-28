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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

const containsBatchTestClass = "ContainsBatchGateTest"

// containsBatchGateFixture wires one Searcher/Store against a class carrying
// one property per gate branch under test, each with a matching (or
// deliberately mismatching) LSM bucket.
type containsBatchGateFixture struct {
	searcher *Searcher
	class    *models.Class
	fallback bool // read by the Searcher's isFallbackToSearchable closure
}

func newContainsBatchGateFixture(t *testing.T) *containsBatchGateFixture {
	t.Helper()
	dir := t.TempDir()
	logger := logrus.New()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Shutdown(context.Background())) })

	vTrue, vFalse := true, false
	class := &models.Class{
		Class: containsBatchTestClass,
		Properties: []*models.Property{
			{Name: "prop-uuid", DataType: schema.DataTypeUUID.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-text-field", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationField, IndexFilterable: &vTrue},
			{Name: "prop-int", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-number", DataType: schema.DataTypeNumber.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-bool", DataType: schema.DataTypeBoolean.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-date", DataType: schema.DataTypeDate.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-text-word", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWord, IndexFilterable: &vTrue},
			{Name: "prop-text-whitespace", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace, IndexFilterable: &vTrue},
			{Name: "prop-fallback", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationField, IndexFilterable: &vTrue, IndexSearchable: &vTrue},
			{Name: "prop-not-filterable", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vFalse, IndexSearchable: &vTrue},
			{Name: "prop-nonroaringset", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-no-bucket", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &vTrue},
			{Name: "prop-ref", DataType: []string{"SomeOtherClass"}},
			{Name: "prop-geo", DataType: schema.DataTypeGeoCoordinates.PropString()},
			{Name: "prop-nested", DataType: schema.DataTypeObject.PropString()},
		},
	}

	ctx := context.Background()
	roaringProps := []string{
		"prop-uuid", "prop-text-field", "prop-int", "prop-number", "prop-bool",
		"prop-date", "prop-text-word", "prop-text-whitespace", "prop-fallback",
		"prop-not-filterable",
	}
	for _, propName := range roaringProps {
		require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(propName),
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop())))
	}
	// deliberately not roaringset: simulates a filterable index backed by a
	// different (e.g. not-yet-migrated) bucket strategy
	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM("prop-nonroaringset"),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	// "prop-no-bucket" deliberately has no backing bucket at all

	f := &containsBatchGateFixture{class: class}
	f.searcher = &Searcher{
		store:                  store,
		logger:                 logger,
		getClass:               func(name string) *models.Class { return f.class },
		isFallbackToSearchable: func() bool { return f.fallback },
		stopwordProvider:       stopwords.NewProvider(fakeStopwordDetector{}, nil),
	}
	return f
}

func containsPath(propName string) *filters.Path {
	return &filters.Path{Property: schema.PropertyName(propName)}
}

func TestExtractContainsBatch_EligibleFamilies(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	t.Run("uuid", func(t *testing.T) {
		values := []string{
			"11111111-1111-1111-1111-111111111111",
			"22222222-2222-2222-2222-222222222222",
			"33333333-3333-3333-3333-333333333333",
		}
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-uuid"),
			schema.DataTypeText, values, filters.ContainsAny, f.class)
		require.NoError(t, err)
		require.True(t, eligible)
		require.NotNil(t, pv)
		require.Equal(t, filters.ContainsAny, pv.operator)
		require.Equal(t, "prop-uuid", pv.prop)
		require.True(t, pv.hasFilterableIndex)
		require.Len(t, pv.containsValues, len(values))
		for i, v := range values {
			want, err := s.extractUUIDValue(v)
			require.NoError(t, err)
			require.Equal(t, want, pv.containsValues[i])
		}
	})

	t.Run("text FIELD", func(t *testing.T) {
		values := []string{"alpha", "beta", "gamma"}
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-text-field"),
			schema.DataTypeText, values, filters.ContainsAll, f.class)
		require.NoError(t, err)
		require.True(t, eligible)
		require.NotNil(t, pv)
		require.Equal(t, filters.ContainsAll, pv.operator)
		require.Len(t, pv.containsValues, len(values))
		prepared := tokenizer.NewPreparedAnalyzer(nil)
		for i, v := range values {
			result := tokenizer.Analyze(v, models.PropertyTokenizationField, f.class.Class, prepared, nil)
			require.Equal(t, []byte(result.Query[0]), pv.containsValues[i])
		}
	})

	t.Run("int", func(t *testing.T) {
		values := []int{1, 2, 3}
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-int"),
			schema.DataTypeInt, values, filters.ContainsAny, f.class)
		require.NoError(t, err)
		require.True(t, eligible)
		require.Len(t, pv.containsValues, len(values))
		for i, v := range values {
			want, err := s.extractIntValue(v)
			require.NoError(t, err)
			require.Equal(t, want, pv.containsValues[i])
		}
	})

	t.Run("number", func(t *testing.T) {
		values := []float64{1.5, 2.5, 3.5}
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-number"),
			schema.DataTypeNumber, values, filters.ContainsAny, f.class)
		require.NoError(t, err)
		require.True(t, eligible)
		require.Len(t, pv.containsValues, len(values))
		for i, v := range values {
			want, err := s.extractNumberValue(v)
			require.NoError(t, err)
			require.Equal(t, want, pv.containsValues[i])
		}
	})

	t.Run("bool", func(t *testing.T) {
		values := []bool{true, false}
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-bool"),
			schema.DataTypeBoolean, values, filters.ContainsAny, f.class)
		require.NoError(t, err)
		require.True(t, eligible)
		require.Len(t, pv.containsValues, len(values))
		for i, v := range values {
			want, err := s.extractBoolValue(v)
			require.NoError(t, err)
			require.Equal(t, want, pv.containsValues[i])
		}
	})

	t.Run("date", func(t *testing.T) {
		values := []string{"2021-01-01T00:00:00Z", "2022-02-02T00:00:00Z", "2023-03-03T00:00:00Z"}
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-date"),
			schema.DataTypeDate, values, filters.ContainsAny, f.class)
		require.NoError(t, err)
		require.True(t, eligible)
		require.Len(t, pv.containsValues, len(values))
		for i, v := range values {
			want, err := s.extractDateValue(v)
			require.NoError(t, err)
			require.Equal(t, want, pv.containsValues[i])
		}
	})
}

func TestExtractContainsBatch_Ineligible(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	assertFallThrough := func(t *testing.T, pv *propValuePair, eligible bool, err error) {
		t.Helper()
		require.False(t, eligible)
		require.NoError(t, err)
		require.Nil(t, pv)
	}

	t.Run("ContainsNone is out of scope", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-int"),
			schema.DataTypeInt, []int{1, 2}, filters.ContainsNone, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("nested path", func(t *testing.T) {
		path := &filters.Path{Property: "addresses", Child: &filters.Path{Property: "city"}}
		pv, eligible, err := s.extractContainsBatch(ctx, path,
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("internal prop", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath(filters.InternalPropID),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("property length meta-filter", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-int"+filters.InternalPropertyLength),
			schema.DataTypeInt, []int{1, 2}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("property not found", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-does-not-exist"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("nested object property", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-nested"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("ref prop", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-ref"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("geo prop", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-geo"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("non-FIELD tokenization WORD", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-text-word"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("non-FIELD tokenization WHITESPACE", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-text-whitespace"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("fallback to searchable", func(t *testing.T) {
		f.fallback = true
		t.Cleanup(func() { f.fallback = false })
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-fallback"),
			schema.DataTypeText, []string{"a", "b"}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("IndexFilterable false", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-not-filterable"),
			schema.DataTypeInt, []int{1, 2}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("bucket strategy not roaringset", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-nonroaringset"),
			schema.DataTypeInt, []int{1, 2}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("bucket not created", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-no-bucket"),
			schema.DataTypeInt, []int{1, 2}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("N=0 values", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-int"),
			schema.DataTypeInt, []int{}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})

	t.Run("N=1 value", func(t *testing.T) {
		pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-int"),
			schema.DataTypeInt, []int{1}, filters.ContainsAny, f.class)
		assertFallThrough(t, pv, eligible, err)
	})
}

func TestExtractContainsBatch_EncodingErrorIsEligibleButFails(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	values := []string{
		"11111111-1111-1111-1111-111111111111",
		"not-a-valid-uuid",
	}
	pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-uuid"),
		schema.DataTypeText, values, filters.ContainsAny, f.class)
	require.True(t, eligible)
	require.Error(t, err)
	require.Nil(t, pv)
}

// TestExtractContains_FallsThroughToPerValuePath proves that once the gate
// declines a shape, extractContains's existing per-value dispatch still
// runs unchanged, producing children (not containsValues).
func TestExtractContains_FallsThroughToPerValuePath(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	path := &filters.Path{Property: "prop-text-word"}
	pv, err := s.extractContains(ctx, path, schema.DataTypeText, []string{"hello world", "goodbye"},
		filters.ContainsAny, f.class)
	require.NoError(t, err)
	require.Nil(t, pv.containsValues)
	require.NotEmpty(t, pv.children)
}

// TestExtractContains_UsesBatchedPathWhenEligible proves the interception at
// the top of extractContains actually wires extractContainsBatch's result
// through, rather than always falling back to the desugared body.
func TestExtractContains_UsesBatchedPathWhenEligible(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	pv, err := s.extractContains(ctx, containsPath("prop-int"), schema.DataTypeInt,
		[]int{1, 2, 3}, filters.ContainsAny, f.class)
	require.NoError(t, err)
	require.NotNil(t, pv)
	require.Nil(t, pv.children)
	require.Len(t, pv.containsValues, 3)
}
