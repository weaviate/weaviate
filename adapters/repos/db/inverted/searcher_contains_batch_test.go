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
	entcfg "github.com/weaviate/weaviate/entities/config"
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

	uuidValues := []string{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
	}
	uuidKey := func(t *testing.T, i int) []byte {
		want, err := s.extractUUIDValue(uuidValues[i])
		require.NoError(t, err)
		return want
	}
	textValues := []string{"alpha", "beta", "gamma"}
	textKey := func(t *testing.T, i int) []byte {
		prepared := tokenizer.NewPreparedAnalyzer(nil)
		result := tokenizer.Analyze(textValues[i], models.PropertyTokenizationField, f.class.Class, prepared, nil)
		return []byte(result.Query[0])
	}
	intValues := []int{1, 2, 3}
	intKey := func(t *testing.T, i int) []byte {
		want, err := s.extractIntValue(intValues[i])
		require.NoError(t, err)
		return want
	}
	numberValues := []float64{1.5, 2.5, 3.5}
	numberKey := func(t *testing.T, i int) []byte {
		want, err := s.extractNumberValue(numberValues[i])
		require.NoError(t, err)
		return want
	}
	boolValues := []bool{true, false}
	boolKey := func(t *testing.T, i int) []byte {
		want, err := s.extractBoolValue(boolValues[i])
		require.NoError(t, err)
		return want
	}
	dateValues := []string{"2021-01-01T00:00:00Z", "2022-02-02T00:00:00Z", "2023-03-03T00:00:00Z"}
	dateKey := func(t *testing.T, i int) []byte {
		want, err := s.extractDateValue(dateValues[i])
		require.NoError(t, err)
		return want
	}

	tests := []struct {
		name     string
		prop     string
		propType schema.DataType
		operator filters.Operator
		// value is what the filter layer hands over: typed slices from
		// internal callers, []interface{} from the GraphQL/gRPC layer
		value   interface{}
		numVals int
		wantKey func(t *testing.T, i int) []byte
	}{
		{"uuid", "prop-uuid", schema.DataTypeText, filters.ContainsAny, uuidValues, 3, uuidKey},
		{"uuid []interface{}", "prop-uuid", schema.DataTypeText, filters.ContainsAny, []interface{}{uuidValues[0], uuidValues[1], uuidValues[2]}, 3, uuidKey},
		{"text FIELD", "prop-text-field", schema.DataTypeText, filters.ContainsAll, textValues, 3, textKey},
		{"text FIELD []interface{}", "prop-text-field", schema.DataTypeText, filters.ContainsAll, []interface{}{"alpha", "beta", "gamma"}, 3, textKey},
		{"int", "prop-int", schema.DataTypeInt, filters.ContainsAny, intValues, 3, intKey},
		// the API layers unmarshal numeric values as float64
		{"int []interface{}", "prop-int", schema.DataTypeInt, filters.ContainsAny, []interface{}{float64(1), float64(2), float64(3)}, 3, intKey},
		{"number", "prop-number", schema.DataTypeNumber, filters.ContainsAny, numberValues, 3, numberKey},
		{"number []interface{}", "prop-number", schema.DataTypeNumber, filters.ContainsAny, []interface{}{1.5, 2.5, 3.5}, 3, numberKey},
		{"bool", "prop-bool", schema.DataTypeBoolean, filters.ContainsAny, boolValues, 2, boolKey},
		{"bool []interface{}", "prop-bool", schema.DataTypeBoolean, filters.ContainsAny, []interface{}{true, false}, 2, boolKey},
		{"date", "prop-date", schema.DataTypeDate, filters.ContainsAny, dateValues, 3, dateKey},
		{"date []interface{}", "prop-date", schema.DataTypeDate, filters.ContainsAny, []interface{}{dateValues[0], dateValues[1], dateValues[2]}, 3, dateKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pv, eligible, err := s.extractContainsBatch(ctx, containsPath(tt.prop),
				tt.propType, tt.value, tt.operator, f.class)
			require.NoError(t, err)
			require.True(t, eligible)
			require.NotNil(t, pv)
			require.Equal(t, tt.operator, pv.operator)
			require.Equal(t, tt.prop, pv.prop)
			require.True(t, pv.hasFilterableIndex)
			require.Len(t, pv.containsValues, tt.numVals)
			for i := 0; i < tt.numVals; i++ {
				require.Equal(t, tt.wantKey(t, i), pv.containsValues[i], "key %d", i)
			}
		})
	}
}

func TestExtractContainsBatch_Ineligible(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	tests := []struct {
		name     string
		path     *filters.Path
		propType schema.DataType
		value    interface{}
		operator filters.Operator
		setup    func(t *testing.T)
	}{
		{
			name: "ContainsNone is out of scope",
			path: containsPath("prop-int"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsNone,
		},
		{
			name:     "nested path",
			path:     &filters.Path{Property: "addresses", Child: &filters.Path{Property: "city"}},
			propType: schema.DataTypeText, value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "internal prop",
			path: containsPath(filters.InternalPropID), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "property length meta-filter",
			path: containsPath("prop-int" + filters.InternalPropertyLength), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
		},
		{
			name: "property not found",
			path: containsPath("prop-does-not-exist"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "nested object property",
			path: containsPath("prop-nested"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "ref prop",
			path: containsPath("prop-ref"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "geo prop",
			path: containsPath("prop-geo"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "non-FIELD tokenization WORD",
			path: containsPath("prop-text-word"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "non-FIELD tokenization WHITESPACE",
			path: containsPath("prop-text-whitespace"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "fallback to searchable",
			path: containsPath("prop-fallback"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			setup: func(t *testing.T) {
				f.fallback = true
				t.Cleanup(func() { f.fallback = false })
			},
		},
		{
			name: "IndexFilterable false",
			path: containsPath("prop-not-filterable"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
		},
		{
			name: "bucket strategy not roaringset",
			path: containsPath("prop-nonroaringset"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
		},
		{
			name: "bucket not created",
			path: containsPath("prop-no-bucket"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
		},
		{
			name: "N=0 values",
			path: containsPath("prop-int"), propType: schema.DataTypeInt,
			value: []int{}, operator: filters.ContainsAny,
		},
		{
			name: "N=1 value",
			path: containsPath("prop-int"), propType: schema.DataTypeInt,
			value: []int{1}, operator: filters.ContainsAny,
		},
		// array value types must decline: the desugared per-value leaf
		// extractors error on them, so accepting them here would succeed
		// where the fallback path errors
		{
			name: "array value type text",
			path: containsPath("prop-text-field"), propType: schema.DataTypeTextArray,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
		},
		{
			name: "array value type int",
			path: containsPath("prop-int"), propType: schema.DataTypeIntArray,
			value: []int{1, 2}, operator: filters.ContainsAny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup(t)
			}
			pv, eligible, err := s.extractContainsBatch(ctx, tt.path, tt.propType,
				tt.value, tt.operator, f.class)
			require.False(t, eligible)
			require.NoError(t, err)
			require.Nil(t, pv)
		})
	}
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

// TestExtractContainsBatch_KillSwitch pins the operator escape hatch: with
// WEAVIATE_DISABLE_BATCHED_CONTAINS set, an otherwise eligible shape
// declines and extractContains resolves through the per-value desugared
// path.
func TestExtractContainsBatch_KillSwitch(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	t.Setenv(entcfg.EnvDisableBatchedContains, "true")

	pv, eligible, err := s.extractContainsBatch(ctx, containsPath("prop-int"),
		schema.DataTypeInt, []int{1, 2, 3}, filters.ContainsAny, f.class)
	require.False(t, eligible)
	require.NoError(t, err)
	require.Nil(t, pv)

	pv, err = s.extractContains(ctx, containsPath("prop-int"), schema.DataTypeInt,
		[]int{1, 2, 3}, filters.ContainsAny, f.class)
	require.NoError(t, err)
	require.Nil(t, pv.containsValues)
	require.NotEmpty(t, pv.children, "with the kill switch on, Contains must desugar per value")
}
