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
	"bytes"
	"context"
	"slices"
	"testing"

	entsInverted "github.com/weaviate/weaviate/entities/inverted"

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
	"github.com/weaviate/weaviate/usecases/config/runtime"
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
		batchedContainsEnabled: runtime.NewDynamicValue(true),
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
	// Value lists that repeat, so wantKey is asked for every value the filter
	// named and the expectation has to be compacted like the builders do.
	boolDupValues := []bool{true, false, true, false}
	boolDupKey := func(t *testing.T, i int) []byte {
		want, err := s.extractBoolValue(boolDupValues[i])
		require.NoError(t, err)
		return want
	}
	intDupValues := []int{2, 2, 2}
	intDupKey := func(t *testing.T, i int) []byte {
		want, err := s.extractIntValue(intDupValues[i])
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
		value interface{}
		// numKeys is how many values wantKey can be asked for; the distinct
		// count is derived from the keys it returns, since the builders drop
		// duplicates.
		numKeys int
		wantKey func(t *testing.T, i int) []byte
	}{
		{"uuid", "prop-uuid", schema.DataTypeText, filters.ContainsAny, uuidValues, 3, uuidKey},
		{"uuid []interface{}", "prop-uuid", schema.DataTypeText, filters.ContainsAny, []interface{}{uuidValues[0], uuidValues[1], uuidValues[2]}, 3, uuidKey},
		{"text FIELD", "prop-text-field", schema.DataTypeText, filters.ContainsAll, textValues, 3, textKey},
		{"text FIELD []interface{}", "prop-text-field", schema.DataTypeText, filters.ContainsAll, []interface{}{"alpha", "beta", "gamma"}, 3, textKey},
		{"int", "prop-int", schema.DataTypeInt, filters.ContainsAny, intValues, 3, intKey},
		{"int ContainsNone", "prop-int", schema.DataTypeInt, filters.ContainsNone, intValues, 3, intKey},
		{"text FIELD ContainsNone", "prop-text-field", schema.DataTypeText, filters.ContainsNone, textValues, 3, textKey},
		// the API layers unmarshal numeric values as float64
		{"int []interface{}", "prop-int", schema.DataTypeInt, filters.ContainsAny, []interface{}{float64(1), float64(2), float64(3)}, 3, intKey},
		{"number", "prop-number", schema.DataTypeNumber, filters.ContainsAny, numberValues, 3, numberKey},
		{"number []interface{}", "prop-number", schema.DataTypeNumber, filters.ContainsAny, []interface{}{1.5, 2.5, 3.5}, 3, numberKey},
		{"bool", "prop-bool", schema.DataTypeBoolean, filters.ContainsAny, boolValues, 2, boolKey},
		{"bool []interface{}", "prop-bool", schema.DataTypeBoolean, filters.ContainsAny, []interface{}{true, false}, 2, boolKey},
		{"date", "prop-date", schema.DataTypeDate, filters.ContainsAny, dateValues, 3, dateKey},
		{"date []interface{}", "prop-date", schema.DataTypeDate, filters.ContainsAny, []interface{}{dateValues[0], dateValues[1], dateValues[2]}, 3, dateKey},
		// Four values, two distinct keys. Every other row has one key per
		// value, so without this the gate's >= 2 values and the leaf's >= 1 key
		// are never seen to disagree.
		{"bool, values repeated", "prop-bool", schema.DataTypeBoolean, filters.ContainsAny, boolDupValues, len(boolDupValues), boolDupKey},
		{"int, values repeated", "prop-int", schema.DataTypeInt, filters.ContainsAny, intDupValues, len(intDupValues), intDupKey},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := helpers.InitSlowQueryDetails(ctx)
			pv, err := s.extractContains(ctx, containsPath(tt.prop),
				tt.propType, tt.value, tt.operator, f.class)
			require.NoError(t, err)
			require.NotNil(t, pv)
			require.Nil(t, pv.children, "eligible shape must resolve batched, not desugared")
			require.Empty(t, extractContainsDesugaredReason(t, ctx),
				"an eligible shape must not annotate a desugar reason")
			require.Equal(t, tt.operator, pv.operator)
			require.Equal(t, tt.prop, pv.prop)
			require.True(t, pv.hasFilterableIndex)
			// The keys come back ascending, not in the order the filter listed
			// them, and duplicates are gone — so the expectation is built from
			// every value and then ordered and compacted the same way. The bool
			// rows are where the two orders differ.
			want := make([][]byte, tt.numKeys)
			for i := range want {
				want[i] = tt.wantKey(t, i)
			}
			slices.SortFunc(want, bytes.Compare)
			want = slices.CompactFunc(want, bytes.Equal)
			require.Equal(t, len(want), pv.containsValues.Len(),
				"one key per distinct value")
			require.Equal(t, want, collectKeys(pv.containsValues))
		})
	}
}

func TestExtractContainsBatch_Ineligible(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	// Every row must NOT resolve through the batched path and must surface
	// its decline reason in the slow-query details. Rows with wantErr expect
	// the desugared continuation to fail on its own terms (which also proves
	// the gate declined: a wrongly-accepted shape would have succeeded with
	// containsValues instead of erroring); the annotation is written before
	// the failure, so the reason is asserted on those rows too.
	tests := []struct {
		name       string
		path       *filters.Path
		propType   schema.DataType
		value      interface{}
		operator   filters.Operator
		setup      func(t *testing.T)
		wantErr    bool
		wantReason string
	}{
		{
			name:     "nested path",
			path:     &filters.Path{Property: "addresses", Child: &filters.Path{Property: "city"}},
			propType: schema.DataTypeText, value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantErr:    true, // fixture class has no "addresses" property
			wantReason: containsDeclineMultiSegmentPath,
		},
		{
			name: "internal prop",
			path: containsPath(filters.InternalPropID), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantReason: containsDeclineInternalProperty,
		},
		{
			name: "property length meta-filter, suffix spelling",
			path: containsPath("prop-int" + filters.InternalPropertyLength), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
			wantErr: true, // fixture class does not index property lengths
			// the suffix spelling is not a schema property name, so the
			// classifier declines it one check later than len()
			wantReason: containsDeclinePropertyNotFound,
		},
		{
			name: "property length meta-filter, len() spelling",
			path: containsPath("len(prop-int)"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
			wantReason: containsDeclineLengthFilter,
		},
		{
			name: "property not found",
			path: containsPath("prop-does-not-exist"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantErr:    true,
			wantReason: containsDeclinePropertyNotFound,
		},
		{
			name: "nested object property",
			path: containsPath("prop-nested"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantErr:    true, // nested filtering preview gate is off in tests
			wantReason: containsDeclineNestedObjectProperty,
		},
		{
			name: "ref prop",
			path: containsPath("prop-ref"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantErr:    true, // desugared leaf rejects text values on a reference
			wantReason: containsDeclineReferenceProperty,
		},
		{
			name: "geo prop",
			path: containsPath("prop-geo"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantErr:    true, // desugared leaf rejects text values on a geo prop
			wantReason: containsDeclineGeoProperty,
		},
		{
			name: "non-FIELD tokenization WORD",
			path: containsPath("prop-text-word"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantReason: containsDeclineTokenizationNotField,
		},
		{
			name: "non-FIELD tokenization WHITESPACE",
			path: containsPath("prop-text-whitespace"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantReason: containsDeclineTokenizationNotField,
		},
		{
			name: "fallback to searchable",
			path: containsPath("prop-fallback"), propType: schema.DataTypeText,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			setup: func(t *testing.T) {
				f.fallback = true
				t.Cleanup(func() { f.fallback = false })
			},
			wantReason: containsDeclineFallbackToSearchable,
		},
		{
			name: "IndexFilterable false",
			path: containsPath("prop-not-filterable"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
			wantErr:    true, // int props have no searchable fallback; the leaf demands the filterable index
			wantReason: containsDeclineNoFilterableIndex,
		},
		{
			name: "bucket strategy not roaringset",
			path: containsPath("prop-nonroaringset"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
			wantReason: containsDeclineNoRoaringSetBucket,
		},
		{
			name: "bucket not created",
			path: containsPath("prop-no-bucket"), propType: schema.DataTypeInt,
			value: []int{1, 2}, operator: filters.ContainsAny,
			wantReason: containsDeclineNoRoaringSetBucket,
		},
		{
			name: "N=0 values",
			path: containsPath("prop-int"), propType: schema.DataTypeInt,
			value: []int{}, operator: filters.ContainsAny,
			wantErr:    true, // the desugared path rejects an empty value set
			wantReason: containsDeclineFewerThanTwoValues,
		},
		{
			name: "N=1 value",
			path: containsPath("prop-int"), propType: schema.DataTypeInt,
			value: []int{1}, operator: filters.ContainsAny,
			wantReason: containsDeclineFewerThanTwoValues,
		},
		// array value types must decline: the desugared per-value leaf
		// extractors error on them, so accepting them in the gate would
		// succeed where the fallback path errors
		{
			name: "array value type text",
			path: containsPath("prop-text-field"), propType: schema.DataTypeTextArray,
			value: []string{"a", "b"}, operator: filters.ContainsAny,
			wantErr:    true,
			wantReason: containsDeclineValueTypeMismatch,
		},
		{
			name: "array value type int",
			path: containsPath("prop-int"), propType: schema.DataTypeIntArray,
			value: []int{1, 2}, operator: filters.ContainsAny,
			wantErr:    true,
			wantReason: containsDeclineValueTypeMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup(t)
			}
			ctx := helpers.InitSlowQueryDetails(ctx)
			pv, err := s.extractContains(ctx, tt.path, tt.propType,
				tt.value, tt.operator, f.class)
			require.Equal(t, tt.wantReason, extractContainsDesugaredReason(t, ctx),
				"decline reason must be surfaced in the slow-query details")
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, pv)
			require.Zero(t, pv.containsValues.Len(), "shape must not resolve through the batched path")
		})
	}
}

// extractContainsDesugaredReason returns the reason of the single
// "contains_desugared" slow-query annotation in ctx, or "" if none was
// written.
func extractContainsDesugaredReason(t *testing.T, ctx context.Context) string {
	t.Helper()
	details := helpers.ExtractSlowQueryDetails(ctx)
	entry, ok := details["contains_desugared"]
	if !ok {
		return ""
	}
	entries, ok := entry.([]map[string]any)
	require.True(t, ok, "contains_desugared must hold []map[string]any, got %T", entry)
	require.Len(t, entries, 1)
	reason, ok := entries[0]["reason"].(string)
	require.True(t, ok)
	return reason
}

func TestExtractContainsBatch_EncodingErrorIsEligibleButFails(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher
	ctx := context.Background()

	values := []string{
		"11111111-1111-1111-1111-111111111111",
		"not-a-valid-uuid",
	}
	pv, err := s.extractContains(ctx, containsPath("prop-uuid"),
		schema.DataTypeText, values, filters.ContainsAny, f.class)
	require.Error(t, err)
	require.ErrorContains(t, err, "extract contains values",
		"the matched shape must fail in the gate, not fall through to desugar")
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
	require.Zero(t, pv.containsValues.Len())
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
	require.Equal(t, 3, pv.containsValues.Len())
}

// TestNewBatchedContainsPair_RejectsNoKeys pins the invariant resolveDocIDs
// routes on.
//
// Routing asks how many keys the leaf holds, not whether it has any, because a
// key list has no absent value to test for — its zero value is an empty list. A
// leaf built with none would therefore route as if it were not a batched
// Contains at all, and reach the children dispatch holding no children. The
// value-count gate in extractContains keeps that unreachable; this keeps the
// two from drifting apart if the gate ever moves.
//
// One key is accepted: the builders drop duplicate values, so a filter naming
// the same value twice — or any boolean filter, which has two distinct keys to
// draw on however many values it names — legitimately arrives with one.
func TestNewBatchedContainsPair_RejectsNoKeys(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	prop := &models.Property{Name: "prop-int"}

	_, err := newBatchedContainsPair(prop, filters.ContainsAny, f.class, entsInverted.SortedKeys{})
	require.ErrorContains(t, err, "no keys")

	for _, tc := range []struct {
		name string
		keys entsInverted.SortedKeys
	}{
		{name: "one key", keys: keysFrom(t, []byte("a"))},
		{name: "two keys", keys: keysFrom(t, []byte("a"), []byte("b"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pv, err := newBatchedContainsPair(prop, filters.ContainsAny, f.class, tc.keys)
			require.NoError(t, err)
			require.Equal(t, tc.keys.Len(), pv.containsValues.Len())
		})
	}
}

// TestExtractContainsBatch_OptInGate pins that the batched resolution is
// opt-in: an unwired (nil) gate and a gate flipped off at runtime both
// route an otherwise eligible shape through the per-value desugared path,
// and flipping the gate back on at runtime restores batching without a
// searcher rebuild.
func TestExtractContainsBatch_OptInGate(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	s := f.searcher

	extract := func(ctx context.Context) (*propValuePair, error) {
		return s.extractContains(ctx, containsPath("prop-int"), schema.DataTypeInt,
			[]int{1, 2, 3}, filters.ContainsAny, f.class)
	}

	t.Run("nil gate declines", func(t *testing.T) {
		s.batchedContainsEnabled = nil
		ctx := helpers.InitSlowQueryDetails(context.Background())
		pv, err := extract(ctx)
		require.NoError(t, err)
		require.Zero(t, pv.containsValues.Len())
		require.NotEmpty(t, pv.children, "with the gate unwired, Contains must desugar per value")
		require.Equal(t, containsDeclineNotEnabled, extractContainsDesugaredReason(t, ctx))
	})

	t.Run("runtime toggle", func(t *testing.T) {
		gate := runtime.NewDynamicValue(false)
		s.batchedContainsEnabled = gate

		ctx := helpers.InitSlowQueryDetails(context.Background())
		pv, err := extract(ctx)
		require.NoError(t, err)
		require.Zero(t, pv.containsValues.Len())
		require.NotEmpty(t, pv.children, "with the gate off, Contains must desugar per value")
		require.Equal(t, containsDeclineNotEnabled, extractContainsDesugaredReason(t, ctx))

		require.NoError(t, gate.SetValue(true))
		pv, err = extract(context.Background())
		require.NoError(t, err)
		require.Equal(t, 3, pv.containsValues.Len(), "gate flipped on at runtime must batch")

		require.NoError(t, gate.SetValue(false))
		ctx = helpers.InitSlowQueryDetails(context.Background())
		pv, err = extract(ctx)
		require.NoError(t, err)
		require.Zero(t, pv.containsValues.Len(), "gate flipped off at runtime must desugar again")
		require.Equal(t, containsDeclineNotEnabled, extractContainsDesugaredReason(t, ctx))
	})
}

// TestFetchContainsBatch_EmptyKeySet pins that a batch with no keys is rejected
// rather than answered. Extraction never produces one — it batches only at two
// or more values, and every path errors rather than dropping a value — so an
// empty batch is a caller bug, and inventing a result for it would mean picking
// semantics per operator with nothing to validate them against.
func TestFetchContainsBatch_EmptyKeySet(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	ctx := context.Background()

	for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
		t.Run(op.Name(), func(t *testing.T) {
			pv := &propValuePair{
				prop:               "prop-int",
				operator:           op,
				containsValues:     keysFrom(t),
				hasFilterableIndex: true,
				Class:              f.class,
			}

			dbm, err := pv.fetchContainsBatch(ctx, f.searcher)
			require.ErrorContains(t, err, "carries no keys")
			require.ErrorContains(t, err, `"prop-int"`, "the error must name the property")
			require.Nil(t, dbm)
		})
	}
}

// TestFetchContainsBatch_BucketErrors pins the two failure paths that precede
// any read: a property with no bucket at all, and one whose bucket is not a
// roaringset (so no batch reader can be opened for it).
func TestFetchContainsBatch_BucketErrors(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		prop    string
		wantErr string
	}{
		{"no bucket", "prop-no-bucket", "not found"},
		{"non-roaringset bucket", "prop-nonroaringset", "expected, got"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pv := &propValuePair{
				prop:               tc.prop,
				operator:           filters.ContainsAny,
				containsValues:     keysFrom(t, []byte("a")),
				hasFilterableIndex: true,
				Class:              f.class,
			}

			dbm, err := pv.fetchContainsBatch(ctx, f.searcher)
			require.ErrorContains(t, err, tc.wantErr)
			require.Nil(t, dbm)
		})
	}
}

// writeContainsRows writes rows into propName's roaringset bucket and flushes
// them to a segment, so the batch reader below reads real disk layers.
func writeContainsRows(t *testing.T, f *containsBatchGateFixture, propName string, rows map[string][]uint64) {
	t.Helper()
	writeContainsRowsUnflushed(t, f, propName, rows)
	b := f.searcher.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NoError(t, b.FlushAndSwitch())
}

// writeContainsRowsUnflushed leaves the rows in the active memtable, which is
// the only state in which the reader's window read runs at all: a flushed bucket
// has an empty active memtable and the reader drops it from the view.
func writeContainsRowsUnflushed(t *testing.T, f *containsBatchGateFixture, propName string, rows map[string][]uint64) {
	t.Helper()
	b := f.searcher.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotNil(t, b)
	for key, docIDs := range rows {
		require.NoError(t, b.RoaringSetAddList([]byte(key), docIDs))
	}
}

// TestFetchContainsBatch_ReadsRows pins the wiring between key extraction and
// the fold: the bucket getBucketName resolves, the reader opened on it, and the
// folded result handed back. Fold semantics — which keys are read, absent keys,
// intersection versus union, multi-segment layouts — are pinned at the fold
// level, where the spy can observe the reads, so this covers only what the two
// layers exchange.
func TestFetchContainsBatch_ReadsRows(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	ctx := context.Background()
	writeContainsRows(t, f, "prop-int", map[string][]uint64{
		"a": {1, 2, 3},
		"b": {3, 4},
	})

	tests := []struct {
		name         string
		operator     filters.Operator
		keys         entsInverted.SortedKeys
		wantDocIDs   []uint64
		wantDenyList bool
	}{
		{"ContainsAny reaches the rows", filters.ContainsAny, keysFrom(t, []byte("a"), []byte("b")), []uint64{1, 2, 3, 4}, false},
		// ContainsNone additionally proves the fold's deny flag reaches the caller
		{"ContainsNone denies", filters.ContainsNone, keysFrom(t, []byte("a"), []byte("b")), []uint64{1, 2, 3, 4}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pv := &propValuePair{
				prop:               "prop-int",
				operator:           tc.operator,
				containsValues:     tc.keys,
				hasFilterableIndex: true,
				Class:              f.class,
			}

			dbm, err := pv.fetchContainsBatch(ctx, f.searcher)
			require.NoError(t, err)
			defer dbm.release()

			require.Equal(t, tc.wantDocIDs, dbm.docIDs.ToArray())
			require.Equal(t, tc.wantDenyList, dbm.IsDenyList())
		})
	}
}

// readerAnnotationFields are the slow-query fields only a reader can fill, so a
// filter that never opened one must carry none of them. Named once because both
// halves of that pair have to name the same set: listing them twice is how one
// drifts and the guard stops being pinned.
var readerAnnotationFields = []string{
	"window_fills", "window_keys_read",
	"window_bytes_peak", "window_bytes_copied", "memtables_read",
}

// TestFetchContainsBatch_AnnotatesSlowQueryLog pins the slow-query annotation
// for a batched read — that it fires and carries the fields an operator reads —
// and that an empty key set, which does no work, logs nothing. Where the timing
// window starts is not observable from here. The view's release is, but only as
// a consequence: a leaked view keeps a segment referenced and this test's bucket
// then hangs on Shutdown.
func TestFetchContainsBatch_AnnotatesSlowQueryLog(t *testing.T) {
	f := newContainsBatchGateFixture(t)
	writeContainsRows(t, f, "prop-int", map[string][]uint64{"a": {1, 2}, "b": {2, 3}})

	newPV := func(keys entsInverted.SortedKeys) *propValuePair {
		return &propValuePair{
			prop:               "prop-int",
			operator:           filters.ContainsAny,
			containsValues:     keys,
			hasFilterableIndex: true,
			Class:              f.class,
		}
	}

	t.Run("a batched read is annotated", func(t *testing.T) {
		ctx := helpers.InitSlowQueryDetails(context.Background())
		dbm, err := newPV(keysFrom(t, []byte("a"), []byte("b"))).fetchContainsBatch(ctx, f.searcher)
		require.NoError(t, err)
		defer dbm.release()

		entries, ok := helpers.ExtractSlowQueryDetails(ctx)["build_allow_list_doc_bitmap"].([]map[string]any)
		require.True(t, ok, "batched contains must annotate the slow query log")
		require.Len(t, entries, 1)

		require.Equal(t, "prop-int", entries[0]["prop"])
		require.Equal(t, filters.ContainsAny.Name(), entries[0]["operator"])
		require.Equal(t, 3, entries[0]["count"])
		require.Equal(t, false, entries[0]["failed"])
		require.Equal(t, 2, entries[0]["batched_keys"])
		require.Contains(t, entries[0], "took")
		require.Contains(t, entries[0], "took_string")
	})

	// What the batching itself did, which is what separates a batched filter that
	// went slow from one that merely ran during a slow query. On its own fixture
	// because the shared one flushes, and a reader over a flushed bucket drops the
	// active memtable, so the fields that count memtable work all read zero.
	t.Run("the annotation carries the reader's work", func(t *testing.T) {
		g := newContainsBatchGateFixture(t)
		writeContainsRowsUnflushed(t, g, "prop-int", map[string][]uint64{"a": {1, 2}, "b": {2, 3}})

		ctx := helpers.InitSlowQueryDetails(context.Background())
		pv := &propValuePair{
			prop:               "prop-int",
			operator:           filters.ContainsAny,
			containsValues:     keysFrom(t, []byte("a"), []byte("b")),
			hasFilterableIndex: true,
			Class:              g.class,
		}
		dbm, err := pv.fetchContainsBatch(ctx, g.searcher)
		require.NoError(t, err)
		defer dbm.release()

		entries, ok := helpers.ExtractSlowQueryDetails(ctx)["build_allow_list_doc_bitmap"].([]map[string]any)
		require.True(t, ok)
		require.Len(t, entries, 1)

		require.Equal(t, 1, entries[0]["memtables_read"], "the unflushed rows must be read from the active memtable")
		require.Equal(t, 2, entries[0]["window_keys_read"], "both keys were folded")
		// A bound, not a count: how many windows a batch costs is pinned in
		// TestBatchReaderStatsReportTheWork. What matters here is that the four
		// are wired through at all, which zeros would not show.
		require.GreaterOrEqual(t, entries[0]["window_fills"], 1)
		require.Greater(t, entries[0]["window_bytes_peak"], 0)
		require.Greater(t, entries[0]["window_bytes_copied"], 0)
	})

	// The annotation counts keys, not the values the filter named, and the two
	// stop agreeing the moment a filter repeats a value. Asserted on a shape
	// where they differ, since a fixture with distinct values reports the same
	// number either way and would not notice the field changing meaning.
	t.Run("the annotation counts distinct keys, not values", func(t *testing.T) {
		ctx := helpers.InitSlowQueryDetails(context.Background())
		keys, err := encodeBoolKeys([]bool{true, false, true, false})
		require.NoError(t, err)
		require.Equal(t, 2, keys.Len(), "four values, two distinct boolean keys")

		pv := newPV(keys)
		pv.prop = "prop-bool"
		writeContainsRows(t, f, "prop-bool", map[string][]uint64{"\x00": {1}, "\x01": {2}})
		dbm, err := pv.fetchContainsBatch(ctx, f.searcher)
		require.NoError(t, err)
		defer dbm.release()

		entries := helpers.ExtractSlowQueryDetails(ctx)["build_allow_list_doc_bitmap"].([]map[string]any)
		require.Equal(t, 2, entries[0]["batched_keys"], "four values must report two keys")
	})

	t.Run("a bucket rejected at open is still annotated", func(t *testing.T) {
		ctx := helpers.InitSlowQueryDetails(context.Background())
		pv := &propValuePair{
			prop:               "prop-nonroaringset",
			operator:           filters.ContainsAny,
			containsValues:     keysFrom(t, []byte("a"), []byte("b")),
			hasFilterableIndex: true,
			Class:              f.class,
		}

		_, err := pv.fetchContainsBatch(ctx, f.searcher)
		require.Error(t, err)

		entries, ok := helpers.ExtractSlowQueryDetails(ctx)["build_allow_list_doc_bitmap"].([]map[string]any)
		require.True(t, ok, "a filter rejected while opening the reader must still be timed")
		require.Len(t, entries, 1)
		require.Equal(t, "prop-nonroaringset", entries[0]["prop"])
		require.Equal(t, 0, entries[0]["count"])
		require.Equal(t, true, entries[0]["failed"],
			"a zero count means nothing without this: an empty result looks identical")
		// No reader was built, so the reader's own fields must be absent rather
		// than present and zero — which is what the guard around them is for.
		for _, k := range readerAnnotationFields {
			require.NotContains(t, entries[0], k,
				"a filter that never opened a reader must not report the reader's work")
		}
	})

	t.Run("a fold that fails after the reader opened is still annotated", func(t *testing.T) {
		// distinct from the case above: there the strategy check rejects the
		// bucket before a view is ever taken, so nothing has been timed yet
		ctx, cancel := context.WithCancel(helpers.InitSlowQueryDetails(context.Background()))
		cancel()

		_, err := newPV(keysFrom(t, []byte("a"), []byte("b"))).fetchContainsBatch(ctx, f.searcher)
		require.ErrorIs(t, err, context.Canceled)

		entries, ok := helpers.ExtractSlowQueryDetails(ctx)["build_allow_list_doc_bitmap"].([]map[string]any)
		require.True(t, ok, "a filter that opens the reader and then fails must still be timed")
		require.Len(t, entries, 1)
		require.Equal(t, "prop-int", entries[0]["prop"])
		require.Equal(t, 0, entries[0]["count"])
		require.Equal(t, true, entries[0]["failed"])
		// The reader was opened, so its fields are present where the case above
		// has them absent. That pair is what holds the guard in place, and it is
		// the only thing proving the reader is read on the failing path at all.
		// The values are not asserted: this fixture flushes, so what the reader
		// found is beside the point here.
		for _, k := range readerAnnotationFields {
			require.Contains(t, entries[0], k,
				"a fold that opened a reader must report its work even when it failed")
		}
	})

	// Both returns that precede the timer: nothing has been done, so there is no
	// duration worth logging.
	for _, tc := range []struct {
		name string
		prop string
		keys entsInverted.SortedKeys
	}{
		{name: "an empty key set is not annotated", prop: "prop-int"},
		{name: "a missing bucket is not annotated", prop: "prop-no-bucket", keys: keysFrom(t, []byte("a"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := helpers.InitSlowQueryDetails(context.Background())
			pv := &propValuePair{
				prop:               tc.prop,
				operator:           filters.ContainsAny,
				containsValues:     tc.keys,
				hasFilterableIndex: true,
				Class:              f.class,
			}

			_, err := pv.fetchContainsBatch(ctx, f.searcher)
			require.Error(t, err)
			require.NotContains(t, helpers.ExtractSlowQueryDetails(ctx), "build_allow_list_doc_bitmap")
		})
	}
}
