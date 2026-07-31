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

package aggregator

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func TestDispatchableProperties(t *testing.T) {
	card := aggregation.ParamProperty{Name: "card", ApproximateCardinality: true}
	cardAndCount := aggregation.ParamProperty{
		Name: "cardAndCount", ApproximateCardinality: true,
		Aggregators: []aggregation.Aggregator{{Type: "count"}},
	}
	count := aggregation.ParamProperty{Name: "count", Aggregators: []aggregation.Aggregator{{Type: "count"}}}
	bare := aggregation.ParamProperty{Name: "bare"}

	tests := []struct {
		name string
		in   []aggregation.ParamProperty
		out  []aggregation.ParamProperty
	}{
		{
			name: "cardinality-only property dropped",
			in:   []aggregation.ParamProperty{card, count},
			out:  []aggregation.ParamProperty{count},
		},
		{
			name: "cardinality alongside aggregators stays",
			in:   []aggregation.ParamProperty{cardAndCount, count},
			out:  []aggregation.ParamProperty{cardAndCount, count},
		},
		{
			// a sibling without aggregators still aggregates by type, exactly as
			// it would in a request with no cardinality flag anywhere
			name: "bare sibling stays",
			in:   []aggregation.ParamProperty{card, bare},
			out:  []aggregation.ParamProperty{bare},
		},
		{
			name: "no cardinality: all pass through",
			in:   []aggregation.ParamProperty{count, bare},
			out:  []aggregation.ParamProperty{count, bare},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := slices.Clone(tt.in)
			got := dispatchableProperties(tt.in)
			assert.Equal(t, tt.out, got)
			assert.Equal(t, in, tt.in, "input must not be mutated")
			if len(got) > 0 && len(tt.in) > 0 {
				// the result is handed to a shallow Aggregator copy that owns its
				// property list, so it must never share the caller's backing array
				assert.NotSame(t, &tt.in[0], &got[0], "result must be a fresh slice")
			}
		})
	}
}

// Under group_by the cardinality flag is ignored: the request dispatches as if
// it were absent, so validation never runs and the cardinality-only
// short-circuit never fires. Grouping by a cross-ref path is rejected inside
// the grouper before it reads anything, which makes reaching dispatch
// observable without a store.
func TestDoIgnoresCardinalityUnderGroupBy(t *testing.T) {
	crossRef := &filters.Path{
		Class:    "MyClass",
		Property: "ofClass",
		Child:    &filters.Path{Class: "OtherClass", Property: "name"},
	}

	tests := []struct {
		name  string
		props []aggregation.ParamProperty
	}{
		{
			// would be rejected by validation: geo values reach no countable bucket
			name:  "cardinality-only property of an unsupported type",
			props: []aggregation.ParamProperty{{Name: "location", ApproximateCardinality: true}},
		},
		{
			// would be rejected by validation: not in the schema
			name:  "cardinality on an unknown property",
			props: []aggregation.ParamProperty{{Name: "titel", ApproximateCardinality: true}},
		},
		{
			name: "cardinality alongside aggregators",
			props: []aggregation.ParamProperty{
				{Name: "title", ApproximateCardinality: true, Aggregators: []aggregation.Aggregator{{Type: "count"}}},
			},
		},
		{
			name:  "no cardinality requested",
			props: []aggregation.ParamProperty{{Name: "title", Aggregators: []aggregation.Aggregator{{Type: "count"}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// no ReadOnlyClass expectation: a validation lookup fails the test
			a := &Aggregator{
				getSchema: schemaUC.NewMockSchemaGetter(t),
				params: aggregation.Params{
					ClassName:  "MyClass",
					Properties: tt.props,
					GroupBy:    crossRef,
				},
			}

			_, err := a.Do(context.Background())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "grouping by cross-refs not supported")
		})
	}
}

func TestValidateCardinalityProperties(t *testing.T) {
	falsePtr, truePtr := new(bool), new(bool)
	*truePtr = true

	const noIndex = "approximate cardinality requires a filterable or searchable inverted index"

	class := &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "location", DataType: []string{"geoCoordinates"}},
			{
				Name: "notes", DataType: []string{"text"},
				IndexFilterable: falsePtr, IndexSearchable: falsePtr,
			},
			{
				Name: "price", DataType: []string{"number"},
				IndexFilterable: falsePtr, IndexRangeFilters: truePtr,
			},
			{Name: "image", DataType: []string{"blob"}},
			{Name: "meta", DataType: []string{"object"}},
			{Name: "ofClass", DataType: []string{"OtherClass"}},
			{Name: "ident", DataType: []string{"uuid"}},
		},
	}

	tests := []struct {
		name string
		// no lookup expectation is registered unless expectLookup is set, so an
		// unwanted ReadOnlyClass call fails the test
		props        []aggregation.ParamProperty
		expectLookup bool
		classMissing bool
		errContains  string
	}{
		{
			name:         "known cardinality-only property",
			props:        []aggregation.ParamProperty{{Name: "title", ApproximateCardinality: true}},
			expectLookup: true,
		},
		{
			name:         "unknown cardinality-only property",
			props:        []aggregation.ParamProperty{{Name: "titel", ApproximateCardinality: true}},
			expectLookup: true,
			errContains:  "titel",
		},
		{
			// geo values are indexed in a dedicated geo index, so no bucket holds
			// their distinct values and an estimate would confidently report 0
			name:         "geo property",
			props:        []aggregation.ParamProperty{{Name: "location", ApproximateCardinality: true}},
			expectLookup: true,
			errContains:  noIndex,
		},
		{
			name:         "text property with both inverted indexes disabled",
			props:        []aggregation.ParamProperty{{Name: "notes", ApproximateCardinality: true}},
			expectLookup: true,
			errContains:  noIndex,
		},
		{
			// the rangeable bucket stores per-bit bitmaps, not the values themselves
			name:         "rangeable-only number property",
			props:        []aggregation.ParamProperty{{Name: "price", ApproximateCardinality: true}},
			expectLookup: true,
			errContains:  noIndex,
		},
		{
			name:         "blob property",
			props:        []aggregation.ParamProperty{{Name: "image", ApproximateCardinality: true}},
			expectLookup: true,
			errContains:  noIndex,
		},
		{
			name:         "object property",
			props:        []aggregation.ParamProperty{{Name: "meta", ApproximateCardinality: true}},
			expectLookup: true,
			errContains:  noIndex,
		},
		{
			name:         "reference property",
			props:        []aggregation.ParamProperty{{Name: "ofClass", ApproximateCardinality: true}},
			expectLookup: true,
		},
		{
			name:         "uuid property",
			props:        []aggregation.ParamProperty{{Name: "ident", ApproximateCardinality: true}},
			expectLookup: true,
		},
		{
			name: "unknown property that also has aggregators",
			props: []aggregation.ParamProperty{
				{Name: "titel", ApproximateCardinality: true, Aggregators: []aggregation.Aggregator{{Type: "count"}}},
			},
			expectLookup: true,
			errContains:  "titel",
		},
		{
			name:  "no cardinality requested",
			props: []aggregation.ParamProperty{{Name: "titel"}},
		},
		{
			name:         "class not in schema",
			props:        []aggregation.ParamProperty{{Name: "title", ApproximateCardinality: true}},
			expectLookup: true,
			classMissing: true,
			errContains:  "could not find class MyClass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getSchema := schemaUC.NewMockSchemaGetter(t)
			if tt.expectLookup {
				found := class
				if tt.classMissing {
					found = nil
				}
				getSchema.EXPECT().ReadOnlyClass("MyClass").Return(found).Once()
			}

			a := &Aggregator{
				getSchema: getSchema,
				params: aggregation.Params{
					ClassName:  "MyClass",
					Properties: tt.props,
				},
			}

			err := a.validateCardinalityProperties()
			if tt.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}
