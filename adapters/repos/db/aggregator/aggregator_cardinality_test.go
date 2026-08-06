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
			// bare has no aggregators either yet stays: only the flag makes a
			// property dispatch-free
			name: "cardinality-only property dropped",
			in:   []aggregation.ParamProperty{card, count, bare},
			out:  []aggregation.ParamProperty{count, bare},
		},
		{
			name: "cardinality alongside aggregators stays",
			in:   []aggregation.ParamProperty{cardAndCount, count},
			out:  []aggregation.ParamProperty{cardAndCount, count},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := slices.Clone(tt.in)
			assert.Equal(t, tt.out, dispatchableProperties(tt.in))
			assert.Equal(t, in, tt.in, "input must not be mutated")
		})
	}
}

// Under group_by the flag is ignored, so validation never runs: a property that
// validation would reject still reaches dispatch, where the grouper rejects the
// cross-ref path before it reads anything.
func TestDoIgnoresCardinalityUnderGroupBy(t *testing.T) {
	a := &Aggregator{
		// no ReadOnlyClass expectation: a validation lookup fails the test
		getSchema: schemaUC.NewMockSchemaGetter(t),
		params: aggregation.Params{
			ClassName:  "MyClass",
			Properties: []aggregation.ParamProperty{{Name: "titel", ApproximateCardinality: true}},
			GroupBy: &filters.Path{
				Class:    "MyClass",
				Property: "ofClass",
				Child:    &filters.Path{Class: "OtherClass", Property: "name"},
			},
		},
	}

	_, err := a.Do(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "grouping by cross-refs not supported")
}

func TestValidateCardinalityProperties(t *testing.T) {
	falsePtr := new(bool)

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
		},
	}

	tests := []struct {
		name         string
		props        []aggregation.ParamProperty
		classMissing bool
		errContains  string
	}{
		{
			name:  "supported property",
			props: []aggregation.ParamProperty{{Name: "title", ApproximateCardinality: true}},
		},
		{
			// aggregators alongside the flag must not exempt the property
			name: "unknown property that also has aggregators",
			props: []aggregation.ParamProperty{
				{Name: "titel", ApproximateCardinality: true, Aggregators: []aggregation.Aggregator{{Type: "count"}}},
			},
			errContains: "titel",
		},
		{
			// geo values are indexed in a dedicated geo index, so the filterable
			// index this property nominally has holds none of them
			name:        "indexed but never analyzed into a countable bucket",
			props:       []aggregation.ParamProperty{{Name: "location", ApproximateCardinality: true}},
			errContains: noIndex,
		},
		{
			name:        "both inverted indexes disabled",
			props:       []aggregation.ParamProperty{{Name: "notes", ApproximateCardinality: true}},
			errContains: noIndex,
		},
		{
			name:         "class not in schema",
			props:        []aggregation.ParamProperty{{Name: "title", ApproximateCardinality: true}},
			classMissing: true,
			errContains:  "could not find class MyClass",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			found := class
			if tt.classMissing {
				found = nil
			}
			getSchema := schemaUC.NewMockSchemaGetter(t)
			getSchema.EXPECT().ReadOnlyClass("MyClass").Return(found).Once()

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
