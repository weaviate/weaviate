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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
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
			assert.Equal(t, tt.out, dispatchableProperties(tt.in))
		})
	}
}

func TestValidateCardinalityOnlyProperties(t *testing.T) {
	class := &models.Class{
		Class: "MyClass",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "location", DataType: []string{"geoCoordinates"}},
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
			// cardinality is a bucket key count, so a type no aggregator supports is fine
			name:         "non-aggregatable cardinality-only property",
			props:        []aggregation.ParamProperty{{Name: "location", ApproximateCardinality: true}},
			expectLookup: true,
		},
		{
			// the normal dispatch path validates this one via aggTypeOfProperty
			name: "unknown property that also has aggregators",
			props: []aggregation.ParamProperty{
				{Name: "titel", ApproximateCardinality: true, Aggregators: []aggregation.Aggregator{{Type: "count"}}},
			},
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

			err := a.validateCardinalityOnlyProperties()
			if tt.errContains == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}
