//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package common_filters

import (
	"testing"

	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/location"
	test_helper "github.com/semi-technologies/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// Basic test on filter
func TestExtractFilterToplevelField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()
	/*localfilter is a struct containing a clause struct
		type filters.Clause struct {
		Operator Operator
		On       *filters.Path
		filters.Value    *filters.Value
		Operands []filters.Clause
	}*/
	expectedParams := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("intField"),
		},
		Value: &filters.Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: { path: ["intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterLike(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()
	expectedParams := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorLike,
		On: &filters.Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("name"),
		},
		Value: &filters.Value{
			Value: "Schn*el",
			Type:  schema.DataTypeString,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: {
			path: ["name"],
			operator: Like,
			valueString: "Schn*el",
		}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterLike_ValueText(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()
	expectedParams := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorLike,
		On: &filters.Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("name"),
		},
		Value: &filters.Value{
			Value: "schn*el",
			Type:  schema.DataTypeText,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: {
			path: ["name"],
			operator: Like,
			valueText: "schn*el",
		}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterGeoLocation(t *testing.T) {
	t.Parallel()

	t.Run("with all fields set as required", func(t *testing.T) {
		resolver := newMockResolver()
		expectedParams := &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorWithinGeoRange,
			On: &filters.Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("location"),
			},
			Value: &filters.Value{
				Value: filters.GeoRange{
					GeoCoordinates: &models.GeoCoordinates{
						Latitude:  ptFloat32(0.5),
						Longitude: ptFloat32(0.6),
					},
					Distance: 2.0,
				},
				Type: schema.DataTypeGeoCoordinates,
			},
		}}

		resolver.On("ReportFilters", expectedParams).
			Return(test_helper.EmptyList(), nil).Once()

		query := `{ SomeAction(where: {
			path: ["location"],
			operator: WithinGeoRange,
			valueGeoRange: {geoCoordinates: { latitude: 0.5, longitude: 0.6 }, distance: { max: 2.0 } }
		}) }`
		resolver.AssertResolve(t, query)
	})

	t.Run("with only some of the fields set", func(t *testing.T) {
		resolver := newMockResolver()
		expectedParams := &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorWithinGeoRange,
			On: &filters.Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("location"),
			},
			Value: &filters.Value{
				Value: filters.GeoRange{
					GeoCoordinates: &models.GeoCoordinates{
						Latitude:  ptFloat32(0.5),
						Longitude: ptFloat32(0.6),
					},
					Distance: 2.0,
				},
				Type: schema.DataTypeGeoCoordinates,
			},
		}}

		resolver.On("ReportFilters", expectedParams).
			Return(test_helper.EmptyList(), nil).Once()

		query := `{ SomeAction(where: {
			path: ["location"],
			operator: WithinGeoRange,
			valueGeoRange: { geoCoordinates: { latitude: 0.5 }, distance: { max: 2.0} }
		}) }`

		expectedErrors := []gqlerrors.FormattedError{
			{
				Message:   "Argument \"where\" has invalid value {path: [\"location\"], operator: WithinGeoRange, valueGeoRange: {geoCoordinates: {latitude: 0.5}, distance: {max: 2.0}}}.\nIn field \"valueGeoRange\": In field \"geoCoordinates\": In field \"longitude\": Expected \"Float!\", found null.",
				Locations: []location.SourceLocation{{Line: 1, Column: 21}},
			},
		}
		resolver.AssertErrors(t, query, expectedErrors)
	})
}

func TestExtractFilterNestedField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On: &filters.Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("hasAction"),
			Child: &filters.Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
		},
		Value: &filters.Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: { path: ["hasAction", "SomeAction", "intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractOperand(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorAnd,
		Operands: []filters.Clause{
			{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("SomeAction"),
					Property: schema.AssertValidPropertyName("intField"),
				},
				Value: &filters.Value{
					Value: 42,
					Type:  schema.DataTypeInt,
				},
			},
			{
				Operator: filters.OperatorEqual,
				On: &filters.Path{
					Class:    schema.AssertValidClassName("SomeAction"),
					Property: schema.AssertValidPropertyName("hasAction"),
					Child: &filters.Path{
						Class:    schema.AssertValidClassName("SomeAction"),
						Property: schema.AssertValidPropertyName("intField"),
					},
				},
				Value: &filters.Value{
					Value: 4242,
					Type:  schema.DataTypeInt,
				},
			},
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: { operator: And, operands: [
      { operator: Equal, valueInt: 42,   path: ["intField"]},
      { operator: Equal, valueInt: 4242, path: ["hasAction", "SomeAction", "intField"] }
    ]}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractCompareOpFailsIfOperandPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	query := `{ SomeAction(where: { operator: Equal, operands: []}) }`
	resolver.AssertFailToResolve(t, query)
}

func TestExtractOperandFailsIfPathPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	query := `{ SomeAction(where: { path:["should", "not", "be", "present"], operator: And  })}`
	resolver.AssertFailToResolve(t, query)
}

func ptFloat32(in float32) *float32 {
	return &in
}
