//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common_filters

import (
	"testing"

	"github.com/tailor-inc/graphql/gqlerrors"
	"github.com/tailor-inc/graphql/language/location"
	test_helper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// Basic test on filter
func TestExtractFilterToplevelField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(t, mockParams{reportFilter: true})
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

	t.Run("extracts with valueText", func(t *testing.T) {
		resolver := newMockResolver(t, mockParams{reportFilter: true})
		expectedParams := &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorLike,
			On: &filters.Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("name"),
			},
			Value: &filters.Value{
				Value: "Schn*el",
				Type:  schema.DataTypeText,
			},
		}}

		resolver.On("ReportFilters", expectedParams).
			Return(test_helper.EmptyList(), nil).Once()

		query := `{ SomeAction(where: {
				path: ["name"],
				operator: Like,
				valueText: "Schn*el",
			}) }`
		resolver.AssertResolve(t, query)
	})

	t.Run("[deprecated string] extracts with valueString", func(t *testing.T) {
		resolver := newMockResolver(t, mockParams{reportFilter: true})
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
	})
}

func TestExtractFilterLike_ValueText(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(t, mockParams{reportFilter: true})
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

func TestExtractFilterIsNull(t *testing.T) {
	resolver := newMockResolver(t, mockParams{reportFilter: true})
	expectedParams := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorIsNull,
		On: &filters.Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("name"),
		},
		Value: &filters.Value{
			Value: "true",
			Type:  schema.DataTypeText,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: {
			path: ["name"],
			operator: IsNull,
			valueText: "true",
		}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterGeoLocation(t *testing.T) {
	t.Parallel()

	t.Run("with all fields set as required", func(t *testing.T) {
		resolver := newMockResolver(t, mockParams{reportFilter: true})
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
		resolver := newMockResolver(t, mockParams{reportFilter: true})
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

	resolver := newMockResolver(t, mockParams{reportFilter: true})

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

	resolver := newMockResolver(t, mockParams{reportFilter: true})

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

	resolver := newMockResolver(t, mockParams{reportFilter: true})

	query := `{ SomeAction(where: { operator: Equal, operands: []}) }`
	resolver.AssertFailToResolve(t, query)
}

func TestExtractOperandFailsIfPathPresent(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver(t, mockParams{reportFilter: true})

	query := `{ SomeAction(where: { path:["should", "not", "be", "present"], operator: And  })}`
	resolver.AssertFailToResolve(t, query)
}

func TestExtractNearVector(t *testing.T) {
	t.Parallel()

	t.Run("with certainty provided", func(t *testing.T) {
		t.Parallel()

		query := `{ SomeAction(nearVector: {vector: [1, 2, 3], certainty: 0.7})}`
		expectedparams := searchparams.NearVector{
			Vector:    []float32{1, 2, 3},
			Certainty: 0.7,
		}

		resolver := newMockResolver(t, mockParams{reportNearVector: true})

		resolver.On("ReportNearVector", expectedparams).
			Return(test_helper.EmptyList(), nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with distance provided", func(t *testing.T) {
		t.Parallel()

		query := `{ SomeAction(nearVector: {vector: [1, 2, 3], distance: 0.4})}`
		expectedparams := searchparams.NearVector{
			Vector:       []float32{1, 2, 3},
			Distance:     0.4,
			WithDistance: true,
		}

		resolver := newMockResolver(t, mockParams{reportNearVector: true})

		resolver.On("ReportNearVector", expectedparams).
			Return(test_helper.EmptyList(), nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with distance and certainty provided", func(t *testing.T) {
		t.Parallel()

		query := `{ SomeAction(nearVector: {vector: [1, 2, 3], distance: 0.4, certainty: 0.7})}`
		resolver := newMockResolver(t, mockParams{reportNearVector: true})
		resolver.AssertFailToResolve(t, query)
	})
}

func TestExtractNearObject(t *testing.T) {
	t.Parallel()

	t.Run("with certainty provided", func(t *testing.T) {
		t.Parallel()

		query := `{ SomeAction(nearObject: {id: "123", certainty: 0.7})}`
		expectedparams := searchparams.NearObject{
			ID:        "123",
			Certainty: 0.7,
		}

		resolver := newMockResolver(t, mockParams{reportNearObject: true})

		resolver.On("ReportNearObject", expectedparams).
			Return(test_helper.EmptyList(), nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with distance provided", func(t *testing.T) {
		t.Parallel()

		query := `{ SomeAction(nearObject: {id: "123", distance: 0.4})}`
		expectedparams := searchparams.NearObject{
			ID:           "123",
			Distance:     0.4,
			WithDistance: true,
		}

		resolver := newMockResolver(t, mockParams{reportNearObject: true})

		resolver.On("ReportNearObject", expectedparams).
			Return(test_helper.EmptyList(), nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("with distance and certainty provided", func(t *testing.T) {
		t.Parallel()

		query := `{ SomeAction(nearObject: {id: "123", distance: 0.4, certainty: 0.7})}`
		resolver := newMockResolver(t, mockParams{reportNearObject: true})
		resolver.AssertFailToResolve(t, query)
	})
}

func ptFloat32(in float32) *float32 {
	return &in
}
