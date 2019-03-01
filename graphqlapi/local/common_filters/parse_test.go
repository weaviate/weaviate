/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */

package common_filters

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/location"
)

// Basic test on filter
func TestExtractFilterToplevelField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()
	/*localfilter is a struct containing a clause struct
		type Clause struct {
		Operator Operator
		On       *Path
		Value    *Value
		Operands []Clause
	}*/
	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorEqual,
		On: &Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("intField"),
		},
		Value: &Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: { path: ["intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractFilterGeoLocation(t *testing.T) {
	t.Parallel()

	t.Run("with all fields set as required", func(t *testing.T) {
		resolver := newMockResolver()
		expectedParams := &LocalFilter{Root: &Clause{
			Operator: OperatorWithinGeoRange,
			On: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("location"),
			},
			Value: &Value{
				Value: GeoRange{
					GeoCoordinates: &models.GeoCoordinates{
						Latitude:  0.5,
						Longitude: 0.6,
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
		expectedParams := &LocalFilter{Root: &Clause{
			Operator: OperatorWithinGeoRange,
			On: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("location"),
			},
			Value: &Value{
				Value: GeoRange{
					GeoCoordinates: &models.GeoCoordinates{
						Latitude:  0.5,
						Longitude: 0.6,
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
			gqlerrors.FormattedError{
				Message:   "Argument \"where\" has invalid value {path: [\"location\"], operator: WithinGeoRange, valueGeoRange: {geoCoordinates: {latitude: 0.5}, distance: {max: 2.0}}}.\nIn field \"valueGeoRange\": In field \"geoCoordinates\": In field \"longitude\": Expected \"Float!\", found null.",
				Locations: []location.SourceLocation{location.SourceLocation{Line: 1, Column: 21}},
			},
		}
		resolver.AssertErrors(t, query, expectedErrors)
	})
}

func TestExtractFilterNestedField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorEqual,
		On: &Path{
			Class:    schema.AssertValidClassName("SomeAction"),
			Property: schema.AssertValidPropertyName("hasAction"),
			Child: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
		},
		Value: &Value{
			Value: 42,
			Type:  schema.DataTypeInt,
		},
	}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: { path: ["HasAction", "SomeAction", "intField"], operator: Equal, valueInt: 42}) }`
	resolver.AssertResolve(t, query)
}

func TestExtractOperand(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalFilter{Root: &Clause{
		Operator: OperatorAnd,
		Operands: []Clause{Clause{
			Operator: OperatorEqual,
			On: &Path{
				Class:    schema.AssertValidClassName("SomeAction"),
				Property: schema.AssertValidPropertyName("intField"),
			},
			Value: &Value{
				Value: 42,
				Type:  schema.DataTypeInt,
			},
		},
			Clause{
				Operator: OperatorEqual,
				On: &Path{
					Class:    schema.AssertValidClassName("SomeAction"),
					Property: schema.AssertValidPropertyName("hasAction"),
					Child: &Path{
						Class:    schema.AssertValidClassName("SomeAction"),
						Property: schema.AssertValidPropertyName("intField"),
					},
				},
				Value: &Value{
					Value: 4242,
					Type:  schema.DataTypeInt,
				},
			},
		}}}

	resolver.On("ReportFilters", expectedParams).
		Return(test_helper.EmptyList(), nil).Once()

	query := `{ SomeAction(where: { operator: And, operands: [
      { operator: Equal, valueInt: 42,   path: ["intField"]},
      { operator: Equal, valueInt: 4242, path: ["HasAction", "SomeAction", "intField"] }
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
