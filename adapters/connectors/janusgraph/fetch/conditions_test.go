/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package fetch

import (
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func Test_QueryBuilder_StringAllOperators(t *testing.T) {
	tests := testCases{
		{
			name:        "string Equal",
			inputParams: paramsFromSingleProp("name", schema.DataTypeString, filters.OperatorEqual, "Amsterdam"),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_1", eq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "string NotEqual",
			inputParams: paramsFromSingleProp("name", schema.DataTypeString, filters.OperatorNotEqual, "Amsterdam"),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_1", neq("Amsterdam"))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}

func Test_QueryBuilder_BooleanAllOperators(t *testing.T) {
	tests := testCases{
		{
			name:        "Boolean Equal",
			inputParams: paramsFromSingleProp("isCapital", schema.DataTypeBoolean, filters.OperatorEqual, true),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_4", eq(true))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "Boolean NotEqual",
			inputParams: paramsFromSingleProp("isCapital", schema.DataTypeBoolean, filters.OperatorNotEqual, true),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_4", neq(true))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}

func Test_QueryBuilder_IntAllOperators(t *testing.T) {
	tests := testCases{
		{
			name:        "with a single class name, single property name, int type, operator Equal",
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, filters.OperatorEqual, 2000),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", eq(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, int type, operator NotEqual",
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, filters.OperatorNotEqual, 2000),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", neq(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, int type, operator LessThan",
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, filters.OperatorLessThan, 2000),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", lt(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, int type, operator GreaterThan",
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, filters.OperatorGreaterThan, 2000),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", gt(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, int type, operator LessThanEqual",
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, filters.OperatorLessThanEqual, 2000),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", lte(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, int type, operator GreaterThanEqual",
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, filters.OperatorGreaterThanEqual, 2000),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_2", gte(2000))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}

func Test_QueryBuilder_NumberAllOperators(t *testing.T) {
	tests := testCases{
		{
			name:        "with a single class name, single property name, Number type, operator Equal",
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, filters.OperatorEqual, float64(2000)),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_6", eq(2000.000000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Number type, operator NotEqual",
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, filters.OperatorNotEqual, float64(2000)),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_6", neq(2000.000000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Number type, operator LessThan",
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, filters.OperatorLessThan, float64(2000)),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_6", lt(2000.000000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Number type, operator GreaterThan",
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, filters.OperatorGreaterThan, float64(2000)),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_6", gt(2000.000000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Number type, operator LessThanEqual",
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, filters.OperatorLessThanEqual, float64(2000)),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_6", lte(2000.000000))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Number type, operator GreaterThanEqual",
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, filters.OperatorGreaterThanEqual, float64(2000)),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_6", gte(2000.000000))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)

}

func Test_QueryBuilder_DateAllOperators(t *testing.T) {
	tests := testCases{
		{
			name:        "with a single class name, single property name, Date type, operator Equal",
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, filters.OperatorEqual, sampleDate()),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_7", eq("2017-08-17T12:47:00+02:00"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Date type, operator NotEqual",
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, filters.OperatorNotEqual, sampleDate()),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_7", neq("2017-08-17T12:47:00+02:00"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Date type, operator LessThan",
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, filters.OperatorLessThan, sampleDate()),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_7", lt("2017-08-17T12:47:00+02:00"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Date type, operator GreaterThan",
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, filters.OperatorGreaterThan, sampleDate()),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_7", gt("2017-08-17T12:47:00+02:00"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Date type, operator LessThanEqual",
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, filters.OperatorLessThanEqual, sampleDate()),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_7", lte("2017-08-17T12:47:00+02:00"))
					)
				).valueMap("uuid", "classId")
			`,
		},
		{
			name:        "with a single class name, single property name, Date type, operator GreaterThanEqual",
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, filters.OperatorGreaterThanEqual, sampleDate()),
			expectedQuery: `
				g.V().has("kind", "thing").and(
					or(
						has("classId", "class_18").has("prop_7", gte("2017-08-17T12:47:00+02:00"))
					)
				).valueMap("uuid", "classId")
			`,
		},
	}

	tests.AssertQuery(t)
}

func sampleDate() time.Time {
	dateString := "2017-08-17T12:47:00+02:00"
	dateTime, _ := time.Parse(time.RFC3339, dateString)
	return dateTime
}

func cityClassSearch() traverser.SearchResults {
	return traverser.SearchResults{
		Results: []traverser.SearchResult{
			{
				Name:      "City",
				Certainty: 1.0,
			},
		},
	}
}

func paramsFromSingleProp(propName string, dataType schema.DataType,
	operator filters.Operator, value interface{}) traverser.FetchParams {
	return traverser.FetchParams{
		Kind:               kind.Thing,
		PossibleClassNames: cityClassSearch(),
		Properties:         singleProp(propName, dataType, operator, value),
	}
}
