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
 */package fetch

import (
	"testing"
	"time"

	cf "github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
)

func Test_QueryBuilder_StringAllOperators(t *testing.T) {
	tests := testCases{
		{
			name:        "string Equal",
			inputParams: paramsFromSingleProp("name", schema.DataTypeString, cf.OperatorEqual, "Amsterdam"),
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
			inputParams: paramsFromSingleProp("name", schema.DataTypeString, cf.OperatorNotEqual, "Amsterdam"),
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
			inputParams: paramsFromSingleProp("isCapital", schema.DataTypeBoolean, cf.OperatorEqual, true),
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
			inputParams: paramsFromSingleProp("isCapital", schema.DataTypeBoolean, cf.OperatorNotEqual, true),
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
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, cf.OperatorEqual, 2000),
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
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, cf.OperatorNotEqual, 2000),
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
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, cf.OperatorLessThan, 2000),
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
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, cf.OperatorGreaterThan, 2000),
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
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, cf.OperatorLessThanEqual, 2000),
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
			inputParams: paramsFromSingleProp("population", schema.DataTypeInt, cf.OperatorGreaterThanEqual, 2000),
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
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, cf.OperatorEqual, float64(2000)),
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
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, cf.OperatorNotEqual, float64(2000)),
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
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, cf.OperatorLessThan, float64(2000)),
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
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, cf.OperatorGreaterThan, float64(2000)),
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
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, cf.OperatorLessThanEqual, float64(2000)),
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
			inputParams: paramsFromSingleProp("area", schema.DataTypeNumber, cf.OperatorGreaterThanEqual, float64(2000)),
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
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, cf.OperatorEqual, sampleDate()),
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
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, cf.OperatorNotEqual, sampleDate()),
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
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, cf.OperatorLessThan, sampleDate()),
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
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, cf.OperatorGreaterThan, sampleDate()),
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
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, cf.OperatorLessThanEqual, sampleDate()),
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
			inputParams: paramsFromSingleProp("dateOfFirstAppearance", schema.DataTypeDate, cf.OperatorGreaterThanEqual, sampleDate()),
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

func cityClassSearch() contextionary.SearchResults {
	return contextionary.SearchResults{
		Results: []contextionary.SearchResult{
			{
				Name:      "City",
				Certainty: 1.0,
			},
		},
	}
}

func paramsFromSingleProp(propName string, dataType schema.DataType,
	operator cf.Operator, value interface{}) fetch.Params {
	return fetch.Params{
		Kind:               kind.THING_KIND,
		PossibleClassNames: cityClassSearch(),
		Properties:         singleProp(propName, dataType, operator, value),
	}
}
