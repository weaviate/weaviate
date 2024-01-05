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

package test

import (
	"testing"

	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func aggregateArrayClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate ArrayClass without group by", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateArrayClassTestCases{}

		expectedAllResultsAssertions := []assertFunc{
			asserts.meta(7),
			asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
			asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
			asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
			asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
			asserts.dateArray("datesAsStrings", 10),
			asserts.dateArray("dates", 10),
		}
		expectedResultsWithDataAssertions := []assertFunc{
			asserts.meta(2),
			asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
			asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
			asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
			asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
			asserts.dateArray("datesAsStrings", 7),
			asserts.dateArray("dates", 7),
		}
		expectedResultsWithoutDataAssertions := []assertFunc{
			asserts.meta(3),
			asserts.booleanArray0("booleans"),
			asserts.textArray0("texts"),
			asserts.numberArray0("numbers"),
			asserts.intArray0("ints"),
			asserts.dateArray0("datesAsStrings"),
			asserts.dateArray0("dates"),
		}
		expectedNoResultsAssertions := []assertFunc{
			asserts.meta(0),
			asserts.booleanArray0("booleans"),
			asserts.textArray0("texts"),
			asserts.numberArray0("numbers"),
			asserts.intArray0("ints"),
			asserts.dateArray0("datesAsStrings"),
			asserts.dateArray0("dates"),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(wrapWithMap(expectedAllResultsAssertions)),

			testCasesGen.WithWhereFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereFilter_ResultsWithData(wrapWithMap(expectedResultsWithDataAssertions)),
			testCasesGen.WithWhereFilter_ResultsWithoutData(wrapWithMap(expectedResultsWithoutDataAssertions)),
			testCasesGen.WithWhereFilter_NoResults(wrapWithMap(expectedNoResultsAssertions)),

			testCasesGen.WithNearObjectFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithNearObjectFilter_ResultsWithData(wrapWithMap(expectedResultsWithDataAssertions)),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(wrapWithMap(expectedResultsWithoutDataAssertions)),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(wrapWithMap(expectedResultsWithDataAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(wrapWithMap(expectedResultsWithoutDataAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(wrapWithMap(expectedNoResultsAssertions)),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassNoGroupByResult(result)

				for _, groupAssertions := range tc.groupedAssertions {
					for _, assertion := range groupAssertions {
						assertion(extracted)
					}
				}
			})
		}
	})
}

func aggregateDuplicatesClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate DuplicatesClass without group by", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateDuplicatesClassTestCases{}

		expectedAllResultsAssertions := []assertFunc{
			asserts.meta(3),
			asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
			asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
			asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
			asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
			asserts.dateArray("datesAsStrings", 9),
		}
		expectedSomeResultsAssertions := []assertFunc{
			asserts.meta(1),
			asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
			asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
			asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
			asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
			asserts.dateArray("datesAsStrings", 4),
		}
		expectedNoResultsAssertions := []assertFunc{
			asserts.meta(0),
			asserts.booleanArray0("booleans"),
			asserts.textArray0("texts"),
			asserts.numberArray0("numbers"),
			asserts.intArray0("ints"),
			asserts.dateArray0("datesAsStrings"),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(wrapWithMap(expectedAllResultsAssertions)),

			testCasesGen.WithWhereFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereFilter_SomeResults(wrapWithMap(expectedSomeResultsAssertions)),
			testCasesGen.WithWhereFilter_NoResults(wrapWithMap(expectedNoResultsAssertions)),
		}

		for _, tc := range testCases {
			query := aggregateDuplicatesClassQuery(tc.filters, "")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractDuplicatesClassNoGroupByResult(result)

				for _, groupAssertions := range tc.groupedAssertions {
					for _, assertion := range groupAssertions {
						assertion(extracted)
					}
				}
			})
		}
	})
}

func aggregateNoPropsClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate NoPropsClass without group by", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateNoPropsClassTestCases{}

		expectedAllResultsAssertions := []assertFunc{
			asserts.meta(2),
		}
		expectedSomeResultsAssertions := []assertFunc{
			asserts.meta(1),
		}
		expectedNoResultsAssertions := []assertFunc{
			asserts.meta(0),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(wrapWithMap(expectedAllResultsAssertions)),

			testCasesGen.WithWhereFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereFilter_SomeResults(wrapWithMap(expectedSomeResultsAssertions)),
			testCasesGen.WithWhereFilter_NoResults(wrapWithMap(expectedNoResultsAssertions)),

			testCasesGen.WithNearObjectFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_SomeResults(wrapWithMap(expectedSomeResultsAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(wrapWithMap(expectedNoResultsAssertions)),
		}

		for _, tc := range testCases {
			query := aggregateNoPropsQuery(tc.filters)

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractNoPropsClassNoGroupByResult(result)

				for _, groupAssertions := range tc.groupedAssertions {
					for _, assertion := range groupAssertions {
						assertion(extracted)
					}
				}
			})
		}
	})
}

func aggregateCityClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate City without group by", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := []assertFunc{
			asserts.meta(6),
			asserts.number("cityArea", 4, 891.96, 217.22, 217.22, 2320.48, 605.6500000000001, 580.12),
			asserts.date("cityRights", 4),
			asserts.text("history", 4, []string{historyAmsterdam, historyRotterdam, historyBerlin, historyDusseldorf}, []int64{1, 1, 1, 1}),
			asserts.boolean("isCapital", 5, 3, 2, 0.6, 0.4),
			asserts.textArray("museums", 9, []string{"German Historical Museum", "Museum Boijmans Van Beuningen", "Onomato", "Rijksmuseum", "Schiffahrt Museum"}, []int64{1, 1, 1, 1, 1}),
			asserts.text("name", 5, []string{"Amsterdam", "Berlin", "Dusseldorf", "Missing Island", "Rotterdam"}, []int64{1, 1, 1, 1, 1}),
			asserts.int("population", 5, 3470000, 0, 600000, 6470000, 600000, 1294000),
			asserts.textArray("timezones", 8, []string{"CEST", "CET"}, []int64{4, 4}),
			asserts.pointingTo("inCountry", "Country"),
		}
		expectedResultsWithDataAssertions := []assertFunc{
			asserts.meta(2),
			asserts.number("cityArea", 2, 891.96, 891.95, 891.95, 1783.91, 891.955, 891.955),
			asserts.date("cityRights", 2),
			asserts.text("history", 2, []string{historyAmsterdam, historyBerlin}, []int64{1, 1}),
			asserts.boolean("isCapital", 2, 0, 2, 0, 1),
			asserts.textArray("museums", 3, []string{"German Historical Museum", "Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1, 1}),
			asserts.text("name", 2, []string{"Amsterdam", "Berlin"}, []int64{1, 1}),
			asserts.int("population", 2, 3470000, 1800000, 1800000, 5270000, 2635000, 2635000),
			asserts.textArray("timezones", 4, []string{"CEST", "CET"}, []int64{2, 2}),
			asserts.pointingTo("inCountry", "Country"),
		}
		expectedResultsWithoutDataAssertions := []assertFunc{
			asserts.meta(1),
			asserts.number0("cityArea"),
			asserts.date0("cityRights"),
			asserts.text0("history"),
			asserts.boolean0("isCapital"),
			asserts.textArray0("museums"),
			asserts.text0("name"),
			asserts.int0("population"),
			asserts.textArray0("timezones"),
			asserts.pointingTo("inCountry", "Country"),
		}
		expectedNoResultsAssertions := []assertFunc{
			asserts.meta(0),
			asserts.number0("cityArea"),
			asserts.date0("cityRights"),
			asserts.text0("history"),
			asserts.boolean0("isCapital"),
			asserts.textArray0("museums"),
			asserts.text0("name"),
			asserts.int0("population"),
			asserts.textArray0("timezones"),
			asserts.pointingTo("inCountry", "Country"),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(wrapWithMap(expectedAllResultsAssertions)),

			testCasesGen.WithWhereFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereFilter_ResultsWithData(wrapWithMap(expectedResultsWithDataAssertions)),
			testCasesGen.WithWhereFilter_ResultsWithoutData(wrapWithMap(expectedResultsWithoutDataAssertions)),
			testCasesGen.WithWhereFilter_NoResults(wrapWithMap(expectedNoResultsAssertions)),

			testCasesGen.WithNearObjectFilter_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithNearObjectFilter_ResultsWithData(wrapWithMap(expectedResultsWithDataAssertions)),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(wrapWithMap(expectedResultsWithoutDataAssertions)),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(wrapWithMap(expectedAllResultsAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(wrapWithMap(expectedResultsWithDataAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(wrapWithMap(expectedResultsWithoutDataAssertions)),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(wrapWithMap(expectedNoResultsAssertions)),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityNoGroupByResult(result)

				for _, groupAssertions := range tc.groupedAssertions {
					for _, assertion := range groupAssertions {
						assertion(extracted)
					}
				}
			})
		}
	})
}

func extractArrayClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, arrayClassName)[0].(map[string]interface{})
}

func extractDuplicatesClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, duplicatesClassName)[0].(map[string]interface{})
}

func extractNoPropsClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, noPropsClassName)[0].(map[string]interface{})
}

func extractCityNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, cityClassName)[0].(map[string]interface{})
}

func wrapWithMap(assertFuncs []assertFunc) map[string][]assertFunc {
	return map[string][]assertFunc{"": assertFuncs}
}
