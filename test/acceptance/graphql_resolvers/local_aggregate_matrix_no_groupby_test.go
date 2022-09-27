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

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/assert"
)

func aggregateArrayClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate ArrayClass without group by", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateArrayClassTestCases{}

		allResults := map[string]interface{}{
			"meta":           h.meta(7),
			"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
			"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
			"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
			"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
			"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
			"datesAsStrings": h.dates(10),
			"dates":          h.dates(10),
		}
		resultsWithData := map[string]interface{}{
			"meta":           h.meta(2),
			"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
			"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
			"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
			"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
			"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
			"datesAsStrings": h.dates(7),
			"dates":          h.dates(7),
		}
		resultsWithoutData := map[string]interface{}{
			"meta":           h.meta(3),
			"booleans":       h.booleans0(),
			"strings":        h.strings0(),
			"texts":          h.texts0(),
			"numbers":        h.numbers0(),
			"ints":           h.ints0(),
			"datesAsStrings": h.dates0(),
			"dates":          h.dates0(),
		}
		noResults := map[string]interface{}{
			"meta":           h.meta(0),
			"booleans":       h.booleans0(),
			"strings":        h.strings0(),
			"texts":          h.texts0(),
			"numbers":        h.numbers0(),
			"ints":           h.ints0(),
			"datesAsStrings": h.dates0(),
			"dates":          h.dates0(),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(allResults),

			testCasesGen.WithWhereFilter_AllResults(allResults),
			testCasesGen.WithWhereFilter_ResultsWithData(resultsWithData),
			testCasesGen.WithWhereFilter_ResultsWithoutData(resultsWithoutData),
			testCasesGen.WithWhereFilter_NoResults(noResults),

			testCasesGen.WithNearObjectFilter_AllResults(allResults),
			testCasesGen.WithNearObjectFilter_ResultsWithData(resultsWithData),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(resultsWithoutData),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(allResults),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(resultsWithData),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(resultsWithoutData),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(noResults),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassNoGroupByResult(result)
				expected := tc.expected.(map[string]interface{})

				assert.Equal(t, expected, extracted)
			})
		}
	})
}

func aggregateNoPropsClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate NoPropsClass without group by", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateNoPropsClassTestCases{}

		allResults := map[string]interface{}{
			"meta": h.meta(2),
		}
		someResults := map[string]interface{}{
			"meta": h.meta(1),
		}
		noResults := map[string]interface{}{
			"meta": h.meta(0),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(allResults),

			testCasesGen.WithWhereFilter_AllResults(allResults),
			testCasesGen.WithWhereFilter_SomeResults(someResults),
			testCasesGen.WithWhereFilter_NoResults(noResults),

			testCasesGen.WithNearObjectFilter_AllResults(allResults),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(allResults),
			testCasesGen.WithWhereAndNearObjectFilters_SomeResults(someResults),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(noResults),
		}

		for _, tc := range testCases {
			query := aggregateNoPropsQuery(tc.filters)

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractNoPropsClassNoGroupByResult(result)
				expected := tc.expected.(map[string]interface{})

				assert.Equal(t, expected, extracted)
			})
		}
	})
}

func aggregateCityClassWithoutGroupByTest(t *testing.T) {
	t.Run("aggregate City without group by", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := map[string]interface{}{
			"meta":       h.meta(6),
			"cityArea":   h.number(4, 891.96, 217.22, 217.22, 2320.48, 605.6500000000001, 580.12),
			"cityRights": h.date(4),
			"history":    h.text(4, []string{historyAmsterdam, historyRotterdam, historyBerlin, historyDusseldorf}, []int64{1, 1, 1, 1}),
			"isCapital":  h.boolean(5, 3, 2, 0.6, 0.4),
			"museums":    h.texts(9, []string{"German Historical Museum", "Museum Boijmans Van Beuningen", "Onomato", "Rijksmuseum", "Schiffahrt Museum"}, []int64{1, 1, 1, 1, 1}),
			"name":       h.string(5, []string{"Amsterdam", "Berlin", "Dusseldorf", "Missing Island", "Rotterdam"}, []int64{1, 1, 1, 1, 1}),
			"population": h.int(5, 3470000, 0, 600000, 6470000, 600000, 1294000),
			"timezones":  h.strings(8, []string{"CEST", "CET"}, []int64{4, 4}),
			"inCountry":  h.pointingTo("Country"),
		}
		resultsWithData := map[string]interface{}{
			"meta":       h.meta(2),
			"cityArea":   h.number(2, 891.96, 891.95, 891.95, 1783.91, 891.955, 891.955),
			"cityRights": h.date(2),
			"history":    h.text(2, []string{historyAmsterdam, historyBerlin}, []int64{1, 1}),
			"isCapital":  h.boolean(2, 0, 2, 0, 1),
			"museums":    h.texts(3, []string{"German Historical Museum", "Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1, 1}),
			"name":       h.string(2, []string{"Amsterdam", "Berlin"}, []int64{1, 1}),
			"population": h.int(2, 3470000, 1800000, 1800000, 5270000, 2635000, 2635000),
			"timezones":  h.strings(4, []string{"CEST", "CET"}, []int64{2, 2}),
			"inCountry":  h.pointingTo("Country"),
		}
		resultsWithoutData := map[string]interface{}{
			"meta":       h.meta(1),
			"cityArea":   h.number0(),
			"cityRights": h.date0(),
			"history":    h.text0(),
			"isCapital":  h.boolean0(),
			"museums":    h.texts0(),
			"name":       h.string0(),
			"population": h.int0(),
			"timezones":  h.strings0(),
			"inCountry":  h.pointingTo("Country"),
		}
		noResults := map[string]interface{}{
			"meta":       h.meta(0),
			"cityArea":   h.number0(),
			"cityRights": h.date0(),
			"history":    h.text0(),
			"isCapital":  h.boolean0(),
			"museums":    h.texts0(),
			"name":       h.string0(),
			"population": h.int0(),
			"timezones":  h.strings0(),
			"inCountry":  h.pointingTo("Country"),
		}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(allResults),

			testCasesGen.WithWhereFilter_AllResults(allResults),
			testCasesGen.WithWhereFilter_ResultsWithData(resultsWithData),
			testCasesGen.WithWhereFilter_ResultsWithoutData(resultsWithoutData),
			testCasesGen.WithWhereFilter_NoResults(noResults),

			testCasesGen.WithNearObjectFilter_AllResults(allResults),
			testCasesGen.WithNearObjectFilter_ResultsWithData(resultsWithData),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(resultsWithoutData),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(allResults),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(resultsWithData),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(resultsWithoutData),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(noResults),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityNoGroupByResult(result)
				expected := tc.expected.(map[string]interface{})

				assert.Equal(t, expected, extracted)
			})
		}
	})
}

func extractArrayClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, arrayClassName)[0].(map[string]interface{})
}

func extractNoPropsClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, noPropsClassName)[0].(map[string]interface{})
}

func extractCityNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, cityClassName)[0].(map[string]interface{})
}
