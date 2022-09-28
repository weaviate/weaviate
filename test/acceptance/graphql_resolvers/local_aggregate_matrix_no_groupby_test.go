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
		h := &gqlAggregateHelper{}
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
		h := &gqlAggregateHelper{}
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

func extractArrayClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, arrayClassName)[0].(map[string]interface{})
}

func extractNoPropsClassNoGroupByResult(result *graphqlhelper.GraphQLResult) map[string]interface{} {
	return extractAggregateResult(result, noPropsClassName)[0].(map[string]interface{})
}
