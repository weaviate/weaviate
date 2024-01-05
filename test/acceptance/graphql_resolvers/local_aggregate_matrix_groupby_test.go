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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func aggregateArrayClassWithGroupByTest(t *testing.T) {
	asserts := newAggregateResponseAssert(t)
	testCasesGen := &aggregateArrayClassTestCases{}

	t.Run("aggregate ArrayClass with group by texts", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"Atxt": {
				asserts.groupedBy("Atxt", "texts"),
				asserts.meta(4),
				asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
				asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
				asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
				asserts.dateArray("datesAsStrings", 10),
				asserts.dateArray("dates", 10),
			},
			"Btxt": {
				asserts.groupedBy("Btxt", "texts"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				asserts.numberArray("numbers", 9, 4, 1, 1, 19, 2, 2.111111111111111),
				asserts.intArray("ints", 9, 104, 101, 101, 919, 102, 102.11111111111111),
				asserts.dateArray("datesAsStrings", 9),
				asserts.dateArray("dates", 9),
			},
			"Ctxt": {
				asserts.groupedBy("Ctxt", "texts"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"Dtxt": {
				asserts.groupedBy("Dtxt", "texts"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"Atxt": {
				asserts.groupedBy("Atxt", "texts"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"Btxt": {
				asserts.groupedBy("Btxt", "texts"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"Ctxt": {
				asserts.groupedBy("Ctxt", "texts"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"Dtxt": {
				asserts.groupedBy("Dtxt", "texts"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"texts\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate ArrayClass with group by ints", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"101": {
				asserts.groupedBy("101", "ints"),
				asserts.meta(4),
				asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
				asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
				asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
				asserts.dateArray("datesAsStrings", 10),
				asserts.dateArray("dates", 10),
			},
			"102": {
				asserts.groupedBy("102", "ints"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				asserts.numberArray("numbers", 9, 4, 1, 1, 19, 2, 2.111111111111111),
				asserts.intArray("ints", 9, 104, 101, 101, 919, 102, 102.11111111111111),
				asserts.dateArray("datesAsStrings", 9),
				asserts.dateArray("dates", 9),
			},
			"103": {
				asserts.groupedBy("103", "ints"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"104": {
				asserts.groupedBy("104", "ints"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"101": {
				asserts.groupedBy("101", "ints"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"102": {
				asserts.groupedBy("102", "ints"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"103": {
				asserts.groupedBy("103", "ints"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"104": {
				asserts.groupedBy("104", "ints"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"ints\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate ArrayClass with group by numbers", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"1": {
				asserts.groupedBy("1", "numbers"),
				asserts.meta(4),
				asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
				asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
				asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
				asserts.dateArray("datesAsStrings", 10),
				asserts.dateArray("dates", 10),
			},
			"2": {
				asserts.groupedBy("2", "numbers"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				asserts.numberArray("numbers", 9, 4, 1, 1, 19, 2, 2.111111111111111),
				asserts.intArray("ints", 9, 104, 101, 101, 919, 102, 102.11111111111111),
				asserts.dateArray("datesAsStrings", 9),
				asserts.dateArray("dates", 9),
			},
			"3": {
				asserts.groupedBy("3", "numbers"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"4": {
				asserts.groupedBy("4", "numbers"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"1": {
				asserts.groupedBy("1", "numbers"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2": {
				asserts.groupedBy("2", "numbers"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"3": {
				asserts.groupedBy("3", "numbers"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"4": {
				asserts.groupedBy("4", "numbers"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"numbers\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate ArrayClass with group by dates", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"2001-06-01T12:00:00Z": {
				asserts.groupedBy("2001-06-01T12:00:00Z", "dates"),
				asserts.meta(4),
				asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
				asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
				asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
				asserts.dateArray("datesAsStrings", 10),
				asserts.dateArray("dates", 10),
			},
			"2002-06-02T12:00:00Z": {
				asserts.groupedBy("2002-06-02T12:00:00Z", "dates"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				asserts.numberArray("numbers", 9, 4, 1, 1, 19, 2, 2.111111111111111),
				asserts.intArray("ints", 9, 104, 101, 101, 919, 102, 102.11111111111111),
				asserts.dateArray("datesAsStrings", 9),
				asserts.dateArray("dates", 9),
			},
			"2003-06-03T12:00:00Z": {
				asserts.groupedBy("2003-06-03T12:00:00Z", "dates"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2004-06-04T12:00:00Z": {
				asserts.groupedBy("2004-06-04T12:00:00Z", "dates"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"2001-06-01T12:00:00Z": {
				asserts.groupedBy("2001-06-01T12:00:00Z", "dates"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2002-06-02T12:00:00Z": {
				asserts.groupedBy("2002-06-02T12:00:00Z", "dates"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2003-06-03T12:00:00Z": {
				asserts.groupedBy("2003-06-03T12:00:00Z", "dates"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2004-06-04T12:00:00Z": {
				asserts.groupedBy("2004-06-04T12:00:00Z", "dates"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"dates\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate ArrayClass with group by dates as strings", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"2021-06-01T22:18:59.640162Z": {
				asserts.groupedBy("2021-06-01T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(4),
				asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
				asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
				asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
				asserts.dateArray("datesAsStrings", 10),
				asserts.dateArray("dates", 10),
			},
			"2022-06-02T22:18:59.640162Z": {
				asserts.groupedBy("2022-06-02T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				asserts.numberArray("numbers", 9, 4, 1, 1, 19, 2, 2.111111111111111),
				asserts.intArray("ints", 9, 104, 101, 101, 919, 102, 102.11111111111111),
				asserts.dateArray("datesAsStrings", 9),
				asserts.dateArray("dates", 9),
			},
			"2023-06-03T22:18:59.640162Z": {
				asserts.groupedBy("2023-06-03T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2024-06-04T22:18:59.640162Z": {
				asserts.groupedBy("2024-06-04T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"2021-06-01T22:18:59.640162Z": {
				asserts.groupedBy("2021-06-01T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2022-06-02T22:18:59.640162Z": {
				asserts.groupedBy("2022-06-02T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2023-06-03T22:18:59.640162Z": {
				asserts.groupedBy("2023-06-03T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"2024-06-04T22:18:59.640162Z": {
				asserts.groupedBy("2024-06-04T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				asserts.numberArray("numbers", 4, 4, 1, 1, 10, 2.5, 2.5),
				asserts.intArray("ints", 4, 104, 101, 101, 410, 102.5, 102.5),
				asserts.dateArray("datesAsStrings", 4),
				asserts.dateArray("dates", 4),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"datesAsStrings\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate ArrayClass with group by booleans", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"true": {
				asserts.groupedBy("true", "booleans"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				asserts.numberArray("numbers", 9, 4, 1, 1, 19, 2, 2.111111111111111),
				asserts.intArray("ints", 9, 104, 101, 101, 919, 102, 102.11111111111111),
				asserts.dateArray("datesAsStrings", 9),
				asserts.dateArray("dates", 9),
			},
			"false": {
				asserts.groupedBy("false", "booleans"),
				asserts.meta(4),
				asserts.booleanArray("booleans", 10, 4, 6, 0.4, 0.6),
				asserts.textArray("texts", 10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				asserts.numberArray("numbers", 10, 4, 1, 1, 20, 2, 2),
				asserts.intArray("ints", 10, 104, 101, 101, 1020, 102, 102),
				asserts.dateArray("datesAsStrings", 10),
				asserts.dateArray("dates", 10),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"true": {
				asserts.groupedBy("true", "booleans"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
			"false": {
				asserts.groupedBy("false", "booleans"),
				asserts.meta(2),
				asserts.booleanArray("booleans", 7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				asserts.textArray("texts", 7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				asserts.numberArray("numbers", 7, 4, 1, 1, 16, 2, 2.2857142857142856),
				asserts.intArray("ints", 7, 104, 101, 101, 716, 102, 102.28571428571429),
				asserts.dateArray("datesAsStrings", 7),
				asserts.dateArray("dates", 7),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"booleans\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})
}

func aggregateDuplicatesClassWithGroupByTest(t *testing.T) {
	asserts := newAggregateResponseAssert(t)
	testCasesGen := &aggregateDuplicatesClassTestCases{}

	t.Run("aggregate DuplicatesClass with group by texts", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"Atxt": {
				asserts.groupedBy("Atxt", "texts"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
			"Btxt": {
				asserts.groupedBy("Btxt", "texts"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
		}
		expectedSomeResultsAssertions := map[string][]assertFunc{
			"Atxt": {
				asserts.groupedBy("Atxt", "texts"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
			"Btxt": {
				asserts.groupedBy("Btxt", "texts"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
		}
		expectedNoResultsAssertsions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_SomeResults(expectedSomeResultsAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertsions),
		}

		for _, tc := range testCases {
			query := aggregateDuplicatesClassQuery(tc.filters, "groupBy: [\"texts\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractDuplicatesClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate DuplicatesClass with group by ints", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"101": {
				asserts.groupedBy("101", "ints"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
			"102": {
				asserts.groupedBy("102", "ints"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
		}
		expectedSomeResultsAssertions := map[string][]assertFunc{
			"101": {
				asserts.groupedBy("101", "ints"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
			"102": {
				asserts.groupedBy("102", "ints"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
		}
		expectedNoResultsAssertsions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_SomeResults(expectedSomeResultsAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertsions),
		}

		for _, tc := range testCases {
			query := aggregateDuplicatesClassQuery(tc.filters, "groupBy: [\"ints\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractDuplicatesClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate DuplicatesClass with group by numbers", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"1": {
				asserts.groupedBy("1", "numbers"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
			"2": {
				asserts.groupedBy("2", "numbers"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
		}
		expectedSomeResultsAssertions := map[string][]assertFunc{
			"1": {
				asserts.groupedBy("1", "numbers"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
			"2": {
				asserts.groupedBy("2", "numbers"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
		}
		expectedNoResultsAssertsions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_SomeResults(expectedSomeResultsAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertsions),
		}

		for _, tc := range testCases {
			query := aggregateDuplicatesClassQuery(tc.filters, "groupBy: [\"numbers\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractDuplicatesClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate DuplicatesClass with group by dates as string", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"2021-06-01T22:18:59.640162Z": {
				asserts.groupedBy("2021-06-01T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
			"2022-06-02T22:18:59.640162Z": {
				asserts.groupedBy("2022-06-02T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
		}
		expectedSomeResultsAssertions := map[string][]assertFunc{
			"2021-06-01T22:18:59.640162Z": {
				asserts.groupedBy("2021-06-01T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
			"2022-06-02T22:18:59.640162Z": {
				asserts.groupedBy("2022-06-02T22:18:59.640162Z", "datesAsStrings"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
		}
		expectedNoResultsAssertsions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_SomeResults(expectedSomeResultsAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertsions),
		}

		for _, tc := range testCases {
			query := aggregateDuplicatesClassQuery(tc.filters, "groupBy: [\"datesAsStrings\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractDuplicatesClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate DuplicatesClass with group by booleans", func(t *testing.T) {
		expectedAllResultsAssertions := map[string][]assertFunc{
			"true": {
				asserts.groupedBy("true", "booleans"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
			"false": {
				asserts.groupedBy("false", "booleans"),
				asserts.meta(3),
				asserts.booleanArray("booleans", 9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				asserts.textArray("texts", 9, []string{"Atxt", "Btxt"}, []int64{6, 3}),
				asserts.numberArray("numbers", 9, 2, 1, 1, 12, 1, 1.3333333333333333),
				asserts.intArray("ints", 9, 102, 101, 101, 912, 101, 101.33333333333333),
				asserts.dateArray("datesAsStrings", 9),
			},
		}
		expectedSomeResultsAssertions := map[string][]assertFunc{
			"true": {
				asserts.groupedBy("true", "booleans"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
			"false": {
				asserts.groupedBy("false", "booleans"),
				asserts.meta(1),
				asserts.booleanArray("booleans", 4, 1, 3, 0.25, 0.75),
				asserts.textArray("texts", 4, []string{"Atxt", "Btxt"}, []int64{3, 1}),
				asserts.numberArray("numbers", 4, 2, 1, 1, 5, 1, 1.25),
				asserts.intArray("ints", 4, 102, 101, 101, 405, 101, 101.25),
				asserts.dateArray("datesAsStrings", 4),
			},
		}
		expectedNoResultsAssertsions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_SomeResults(expectedSomeResultsAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertsions),
		}

		for _, tc := range testCases {
			query := aggregateDuplicatesClassQuery(tc.filters, "groupBy: [\"booleans\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractDuplicatesClassGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})
}

func aggregateCityClassWithGroupByTest(t *testing.T) {
	t.Run("aggregate City with group by city area", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"891.96": {
				asserts.groupedBy("891.96", "cityArea"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"891.95": {
				asserts.groupedBy("891.95", "cityArea"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"217.22": {
				asserts.groupedBy("217.22", "cityArea"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"319.35": {
				asserts.groupedBy("319.35", "cityArea"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"891.96": {
				asserts.groupedBy("891.96", "cityArea"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"891.95": {
				asserts.groupedBy("891.95", "cityArea"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"cityArea\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by history", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			historyBerlin: {
				asserts.groupedBy(historyBerlin, "history"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			historyAmsterdam: {
				asserts.groupedBy(historyAmsterdam, "history"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			historyDusseldorf: {
				asserts.groupedBy(historyDusseldorf, "history"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			historyRotterdam: {
				asserts.groupedBy(historyRotterdam, "history"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			historyBerlin: {
				asserts.groupedBy(historyBerlin, "history"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			historyAmsterdam: {
				asserts.groupedBy(historyAmsterdam, "history"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"history\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by name", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"Berlin": {
				asserts.groupedBy("Berlin", "name"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Amsterdam": {
				asserts.groupedBy("Amsterdam", "name"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Dusseldorf": {
				asserts.groupedBy("Dusseldorf", "name"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Rotterdam": {
				asserts.groupedBy("Rotterdam", "name"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Missing Island": {
				asserts.groupedBy("Missing Island", "name"),
				asserts.meta(1),
				asserts.number0("cityArea"),
				asserts.date0("cityRights"),
				asserts.text0("history"),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray0("museums"),
				asserts.text("name", 1, []string{"Missing Island"}, []int64{1}),
				asserts.int("population", 1, 0, 0, 0, 0, 0, 0),
				asserts.textArray0("timezones"),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"Berlin": {
				asserts.groupedBy("Berlin", "name"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Amsterdam": {
				asserts.groupedBy("Amsterdam", "name"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"name\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by is capital", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"true": {
				asserts.groupedBy("true", "isCapital"),
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
			},
			"false": {
				asserts.groupedBy("false", "isCapital"),
				asserts.meta(3),
				asserts.number("cityArea", 2, 319.35, 217.22, 217.22, 536.57, 268.285, 268.285),
				asserts.date("cityRights", 2),
				asserts.text("history", 2, []string{historyRotterdam, historyDusseldorf}, []int64{1, 1}),
				asserts.boolean("isCapital", 3, 3, 0, 1, 0),
				asserts.textArray("museums", 6, []string{"Museum Boijmans Van Beuningen", "Onomato", "Schiffahrt Museum", "Schlossturm", "Wereldmuseum"}, []int64{1, 1, 1, 1, 1}),
				asserts.text("name", 3, []string{"Dusseldorf", "Missing Island", "Rotterdam"}, []int64{1, 1, 1}),
				asserts.int("population", 3, 600000, 0, 600000, 1200000, 600000, 400000),
				asserts.textArray("timezones", 4, []string{"CEST", "CET"}, []int64{2, 2}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"true": {
				asserts.groupedBy("true", "isCapital"),
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
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"isCapital\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by name", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"1400-01-01T00:00:00+02:00": {
				asserts.groupedBy("1400-01-01T00:00:00+02:00", "cityRights"),
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
			},
			"1135-01-01T00:00:00+02:00": {
				asserts.groupedBy("1135-01-01T00:00:00+02:00", "cityRights"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"1283-01-01T00:00:00+02:00": {
				asserts.groupedBy("1283-01-01T00:00:00+02:00", "cityRights"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"1400-01-01T00:00:00+02:00": {
				asserts.groupedBy("1400-01-01T00:00:00+02:00", "cityRights"),
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
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"cityRights\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by population", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"600000": {
				asserts.groupedBy("600000", "population"),
				asserts.meta(2),
				asserts.number("cityArea", 2, 319.35, 217.22, 217.22, 536.57, 268.285, 268.285),
				asserts.date("cityRights", 2),
				asserts.text("history", 2, []string{historyRotterdam, historyDusseldorf}, []int64{1, 1}),
				asserts.boolean("isCapital", 2, 2, 0, 1, 0),
				asserts.textArray("museums", 6, []string{"Museum Boijmans Van Beuningen", "Onomato", "Schiffahrt Museum", "Schlossturm", "Wereldmuseum"}, []int64{1, 1, 1, 1, 1}),
				asserts.text("name", 2, []string{"Dusseldorf", "Rotterdam"}, []int64{1, 1}),
				asserts.int("population", 2, 600000, 600000, 600000, 1200000, 600000, 600000),
				asserts.textArray("timezones", 4, []string{"CEST", "CET"}, []int64{2, 2}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"3.47e+06": {
				asserts.groupedBy("3.47e+06", "population"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"1.8e+06": {
				asserts.groupedBy("1.8e+06", "population"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"0": {
				asserts.groupedBy("0", "population"),
				asserts.meta(1),
				asserts.number0("cityArea"),
				asserts.date0("cityRights"),
				asserts.text0("history"),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray0("museums"),
				asserts.text("name", 1, []string{"Missing Island"}, []int64{1}),
				asserts.int("population", 1, 0, 0, 0, 0, 0, 0),
				asserts.textArray0("timezones"),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"3.47e+06": {
				asserts.groupedBy("3.47e+06", "population"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"1.8e+06": {
				asserts.groupedBy("1.8e+06", "population"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"population\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by timezones", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"CEST": {
				asserts.groupedBy("CEST", "timezones"),
				asserts.meta(4),
				asserts.number("cityArea", 4, 891.96, 217.22, 217.22, 2320.48, 605.6500000000001, 580.12),
				asserts.date("cityRights", 4),
				asserts.text("history", 4, []string{historyAmsterdam, historyRotterdam, historyBerlin, historyDusseldorf}, []int64{1, 1, 1, 1}),
				asserts.boolean("isCapital", 4, 2, 2, 0.5, 0.5),
				asserts.textArray("museums", 9, []string{"German Historical Museum", "Museum Boijmans Van Beuningen", "Onomato", "Rijksmuseum", "Schiffahrt Museum"}, []int64{1, 1, 1, 1, 1}),
				asserts.text("name", 4, []string{"Amsterdam", "Berlin", "Dusseldorf", "Rotterdam"}, []int64{1, 1, 1, 1}),
				asserts.int("population", 4, 3470000, 600000, 600000, 6470000, 1200000, 1617500),
				asserts.textArray("timezones", 8, []string{"CEST", "CET"}, []int64{4, 4}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"CET": {
				asserts.groupedBy("CET", "timezones"),
				asserts.meta(4),
				asserts.number("cityArea", 4, 891.96, 217.22, 217.22, 2320.48, 605.6500000000001, 580.12),
				asserts.date("cityRights", 4),
				asserts.text("history", 4, []string{historyAmsterdam, historyRotterdam, historyBerlin, historyDusseldorf}, []int64{1, 1, 1, 1}),
				asserts.boolean("isCapital", 4, 2, 2, 0.5, 0.5),
				asserts.textArray("museums", 9, []string{"German Historical Museum", "Museum Boijmans Van Beuningen", "Onomato", "Rijksmuseum", "Schiffahrt Museum"}, []int64{1, 1, 1, 1, 1}),
				asserts.text("name", 4, []string{"Amsterdam", "Berlin", "Dusseldorf", "Rotterdam"}, []int64{1, 1, 1, 1}),
				asserts.int("population", 4, 3470000, 600000, 600000, 6470000, 1200000, 1617500),
				asserts.textArray("timezones", 8, []string{"CEST", "CET"}, []int64{4, 4}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"CEST": {
				asserts.groupedBy("CEST", "timezones"),
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
			},
			"CET": {
				asserts.groupedBy("CET", "timezones"),
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
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"timezones\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})

	t.Run("aggregate City with group by museums", func(t *testing.T) {
		asserts := newAggregateResponseAssert(t)
		testCasesGen := &aggregateCityTestCases{}

		expectedAllResultsAssertions := map[string][]assertFunc{
			"German Historical Museum": {
				asserts.groupedBy("German Historical Museum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Rijksmuseum": {
				asserts.groupedBy("Rijksmuseum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Stedelijk Museum": {
				asserts.groupedBy("Stedelijk Museum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Onomato": {
				asserts.groupedBy("Onomato", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Schiffahrt Museum": {
				asserts.groupedBy("Schiffahrt Museum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Schlossturm": {
				asserts.groupedBy("Schlossturm", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyDusseldorf}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Dusseldorf"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Museum Boijmans Van Beuningen": {
				asserts.groupedBy("Museum Boijmans Van Beuningen", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Wereldmuseum": {
				asserts.groupedBy("Wereldmuseum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Witte de With Center for Contemporary Art": {
				asserts.groupedBy("Witte de With Center for Contemporary Art", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyRotterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 1, 0, 1, 0),
				asserts.textArray("museums", 3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				asserts.text("name", 1, []string{"Rotterdam"}, []int64{1}),
				asserts.int("population", 1, 600000, 600000, 600000, 600000, 600000, 600000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithDataAssertions := map[string][]assertFunc{
			"German Historical Museum": {
				asserts.groupedBy("German Historical Museum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyBerlin}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 1, []string{"German Historical Museum"}, []int64{1}),
				asserts.text("name", 1, []string{"Berlin"}, []int64{1}),
				asserts.int("population", 1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Rijksmuseum": {
				asserts.groupedBy("Rijksmuseum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
			"Stedelijk Museum": {
				asserts.groupedBy("Stedelijk Museum", "museums"),
				asserts.meta(1),
				asserts.number("cityArea", 1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				asserts.date("cityRights", 1),
				asserts.text("history", 1, []string{historyAmsterdam}, []int64{1}),
				asserts.boolean("isCapital", 1, 0, 1, 0, 1),
				asserts.textArray("museums", 2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				asserts.text("name", 1, []string{"Amsterdam"}, []int64{1}),
				asserts.int("population", 1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				asserts.textArray("timezones", 2, []string{"CEST", "CET"}, []int64{1, 1}),
				asserts.pointingTo("inCountry", "Country"),
			},
		}
		expectedResultsWithoutDataAssertions := map[string][]assertFunc{}
		expectedNoResultsAssertions := map[string][]assertFunc{}

		testCases := []aggregateTestCase{
			testCasesGen.WithoutFilters(expectedAllResultsAssertions),

			testCasesGen.WithWhereFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereFilter_NoResults(expectedNoResultsAssertions),

			testCasesGen.WithNearObjectFilter_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithNearObjectFilter_ResultsWithoutData(expectedResultsWithoutDataAssertions),

			testCasesGen.WithWhereAndNearObjectFilters_AllResults(expectedAllResultsAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithData(expectedResultsWithDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_ResultsWithoutData(expectedResultsWithoutDataAssertions),
			testCasesGen.WithWhereAndNearObjectFilters_NoResults(expectedNoResultsAssertions),
		}

		for _, tc := range testCases {
			query := aggregateCityQuery(tc.filters, "groupBy: [\"museums\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)

				assert.Len(t, extracted, len(tc.groupedAssertions))
				for groupedBy, groupAssertions := range tc.groupedAssertions {
					group := findGroup(groupedBy, extracted)
					require.NotNil(t, group, fmt.Sprintf("Group '%s' not found", groupedBy))

					for _, assertion := range groupAssertions {
						assertion(group)
					}
				}
			})
		}
	})
}

func extractArrayClassGroupByResult(result *graphqlhelper.GraphQLResult) []interface{} {
	return extractAggregateResult(result, arrayClassName)
}

func extractDuplicatesClassGroupByResult(result *graphqlhelper.GraphQLResult) []interface{} {
	return extractAggregateResult(result, duplicatesClassName)
}

func extractCityGroupByResult(result *graphqlhelper.GraphQLResult) []interface{} {
	return extractAggregateResult(result, cityClassName)
}

func findGroup(groupedBy string, groups []interface{}) map[string]interface{} {
	for i := range groups {
		if group, ok := groups[i].(map[string]interface{}); !ok {
			continue
		} else if gb, exists := group["groupedBy"]; !exists {
			continue
		} else if gbm, ok := gb.(map[string]interface{}); !ok {
			continue
		} else if value, exists := gbm["value"]; !exists {
			continue
		} else if groupedByValue, ok := value.(string); !ok {
			continue
		} else if groupedByValue == groupedBy {
			return group
		}
	}
	return nil
}
