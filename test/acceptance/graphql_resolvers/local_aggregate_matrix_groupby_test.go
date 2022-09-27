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

func aggregateArrayClassWithGroupByTest(t *testing.T) {
	h := &gqlAggregateResponseHelper{}
	testCasesGen := &aggregateArrayClassTestCases{}

	t.Run("aggregate ArrayClass with group by strings", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Astr", "strings"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Bstr", "strings"),
				"meta":           h.meta(3),
				"booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				"strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				"texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				"numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				"ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				"datesAsStrings": h.dates(9),
				"dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Cstr", "strings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Dstr", "strings"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Astr", "strings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Bstr", "strings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Cstr", "strings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Dstr", "strings"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"strings\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate ArrayClass with group by texts", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Atxt", "texts"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Btxt", "texts"),
				"meta":           h.meta(3),
				"booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				"strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				"texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				"numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				"ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				"datesAsStrings": h.dates(9),
				"dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Ctxt", "texts"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Dtxt", "texts"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Atxt", "texts"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Btxt", "texts"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Ctxt", "texts"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("Dtxt", "texts"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"texts\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate ArrayClass with group by ints", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("101", "ints"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("102", "ints"),
				"meta":           h.meta(3),
				"booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				"strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				"texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				"numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				"ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				"datesAsStrings": h.dates(9),
				"dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("103", "ints"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("104", "ints"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("101", "ints"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("102", "ints"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("103", "ints"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("104", "ints"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"ints\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate ArrayClass with group by numbers", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("1", "numbers"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2", "numbers"),
				"meta":           h.meta(3),
				"booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				"strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				"texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				"numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				"ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				"datesAsStrings": h.dates(9),
				"dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("3", "numbers"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("4", "numbers"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("1", "numbers"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2", "numbers"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("3", "numbers"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("4", "numbers"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"numbers\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate ArrayClass with group by dates", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2001-06-01T12:00:00Z", "dates"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2002-06-02T12:00:00Z", "dates"),
				"meta":           h.meta(3),
				"booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				"strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				"texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				"numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				"ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				"datesAsStrings": h.dates(9),
				"dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2003-06-03T12:00:00Z", "dates"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2004-06-04T12:00:00Z", "dates"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2001-06-01T12:00:00Z", "dates"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2002-06-02T12:00:00Z", "dates"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2003-06-03T12:00:00Z", "dates"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2004-06-04T12:00:00Z", "dates"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"dates\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate ArrayClass with group by dates as strings", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2021-06-01T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2022-06-02T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(3),
				"booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				"strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				"texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				"numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				"ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				"datesAsStrings": h.dates(9),
				"dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2023-06-03T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2024-06-04T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2021-06-01T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2022-06-02T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2023-06-03T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("2024-06-04T22:18:59.640162Z", "datesAsStrings"),
				"meta":           h.meta(1),
				"booleans":       h.booleans(4, 1, 3, 0.25, 0.75),
				"strings":        h.strings(4, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{1, 1, 1, 1}),
				"texts":          h.texts(4, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{1, 1, 1, 1}),
				"numbers":        h.numbers(4, 4, 1, 1, 10, 2.5, 2.5),
				"ints":           h.ints(4, 104, 101, 101, 410, 102.5, 102.5),
				"datesAsStrings": h.dates(4),
				"dates":          h.dates(4),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"datesAsStrings\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate ArrayClass with group by booleans", func(t *testing.T) {
		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("true", "booleans"),
				"meta":           h.meta(6),
				"booleans":       h.booleans(20, 6, 14, 0.3, 0.7),
				"strings":        h.strings(20, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{6, 6, 5, 3}),
				"texts":          h.texts(20, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{6, 6, 5, 3}),
				"numbers":        h.numbers(20, 4, 1, 1, 45, 2, 2.25),
				"ints":           h.ints(20, 104, 101, 101, 2045, 102, 102.25),
				"datesAsStrings": h.dates(20),
				"dates":          h.dates(20),
				// TODO fix grouping of duplicates
				// "meta":           h.meta(3),
				// "booleans":       h.booleans(9, 3, 6, 0.3333333333333333, 0.6666666666666666),
				// "strings":        h.strings(9, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{3, 3, 2, 1}),
				// "texts":          h.texts(9, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{3, 3, 2, 1}),
				// "numbers":        h.numbers(9, 4, 1, 1, 19, 2, 2.111111111111111),
				// "ints":           h.ints(9, 104, 101, 101, 919, 102, 102.11111111111111),
				// "datesAsStrings": h.dates(9),
				// "dates":          h.dates(9),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("false", "booleans"),
				"meta":           h.meta(4),
				"booleans":       h.booleans(10, 4, 6, 0.4, 0.6),
				"strings":        h.strings(10, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{4, 3, 2, 1}),
				"texts":          h.texts(10, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{4, 3, 2, 1}),
				"numbers":        h.numbers(10, 4, 1, 1, 20, 2, 2),
				"ints":           h.ints(10, 104, 101, 101, 1020, 102, 102),
				"datesAsStrings": h.dates(10),
				"dates":          h.dates(10),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":      h.groupedBy("true", "booleans"),
				"meta":           h.meta(5),
				"booleans":       h.booleans(18, 5, 13, 0.2777777777777778, 0.7222222222222222),
				"strings":        h.strings(18, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{5, 5, 5, 3}),
				"texts":          h.texts(18, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{5, 5, 5, 3}),
				"numbers":        h.numbers(18, 4, 1, 1, 42, 2, 2.3333333333333335),
				"ints":           h.ints(18, 104, 101, 101, 1842, 102, 102.33333333333333),
				"datesAsStrings": h.dates(18),
				"dates":          h.dates(18),
				// TODO fix grouping of duplicates
				// "meta":           h.meta(2),
				// "booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				// "strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				// "texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				// "numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				// "ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				// "datesAsStrings": h.dates(7),
				// "dates":          h.dates(7),
			},
			map[string]interface{}{
				"groupedBy":      h.groupedBy("false", "booleans"),
				"meta":           h.meta(2),
				"booleans":       h.booleans(7, 2, 5, 0.2857142857142857, 0.7142857142857143),
				"strings":        h.strings(7, []string{"Astr", "Bstr", "Cstr", "Dstr"}, []int64{2, 2, 2, 1}),
				"texts":          h.texts(7, []string{"Atxt", "Btxt", "Ctxt", "Dtxt"}, []int64{2, 2, 2, 1}),
				"numbers":        h.numbers(7, 4, 1, 1, 16, 2, 2.2857142857142856),
				"ints":           h.ints(7, 104, 101, 101, 716, 102, 102.28571428571429),
				"datesAsStrings": h.dates(7),
				"dates":          h.dates(7),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateArrayClassQuery(tc.filters, "groupBy: [\"booleans\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractArrayClassGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})
}

func aggregateCityClassWithGroupByTest(t *testing.T) {
	t.Run("aggregate City with group by city area", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("891.96", "cityArea"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("891.95", "cityArea"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("217.22", "cityArea"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("319.35", "cityArea"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("891.96", "cityArea"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("891.95", "cityArea"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"cityArea\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by history", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy(historyBerlin, "history"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy(historyAmsterdam, "history"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy(historyDusseldorf, "history"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy(historyRotterdam, "history"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy(historyBerlin, "history"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy(historyAmsterdam, "history"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"history\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by name", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Berlin", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Amsterdam", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Dusseldorf", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Rotterdam", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Missing Island", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number0(),
				"cityRights": h.date0(),
				"history":    h.text0(),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts0(),
				"name":       h.string(1, []string{"Missing Island"}, []int64{1}),
				"population": h.int(1, 0, 0, 0, 0, 0, 0),
				"timezones":  h.strings0(),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Berlin", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Amsterdam", "name"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"name\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by is capital", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("true", "isCapital"),
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
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("false", "isCapital"),
				"meta":       h.meta(3),
				"cityArea":   h.number(2, 319.35, 217.22, 217.22, 536.57, 268.285, 268.285),
				"cityRights": h.date(2),
				"history":    h.text(2, []string{historyRotterdam, historyDusseldorf}, []int64{1, 1}),
				"isCapital":  h.boolean(3, 3, 0, 1, 0),
				"museums":    h.texts(6, []string{"Museum Boijmans Van Beuningen", "Onomato", "Schiffahrt Museum", "Schlossturm", "Wereldmuseum"}, []int64{1, 1, 1, 1, 1}),
				"name":       h.string(3, []string{"Dusseldorf", "Missing Island", "Rotterdam"}, []int64{1, 1, 1}),
				"population": h.int(3, 600000, 0, 600000, 1200000, 600000, 400000),
				"timezones":  h.strings(4, []string{"CEST", "CET"}, []int64{2, 2}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("true", "isCapital"),
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
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"isCapital\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by name", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("1400-01-01T00:00:00+02:00", "cityRights"),
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
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("1135-01-01T00:00:00+02:00", "cityRights"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("1283-01-01T00:00:00+02:00", "cityRights"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("1400-01-01T00:00:00+02:00", "cityRights"),
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
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"cityRights\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by population", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("600000", "population"),
				"meta":       h.meta(2),
				"cityArea":   h.number(2, 319.35, 217.22, 217.22, 536.57, 268.285, 268.285),
				"cityRights": h.date(2),
				"history":    h.text(2, []string{historyRotterdam, historyDusseldorf}, []int64{1, 1}),
				"isCapital":  h.boolean(2, 2, 0, 1, 0),
				"museums":    h.texts(6, []string{"Museum Boijmans Van Beuningen", "Onomato", "Schiffahrt Museum", "Schlossturm", "Wereldmuseum"}, []int64{1, 1, 1, 1, 1}),
				"name":       h.string(2, []string{"Dusseldorf", "Rotterdam"}, []int64{1, 1}),
				"population": h.int(2, 600000, 600000, 600000, 1200000, 600000, 600000),
				"timezones":  h.strings(4, []string{"CEST", "CET"}, []int64{2, 2}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("3.47e+06", "population"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("1.8e+06", "population"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("0", "population"),
				"meta":       h.meta(1),
				"cityArea":   h.number0(),
				"cityRights": h.date0(),
				"history":    h.text0(),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts0(),
				"name":       h.string(1, []string{"Missing Island"}, []int64{1}),
				"population": h.int(1, 0, 0, 0, 0, 0, 0),
				"timezones":  h.strings0(),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("3.47e+06", "population"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("1.8e+06", "population"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"population\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by timezones", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("CEST", "timezones"),
				"meta":       h.meta(4),
				"cityArea":   h.number(4, 891.96, 217.22, 217.22, 2320.48, 605.6500000000001, 580.12),
				"cityRights": h.date(4),
				"history":    h.text(4, []string{historyAmsterdam, historyRotterdam, historyBerlin, historyDusseldorf}, []int64{1, 1, 1, 1}),
				"isCapital":  h.boolean(4, 2, 2, 0.5, 0.5),
				"museums":    h.texts(9, []string{"German Historical Museum", "Museum Boijmans Van Beuningen", "Onomato", "Rijksmuseum", "Schiffahrt Museum"}, []int64{1, 1, 1, 1, 1}),
				"name":       h.string(4, []string{"Amsterdam", "Berlin", "Dusseldorf", "Rotterdam"}, []int64{1, 1, 1, 1}),
				"population": h.int(4, 3470000, 600000, 600000, 6470000, 1200000, 1617500),
				"timezones":  h.strings(8, []string{"CEST", "CET"}, []int64{4, 4}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("CET", "timezones"),
				"meta":       h.meta(4),
				"cityArea":   h.number(4, 891.96, 217.22, 217.22, 2320.48, 605.6500000000001, 580.12),
				"cityRights": h.date(4),
				"history":    h.text(4, []string{historyAmsterdam, historyRotterdam, historyBerlin, historyDusseldorf}, []int64{1, 1, 1, 1}),
				"isCapital":  h.boolean(4, 2, 2, 0.5, 0.5),
				"museums":    h.texts(9, []string{"German Historical Museum", "Museum Boijmans Van Beuningen", "Onomato", "Rijksmuseum", "Schiffahrt Museum"}, []int64{1, 1, 1, 1, 1}),
				"name":       h.string(4, []string{"Amsterdam", "Berlin", "Dusseldorf", "Rotterdam"}, []int64{1, 1, 1, 1}),
				"population": h.int(4, 3470000, 600000, 600000, 6470000, 1200000, 1617500),
				"timezones":  h.strings(8, []string{"CEST", "CET"}, []int64{4, 4}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("CEST", "timezones"),
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
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("CET", "timezones"),
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
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"timezones\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})

	t.Run("aggregate City with group by museums", func(t *testing.T) {
		h := &gqlAggregateResponseHelper{}
		testCasesGen := &aggregateCityTestCases{}

		allResults := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("German Historical Museum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Rijksmuseum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Stedelijk Museum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Onomato", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Schiffahrt Museum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Schlossturm", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 217.22, 217.22, 217.22, 217.22, 217.22, 217.22),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyDusseldorf}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Onomato", "Schiffahrt Museum", "Schlossturm"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Dusseldorf"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Museum Boijmans Van Beuningen", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Wereldmuseum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Witte de With Center for Contemporary Art", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 319.35, 319.35, 319.35, 319.35, 319.35, 319.35),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyRotterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 1, 0, 1, 0),
				"museums":    h.texts(3, []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"}, []int64{1, 1, 1}),
				"name":       h.string(1, []string{"Rotterdam"}, []int64{1}),
				"population": h.int(1, 600000, 600000, 600000, 600000, 600000, 600000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithData := []interface{}{
			map[string]interface{}{
				"groupedBy":  h.groupedBy("German Historical Museum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.96, 891.96, 891.96, 891.96, 891.96, 891.96),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyBerlin}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(1, []string{"German Historical Museum"}, []int64{1}),
				"name":       h.string(1, []string{"Berlin"}, []int64{1}),
				"population": h.int(1, 3470000, 3470000, 3470000, 3470000, 3470000, 3470000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Rijksmuseum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
			map[string]interface{}{
				"groupedBy":  h.groupedBy("Stedelijk Museum", "museums"),
				"meta":       h.meta(1),
				"cityArea":   h.number(1, 891.95, 891.95, 891.95, 891.95, 891.95, 891.95),
				"cityRights": h.date(1),
				"history":    h.text(1, []string{historyAmsterdam}, []int64{1}),
				"isCapital":  h.boolean(1, 0, 1, 0, 1),
				"museums":    h.texts(2, []string{"Rijksmuseum", "Stedelijk Museum"}, []int64{1, 1}),
				"name":       h.string(1, []string{"Amsterdam"}, []int64{1}),
				"population": h.int(1, 1800000, 1800000, 1800000, 1800000, 1800000, 1800000),
				"timezones":  h.strings(2, []string{"CEST", "CET"}, []int64{1, 1}),
				"inCountry":  h.pointingTo("Country"),
			},
		}
		resultsWithoutData := []interface{}{}
		noResults := []interface{}{}

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
			query := aggregateCityQuery(tc.filters, "groupBy: [\"museums\"]")

			t.Run(tc.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				extracted := extractCityGroupByResult(result)
				expected := tc.expected.([]interface{})

				assert.ElementsMatch(t, expected, extracted)
			})
		}
	})
}

func extractArrayClassGroupByResult(result *graphqlhelper.GraphQLResult) []interface{} {
	return extractAggregateResult(result, arrayClassName)
}

func extractCityGroupByResult(result *graphqlhelper.GraphQLResult) []interface{} {
	return extractAggregateResult(result, cityClassName)
}
