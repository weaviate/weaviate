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
	h := &gqlAggregateHelper{}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
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

				assert.Len(t, extracted, len(expected))
				for _, singleExpected := range expected {
					assert.Contains(t, extracted, singleExpected, "expected group not found in results array")
				}
			})
		}
	})
}

func extractArrayClassGroupByResult(result *graphqlhelper.GraphQLResult) []interface{} {
	return extractAggregateResult(result, arrayClassName)
}
