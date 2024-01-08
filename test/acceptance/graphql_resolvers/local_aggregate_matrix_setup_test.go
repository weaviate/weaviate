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
	"time"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const notExistingObjectId = "cfa3b21e-ca5f-4db7-a412-ffffffffffff"

const (
	arrayClassName = "ArrayClass"

	objectArrayClassID1_4el   = "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a"
	objectArrayClassID2_3el   = "cfa3b21e-ca5f-4db7-a412-5fc6a23c534b"
	objectArrayClassID3_2el   = "cfa3b21e-ca5f-4db7-a412-5fc6a23c535a"
	objectArrayClassID4_1el   = "cfa3b21e-ca5f-4db7-a412-5fc6a23c535b"
	objectArrayClassID5_0el   = "cfa3b21e-ca5f-4db7-a412-5fc6a23c536a"
	objectArrayClassID6_nils  = "cfa3b21e-ca5f-4db7-a412-5fc6a23c536b"
	objectArrayClassID7_empty = "cfa3b21e-ca5f-4db7-a412-5fc6a23c536c"
)

const (
	noPropsClassName = "ClassWithoutProperties"

	objectNoPropsClassID1 = "dfa3b21e-ca5f-4db7-a412-5fc6a23c5301"
	objectNoPropsClassID2 = "dfa3b21e-ca5f-4db7-a412-5fc6a23c5311"
)

const (
	cityClassName = "City"
)

const (
	duplicatesClassName = "DuplicatesClass"

	objectDuplicatesClassID1_4el = "a8076f34-ec16-4333-a963-00c89c5ba001"
	objectDuplicatesClassID2_3el = "a8076f34-ec16-4333-a963-00c89c5ba002"
	objectDuplicatesClassID3_2el = "a8076f34-ec16-4333-a963-00c89c5ba003"
)

func arrayClassSchema() *models.Class {
	return &models.Class{
		Class: arrayClassName,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexPropertyLength: true, IndexNullState: true},
		Properties: []*models.Property{
			{
				Name:         "texts",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "numbers",
				DataType: []string{"number[]"},
			},
			{
				Name:     "ints",
				DataType: []string{"int[]"},
			},
			{
				Name:     "booleans",
				DataType: []string{"boolean[]"},
			},
			{
				Name:     "datesAsStrings",
				DataType: []string{"date[]"},
			},
			{
				Name:     "dates",
				DataType: []string{"date[]"},
			},
		},
	}
}

func arrayClassObjects() []*models.Object {
	return []*models.Object{
		objectArrayClass4el(),
		objectArrayClass3el(),
		objectArrayClass2el(),
		objectArrayClass1el(),
		objectArrayClass0el(),
		objectArrayClassNils(),
		objectArrayClassEmpty(),
	}
}

func objectArrayClass4el() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID1_4el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt", "Btxt", "Ctxt", "Dtxt"},
			"numbers":  []float64{1.0, 2.0, 3.0, 4.0},
			"ints":     []int{101, 102, 103, 104},
			"booleans": []bool{true, true, true, false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
				"2022-06-02T22:18:59.640162Z",
				"2023-06-03T22:18:59.640162Z",
				"2024-06-04T22:18:59.640162Z",
			},
			"dates": []time.Time{
				time.Date(2001, 6, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2002, 6, 2, 12, 0, 0, 0, time.UTC),
				time.Date(2003, 6, 3, 12, 0, 0, 0, time.UTC),
				time.Date(2004, 6, 4, 12, 0, 0, 0, time.UTC),
			},
		},
	}
}

func objectArrayClass3el() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID2_3el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt", "Btxt", "Ctxt"},
			"numbers":  []float64{1.0, 2.0, 3.0},
			"ints":     []int{101, 102, 103},
			"booleans": []bool{true, true, false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
				"2022-06-02T22:18:59.640162Z",
				"2023-06-03T22:18:59.640162Z",
			},
			"dates": []time.Time{
				time.Date(2001, 6, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2002, 6, 2, 12, 0, 0, 0, time.UTC),
				time.Date(2003, 6, 3, 12, 0, 0, 0, time.UTC),
			},
		},
	}
}

func objectArrayClass2el() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID3_2el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt", "Btxt"},
			"numbers":  []float64{1.0, 2.0},
			"ints":     []int{101, 102},
			"booleans": []bool{true, false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
				"2022-06-02T22:18:59.640162Z",
			},
			"dates": []time.Time{
				time.Date(2001, 6, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2002, 6, 2, 12, 0, 0, 0, time.UTC),
			},
		},
	}
}

func objectArrayClass1el() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID4_1el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt"},
			"numbers":  []float64{1.0},
			"ints":     []int{101},
			"booleans": []bool{false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
			},
			"dates": []time.Time{
				time.Date(2001, 6, 1, 12, 0, 0, 0, time.UTC),
			},
		},
	}
}

func objectArrayClass0el() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID5_0el,
		Properties: map[string]interface{}{
			"texts":          []string{},
			"numbers":        []float64{},
			"ints":           []int{},
			"booleans":       []bool{},
			"datesAsStrings": []string{},
			"dates":          []time.Time{},
		},
	}
}

func objectArrayClassNils() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID6_nils,
		Properties: map[string]interface{}{
			"texts":          nil,
			"numbers":        nil,
			"ints":           nil,
			"booleans":       nil,
			"datesAsStrings": nil,
			"dates":          nil,
		},
	}
}

func objectArrayClassEmpty() *models.Object {
	return &models.Object{
		Class: arrayClassName,
		ID:    objectArrayClassID7_empty,
	}
}

func aggregateArrayClassQuery(filters, groupBy string) string {
	query := `{
			Aggregate {
				%s
				%s
				{
					meta{
						count
					}
					booleans{
						count
						type
						totalTrue
						totalFalse
						percentageTrue
						percentageFalse
					}
					texts{
						count
						type
						topOccurrences {
							value
							occurs
						}
					}
					numbers{
						count
						type
						minimum
						maximum
						mean
						median
						mode
						sum
					}
					ints{
						count
						type
						minimum
						maximum
						mean
						median
						mode
						sum
					}
					datesAsStrings{
						count
					}
					dates{
						count
					}
					%s
				}
			}
		}`

	params := ""
	if filters != "" || groupBy != "" {
		params = fmt.Sprintf(
			`(
				%s
				%s
			)`, filters, groupBy)
	}
	groupedBy := ""
	if groupBy != "" {
		groupedBy = `groupedBy{
						value
						path
					}`
	}

	return fmt.Sprintf(query, arrayClassName, params, groupedBy)
}

func extractAggregateResult(result *graphqlhelper.GraphQLResult, className string) []interface{} {
	return result.Get("Aggregate", className).AsSlice()
}

func noPropsClassSchema() *models.Class {
	return &models.Class{
		Class: noPropsClassName,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
	}
}

func noPropsClassObjects() []*models.Object {
	return []*models.Object{
		objectNoPropsClass1(),
		objectNoPropsClass2(),
	}
}

func objectNoPropsClass1() *models.Object {
	return &models.Object{
		Class: noPropsClassName,
		ID:    objectNoPropsClassID1,
	}
}

func objectNoPropsClass2() *models.Object {
	return &models.Object{
		Class: noPropsClassName,
		ID:    objectNoPropsClassID2,
	}
}

func aggregateNoPropsQuery(filters string) string {
	query := `
			{
				Aggregate {
					%s
					%s
					{
						meta{
							count
						}
					}
				}
			}
		`

	params := ""
	if filters != "" {
		params = fmt.Sprintf(
			`(
				%s
			)`, filters)
	}

	return fmt.Sprintf(query, noPropsClassName, params)
}

func aggregateCityQuery(filters, groupBy string) string {
	query := `{
			Aggregate {
				%s
				%s
				{
					meta {
						count
					}
					name {
						count
						type
						topOccurrences {
							value
							occurs
						}
					}
					cityArea {
						count
						type
						minimum
						maximum
						mean
						median
						mode
						sum
					}
					isCapital {
						count
						type
						totalTrue
						totalFalse
						percentageTrue
						percentageFalse
					}
					population {
						count
						type
						minimum
						maximum
						mean
						median
						mode
						sum
					}
					cityRights {
						count
					}
					history {
						count
						type
						topOccurrences {
							value
							occurs
						}
					}
					museums {
						count
						type
						topOccurrences {
							value
							occurs
						}
					}
					timezones {
						count
						type
						topOccurrences {
							value
							occurs
						}
					}
					inCountry {
						pointingTo
						type
					}
					%s
				}
			}
		}`

	params := ""
	if filters != "" || groupBy != "" {
		params = fmt.Sprintf(
			`(
				%s
				%s
			)`, filters, groupBy)
	}
	groupedBy := ""
	if groupBy != "" {
		groupedBy = `groupedBy{
						value
						path
					}`
	}

	return fmt.Sprintf(query, cityClassName, params, groupedBy)
}

func duplicatesClassSchema() *models.Class {
	return &models.Class{
		Class: duplicatesClassName,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "texts",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:     "numbers",
				DataType: []string{"number[]"},
			},
			{
				Name:     "ints",
				DataType: []string{"int[]"},
			},
			{
				Name:     "booleans",
				DataType: []string{"boolean[]"},
			},
			{
				Name:     "datesAsStrings",
				DataType: []string{"date[]"},
			},
		},
	}
}

func duplicatesClassObjects() []*models.Object {
	return []*models.Object{
		objectDuplicatesClass4el(),
		objectDuplicatesClass3el(),
		objectDuplicatesClass2el(),
	}
}

func objectDuplicatesClass4el() *models.Object {
	return &models.Object{
		Class: duplicatesClassName,
		ID:    objectDuplicatesClassID1_4el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt", "Atxt", "Atxt", "Btxt"},
			"numbers":  []float64{1.0, 1.0, 1.0, 2.0},
			"ints":     []int{101, 101, 101, 102},
			"booleans": []bool{true, true, true, false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
				"2021-06-01T22:18:59.640162Z",
				"2021-06-01T22:18:59.640162Z",
				"2022-06-02T22:18:59.640162Z",
			},
		},
	}
}

func objectDuplicatesClass3el() *models.Object {
	return &models.Object{
		Class: duplicatesClassName,
		ID:    objectDuplicatesClassID2_3el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt", "Atxt", "Btxt"},
			"numbers":  []float64{1.0, 1.0, 2.0},
			"ints":     []int{101, 101, 102},
			"booleans": []bool{true, true, false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
				"2021-06-01T22:18:59.640162Z",
				"2022-06-02T22:18:59.640162Z",
			},
		},
	}
}

func objectDuplicatesClass2el() *models.Object {
	return &models.Object{
		Class: duplicatesClassName,
		ID:    objectDuplicatesClassID3_2el,
		Properties: map[string]interface{}{
			"texts":    []string{"Atxt", "Btxt"},
			"numbers":  []float64{1.0, 2.0},
			"ints":     []int{101, 102},
			"booleans": []bool{true, false},
			"datesAsStrings": []string{
				"2021-06-01T22:18:59.640162Z",
				"2022-06-02T22:18:59.640162Z",
			},
		},
	}
}

func aggregateDuplicatesClassQuery(filters, groupBy string) string {
	query := `{
			Aggregate {
				%s
				%s
				{
					meta{
						count
					}
					booleans{
						count
						type
						totalTrue
						totalFalse
						percentageTrue
						percentageFalse
					}
					texts{
						count
						type
						topOccurrences {
							value
							occurs
						}
					}
					numbers{
						count
						type
						minimum
						maximum
						mean
						median
						mode
						sum
					}
					ints{
						count
						type
						minimum
						maximum
						mean
						median
						mode
						sum
					}
					datesAsStrings{
						count
					}
					%s
				}
			}
		}`

	params := ""
	if filters != "" || groupBy != "" {
		params = fmt.Sprintf(
			`(
				%s
				%s
			)`, filters, groupBy)
	}
	groupedBy := ""
	if groupBy != "" {
		groupedBy = `groupedBy{
						value
						path
					}`
	}

	return fmt.Sprintf(query, duplicatesClassName, params, groupedBy)
}

type aggregateTestCase struct {
	name              string
	filters           string
	groupedAssertions map[string][]assertFunc // map[groupedBy]assertionsForGroup
}

type aggregateArrayClassTestCases struct{}

func (tc *aggregateArrayClassTestCases) WithoutFilters(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name:              "without filters",
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (all results)",
		filters: `
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}`,
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereFilter_ResultsWithData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (results with data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, objectArrayClassID1_4el[:35]+"?"),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereFilter_ResultsWithoutData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (results without data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, objectArrayClassID5_0el[:35]+"?"),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereFilter_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, notExistingObjectId),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithNearObjectFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (all results)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, objectArrayClassID1_4el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithNearObjectFilter_ResultsWithData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (results with data)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 0.988
			}`, objectArrayClassID1_4el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithNearObjectFilter_ResultsWithoutData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (results without data)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 1
			}`, objectArrayClassID5_0el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereAndNearObjectFilters_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (all results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, objectArrayClassID1_4el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereAndNearObjectFilters_ResultsWithData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (results with data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 0.98
			}`, objectArrayClassID1_4el[:35]+"?", objectArrayClassID1_4el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereAndNearObjectFilters_ResultsWithoutData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (results without data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 1
			}`, objectArrayClassID5_0el[:35]+"?", objectArrayClassID5_0el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateArrayClassTestCases) WithWhereAndNearObjectFilters_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, notExistingObjectId, objectArrayClassID1_4el),
		groupedAssertions: groupedAssertions,
	}
}

type aggregateNoPropsClassTestCases struct{}

func (tc *aggregateNoPropsClassTestCases) WithoutFilters(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name:              "without filters",
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithWhereFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (all results)",
		filters: `
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}`,
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithWhereFilter_SomeResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (some results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, objectNoPropsClassID1[:35]+"?"),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithWhereFilter_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, notExistingObjectId),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithNearObjectFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (all results)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, objectNoPropsClassID1),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithWhereAndNearObjectFilters_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (all results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, objectNoPropsClassID1),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithWhereAndNearObjectFilters_SomeResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (some results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 1
			}`, objectNoPropsClassID1[:35]+"?", objectNoPropsClassID1),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateNoPropsClassTestCases) WithWhereAndNearObjectFilters_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, notExistingObjectId, objectNoPropsClassID1),
		groupedAssertions: groupedAssertions,
	}
}

type aggregateCityTestCases struct{}

func (tc *aggregateCityTestCases) WithoutFilters(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name:              "without filters",
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (all results)",
		filters: `
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}`,
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereFilter_ResultsWithData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (results with data)",
		filters: `
			where: {
				operator: Equal,
				path: ["isCapital"],
				valueBoolean: true
			}`,
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereFilter_ResultsWithoutData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (results without data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, nullisland),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereFilter_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, notExistingObjectId),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithNearObjectFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (all results)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, berlin),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithNearObjectFilter_ResultsWithData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (results with data)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 0.81
			}`, berlin),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithNearObjectFilter_ResultsWithoutData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with nearObject filter (results without data)",
		filters: fmt.Sprintf(`
			nearObject: {
				id: "%s"
				certainty: 0.9
			}`, nullisland),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereAndNearObjectFilters_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (all results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, berlin),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereAndNearObjectFilters_ResultsWithData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (results with data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Equal,
				path: ["isCapital"],
				valueBoolean: true
			}
			nearObject: {
				id: "%s"
				certainty: 0.81
			}`, berlin),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereAndNearObjectFilters_ResultsWithoutData(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (results without data)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 0.9
			}`, nullisland, nullisland),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateCityTestCases) WithWhereAndNearObjectFilters_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where & nearObject filters (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}
			nearObject: {
				id: "%s"
				certainty: 0.1
			}`, notExistingObjectId, berlin),
		groupedAssertions: groupedAssertions,
	}
}

type aggregateDuplicatesClassTestCases struct{}

func (tc *aggregateDuplicatesClassTestCases) WithoutFilters(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name:              "without filters",
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateDuplicatesClassTestCases) WithWhereFilter_AllResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (all results)",
		filters: `
			where: {
				operator: Like
				path: ["id"]
				valueText: "*"
			}`,
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateDuplicatesClassTestCases) WithWhereFilter_SomeResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (some results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, objectDuplicatesClassID1_4el),
		groupedAssertions: groupedAssertions,
	}
}

func (tc *aggregateDuplicatesClassTestCases) WithWhereFilter_NoResults(groupedAssertions map[string][]assertFunc) aggregateTestCase {
	return aggregateTestCase{
		name: "with where filter (no results)",
		filters: fmt.Sprintf(`
			where: {
				operator: Like
				path: ["id"]
				valueText: "%s"
			}`, notExistingObjectId),
		groupedAssertions: groupedAssertions,
	}
}
