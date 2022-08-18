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
	"fmt"
	"reflect"
	"testing"

	"github.com/semi-technologies/weaviate/test/helper"
	graphqlhelper "github.com/semi-technologies/weaviate/test/helper/graphql"
	"github.com/stretchr/testify/require"
)

func aggregatesArrayClassWithGroupingAndNearObject(t *testing.T) {
	tests := []struct {
		name     string
		property string
		expected map[string]string
	}{
		{
			name:     "ints",
			property: "ints",
			expected: map[string]string{"1": "3", "2": "2", "3": "1"},
		},
		{
			name:     "numbers",
			property: "numbers",
			expected: map[string]string{"1": "3", "2": "2", "3": "1"},
		},
		{
			name:     "strings",
			property: "strings",
			expected: map[string]string{"a": "3", "b": "2", "c": "1"},
		},
		{
			name:     "texts",
			property: "texts",
			expected: map[string]string{"a": "3", "b": "2", "c": "1"},
		},
		{
			name:     "dates",
			property: "dates",
			expected: map[string]string{
				"2022-06-01T22:18:59.640162Z": "3",
				"2022-06-02T22:18:59.640162Z": "2",
				"2022-06-03T22:18:59.640162Z": "1",
			},
		},
		{
			name:     "booleans",
			property: "booleans",
			expected: map[string]string{"true": "3", "false": "3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := `
				{
					Aggregate{
						ArrayClass(
							%s
							groupBy:["%s"]
						){
							meta{
								count
							}
							groupedBy{
								value
							}
						}
					}
				}
			`
			testCase := func(t *testing.T, query string, expected map[string]string) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)

				groupedByResults := result.Get("Aggregate", "ArrayClass").AsSlice()
				require.GreaterOrEqual(t, len(groupedByResults), 2)

				results := make(map[string]string)
				for _, res := range groupedByResults {
					meta := res.(map[string]interface{})["meta"]
					count := meta.(map[string]interface{})["count"]
					groupedBy := res.(map[string]interface{})["groupedBy"]
					value := groupedBy.(map[string]interface{})["value"]
					valueString := value.(string)

					results[valueString] = fmt.Sprintf("%v", count)
				}

				if equal := reflect.DeepEqual(results, expected); !equal {
					t.Errorf("results don't match got: %+v want: %+v", results, expected)
				}
			}
			t.Run("with nearObject", func(t *testing.T) {
				queryWithNearObject := fmt.Sprintf(query,
					`nearObject:{id: "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a" certainty: 0.1}`,
					tt.property)
				testCase(t, queryWithNearObject, tt.expected)
			})
			t.Run("without nearObject", func(t *testing.T) {
				queryWithoutNearObject := fmt.Sprintf(query, "", tt.property)
				testCase(t, queryWithoutNearObject, tt.expected)
			})
		})
	}
}
