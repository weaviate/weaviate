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
	"reflect"
	"strings"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/test/helper"
)

func gettingObjectsWithSort(t *testing.T) {
	buildSort := func(path []string, order string) string {
		pathArgs := make([]string, len(path))
		for i := range path {
			pathArgs[i] = fmt.Sprintf("\"%s\"", path[i])
		}
		return fmt.Sprintf("{path:[%s] order:%s}", strings.Join(pathArgs, ","), order)
	}
	buildSortFilter := func(sort []string) string {
		return fmt.Sprintf("sort:[%s]", strings.Join(sort, ","))
	}

	t.Run("simple sort", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					sort: [{
						path: ["%s"]
						order: %s
					}]
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name            string
			property, order string
			expected        []interface{}
		}{
			{
				name:     "sort by name asc",
				property: "name",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
				},
			},
			{
				name:     "sort by name desc",
				property: "name",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by population asc",
				property: "population",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name:     "sort by population desc",
				property: "population",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by isCapital asc",
				property: "isCapital",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name:     "sort by isCapital desc",
				property: "isCapital",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by cityArea asc",
				property: "cityArea",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name:     "sort by cityArea desc",
				property: "cityArea",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by cityRights asc",
				property: "cityRights",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name:     "sort by cityRights desc",
				property: "cityRights",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by timezones asc",
				property: "timezones",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name:     "sort by timezones desc",
				property: "timezones",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by museums asc",
				property: "museums",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name:     "sort by museums desc",
				property: "museums",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by history asc",
				property: "history",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
				},
			},
			{
				name:     "sort by history desc",
				property: "history",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by phoneNumber asc",
				property: "phoneNumber",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name:     "sort by phoneNumber desc",
				property: "phoneNumber",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name:     "sort by location asc",
				property: "location",
				order:    "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
				},
			},
			{
				name:     "sort by location desc",
				property: "location",
				order:    "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Berlin"},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.property, tt.order))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("complex sort", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					%s
				) {
					name
				}
			}
		}
		`
		queryLimit := `
		{
			Get {
				City(
					limit: %d
					%s
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "sort by population and name asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "sort by population asc and name desc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "sort by name asc and population desc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
				},
			},
			{
				name: "sort by population and name desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name: "sort by phoneNumber and population and name asc",
				sort: []string{
					buildSort([]string{"phoneNumber"}, "asc"),
					buildSort([]string{"population"}, "asc"),
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "sort by isCapital asc and name asc",
				sort: []string{
					buildSort([]string{"isCapital"}, "asc"),
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "sort by isCapital asc and name desc",
				sort: []string{
					buildSort([]string{"isCapital"}, "asc"),
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "sort by isCapital desc and name asc",
				sort: []string{
					buildSort([]string{"isCapital"}, "desc"),
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name: "sort by isCapital desc and name desc",
				sort: []string{
					buildSort([]string{"isCapital"}, "desc"),
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name: "sort by isCapital asc and population desc and name asc",
				sort: []string{
					buildSort([]string{"isCapital"}, "asc"),
					buildSort([]string{"population"}, "desc"),
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "sort by isCapital desc and population desc and name desc",
				sort: []string{
					buildSort([]string{"isCapital"}, "desc"),
					buildSort([]string{"population"}, "desc"),
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name: "sort by isCapital asc and timezones asc and city rights asc and name asc",
				sort: []string{
					buildSort([]string{"isCapital"}, "asc"),
					buildSort([]string{"timezones"}, "asc"),
					buildSort([]string{"cityRights"}, "asc"),
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "sort by isCapital desc and timezones asc and city rights asc and name desc",
				sort: []string{
					buildSort([]string{"isCapital"}, "desc"),
					buildSort([]string{"timezones"}, "asc"),
					buildSort([]string{"cityRights"}, "asc"),
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": nil},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Run("without limit", func(t *testing.T) {
					result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
					got := result.Get("Get", "City").AsSlice()
					if !reflect.DeepEqual(got, tt.expected) {
						t.Errorf("sort objects got = %v, want %v", got, tt.expected)
					}
				})
				t.Run("with limit", func(t *testing.T) {
					limit := 4
					result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(queryLimit, limit, buildSortFilter(tt.sort)))
					got := result.Get("Get", "City").AsSlice()
					if !reflect.DeepEqual(got, tt.expected[:limit]) {
						t.Errorf("sort objects got = %v, want %v", got, tt.expected)
					}
				})
			})
		}
	})

	t.Run("sort with where", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					sort: [{
						path: ["location"]
						order: %s
					}]
					where: {
						operator: Or,
						operands: [
							{valueText: "6ffb03f8-a853-4ec5-a5d8-302e45aaaf13", path: ["id"], operator: Equal},
							{valueText: "823abeca-eef3-41c7-b587-7a6977b08003", path: ["id"], operator: Equal}
					]}
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			order    string
			expected []interface{}
		}{
			{
				name:  "location asc",
				order: "asc",
				expected: []interface{}{
					map[string]interface{}{"name": "Missing Island"},
					map[string]interface{}{"name": "Dusseldorf"},
				},
			},
			{
				name:  "location desc",
				order: "desc",
				expected: []interface{}{
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Missing Island"},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.order))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("sort with where with non-existent-uuid", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					sort: [{
						path: ["location"]
						order: asc
					}]
					where: {
						valueText: "non-existent-uuid", path: ["id"], operator: Equal
					}
				) {
					name
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		got := result.Get("Get", "City").AsSlice()
		assert.Empty(t, got)
	})

	t.Run("sort with nearText (with distance)", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					nearText: {
						concepts: ["Berlin"]
						distance: 0.6
					}
					%s
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "name asc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
				},
			},
			{
				name: "name desc",
				sort: []string{
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name: "population asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "population desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": nil},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("sort with nearText (with certainty)", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					nearText: {
						concepts: ["Berlin"]
						certainty: 0.7
					}
					%s
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "name asc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
				},
			},
			{
				name: "name desc",
				sort: []string{
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": nil},
				},
			},
			{
				name: "population asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": nil},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "population desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Dusseldorf"},
					map[string]interface{}{"name": "Rotterdam"},
					map[string]interface{}{"name": nil},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("sort with nearText and limit (with distance)", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					nearText: {
						concepts: ["Berlin"]
						distance: 0.6
					}
					%s
					limit: 2
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "name asc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "name desc",
				sort: []string{
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "population asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "population desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("sort with nearText and limit (with certainty)", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					nearText: {
						concepts: ["Berlin"]
						certainty: 0.7
					}
					%s
					limit: 2
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "name asc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "name desc",
				sort: []string{
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "population asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "population desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("sort with where and nearText and limit (with distance)", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearText: {
						concepts: ["Amsterdam"]
						distance: 0.6
					}
					%s
					limit: 2
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "name asc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "name desc",
				sort: []string{
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "population asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "population desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("sort with where and nearText and limit (with certainty)", func(t *testing.T) {
		query := `
		{
			Get {
				City(
					where: {
						valueBoolean: true,
						operator: Equal,
						path: ["isCapital"]
					}
					nearText: {
						concepts: ["Amsterdam"]
						certainty: 0.7
					}
					%s
					limit: 2
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name     string
			sort     []string
			expected []interface{}
		}{
			{
				name: "name asc",
				sort: []string{
					buildSort([]string{"name"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "name desc",
				sort: []string{
					buildSort([]string{"name"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
			{
				name: "population asc",
				sort: []string{
					buildSort([]string{"population"}, "asc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
					map[string]interface{}{"name": "Berlin"},
				},
			},
			{
				name: "population desc",
				sort: []string{
					buildSort([]string{"population"}, "desc"),
				},
				expected: []interface{}{
					map[string]interface{}{"name": "Berlin"},
					map[string]interface{}{"name": "Amsterdam"},
				},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, buildSortFilter(tt.sort)))
				got := result.Get("Get", "City").AsSlice()
				if !reflect.DeepEqual(got, tt.expected) {
					t.Errorf("sort objects got = %v, want %v", got, tt.expected)
				}
			})
		}
	})

	t.Run("broken sort clause", func(t *testing.T) {
		query := `
		{
			Get {
				%s(
					%s
				) {
					name
				}
			}
		}
		`
		tests := []struct {
			name        string
			className   string
			sort        []string
			expectedMsg string
		}{
			{
				name:      "empty path",
				className: "City",
				sort: []string{
					buildSort([]string{}, "asc"),
				},
				expectedMsg: "invalid 'sort' parameter: sort parameter at position 0: " +
					"path parameter cannot be empty",
			},
			{
				name:      "empty property in path",
				className: "City",
				sort: []string{
					buildSort([]string{""}, "asc"),
				},
				expectedMsg: "invalid 'sort' parameter: sort parameter at position 0: " +
					"no such prop with name '' found in class 'City' in the schema. " +
					"Check your schema files for which properties in this class are available",
			},
			{
				name:      "reference prop in path",
				className: "City",
				sort: []string{
					buildSort([]string{"ref", "prop"}, "asc"),
				},
				expectedMsg: "invalid 'sort' parameter: sort parameter at position 0: " +
					"sorting by reference not supported, path must have exactly one argument",
			},
			{
				name:      "non-existent class",
				className: "NonExistentClass",
				sort: []string{
					buildSort([]string{"property"}, "asc"),
				},
				expectedMsg: "Cannot query field \"NonExistentClass\" on type \"GetObjectsObj\".",
			},
			{
				name:      "non-existent property",
				className: "City",
				sort: []string{
					buildSort([]string{"nonexistentproperty"}, "asc"),
				},
				expectedMsg: "invalid 'sort' parameter: sort parameter at position 0: " +
					"no such prop with name 'nonexistentproperty' found in class 'City' in the schema. " +
					"Check your schema files for which properties in this class are available",
			},
			{
				name:      "reference property",
				className: "City",
				sort: []string{
					buildSort([]string{"inCountry"}, "asc"),
				},
				expectedMsg: "invalid 'sort' parameter: sort parameter at position 0: " +
					"sorting by reference not supported, " +
					"property \"inCountry\" is a ref prop to the class \"Country\"",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := graphqlhelper.ErrorGraphQL(t, helper.RootAuth, fmt.Sprintf(query, tt.className, buildSortFilter(tt.sort)))
				for _, gqlError := range result {
					assert.Equal(t, tt.expectedMsg, gqlError.Message)
				}
			})
		}
	})
}
