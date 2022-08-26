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

package traverser

import (
	"context"
	"errors"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	testLogger "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_Explorer_GetClass_WithSort(t *testing.T) {
	type testData struct {
		name          string
		params        GetParams
		expectedError error
	}

	oneSortFilter := []testData{
		{
			name: "invalid order parameter",
			params: GetParams{
				ClassName: "ClassOne",
				Sort:      []filters.Sort{{Path: nil, Order: "asce"}},
			},
			expectedError: errors.New(`invalid 'sort' filter: sort parameter at position 0: ` +
				`invalid order parameter, possible values are: ["asc", "desc"] not: "asce"`),
		},
		{
			name: "empty path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort:      []filters.Sort{{Path: nil, Order: "asc"}},
			},
			expectedError: errors.New("invalid 'sort' filter: sort parameter at position 0: " +
				"path parameter cannot be empty"),
		},
		{
			name: "non-existent class",
			params: GetParams{
				ClassName: "NonExistentClass",
				Sort:      []filters.Sort{{Path: []string{"property"}, Order: "asc"}},
			},
			expectedError: errors.New("invalid 'sort' filter: sort parameter at position 0: " +
				"class \"NonExistentClass\" does not exist in schema"),
		},
		{
			name: "non-existent property in class",
			params: GetParams{
				ClassName: "ClassOne",
				Sort:      []filters.Sort{{Path: []string{"nonexistentproperty"}, Order: "asc"}},
			},
			expectedError: errors.New("invalid 'sort' filter: sort parameter at position 0: " +
				"no such prop with name 'nonexistentproperty' found in class 'ClassOne' in the schema. " +
				"Check your schema files for which properties in this class are available"),
		},
		{
			name: "reference property in class",
			params: GetParams{
				ClassName: "ClassOne",
				Sort:      []filters.Sort{{Path: []string{"ref_prop"}, Order: "asc"}},
			},
			expectedError: errors.New("invalid 'sort' filter: sort parameter at position 0: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\""),
		},
		{
			name: "reference property path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort:      []filters.Sort{{Path: []string{"ref", "prop"}, Order: "asc"}},
			},
			expectedError: errors.New("invalid 'sort' filter: sort parameter at position 0: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument"),
		},
		{
			name: "invalid order parameter",
			params: GetParams{
				ClassName: "ClassOne",
				Sort:      []filters.Sort{{Path: nil, Order: "asce"}},
			},
			expectedError: errors.New(`invalid 'sort' filter: sort parameter at position 0: ` +
				`invalid order parameter, possible values are: ["asc", "desc"] not: "asce"`),
		},
	}

	twoSortFilters := []testData{
		{
			name: "invalid order parameter",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: nil, Order: "asce"},
					{Path: nil, Order: "desce"},
				},
			},
			expectedError: errors.New(`invalid 'sort' filter: ` +
				`sort parameter at position 0: ` +
				`invalid order parameter, possible values are: ["asc", "desc"] not: "asce", ` +
				`sort parameter at position 1: ` +
				`invalid order parameter, possible values are: ["asc", "desc"] not: "desce"`),
		},
		{
			name: "empty path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: nil, Order: "asc"},
					{Path: []string{}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: path parameter cannot be empty, " +
				"sort parameter at position 1: path parameter cannot be empty"),
		},
		{
			name: "non-existent class",
			params: GetParams{
				ClassName: "NonExistentClass",
				Sort: []filters.Sort{
					{Path: []string{"property"}, Order: "asc"},
					{Path: []string{"property"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: " +
				"class \"NonExistentClass\" does not exist in schema, " +
				"sort parameter at position 1: " +
				"class \"NonExistentClass\" does not exist in schema"),
		},
		{
			name: "non-existent property in class",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"nonexistentproperty1"}, Order: "asc"},
					{Path: []string{"nonexistentproperty2"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: " +
				"no such prop with name 'nonexistentproperty1' found in class 'ClassOne' in the schema. " +
				"Check your schema files for which properties in this class are available, " +
				"sort parameter at position 1: " +
				"no such prop with name 'nonexistentproperty2' found in class 'ClassOne' in the schema. " +
				"Check your schema files for which properties in this class are available"),
		},
		{
			name: "reference property in class",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"ref_prop"}, Order: "asc"},
					{Path: []string{"ref_prop"}, Order: "desc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\", " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\""),
		},
		{
			name: "reference property path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"ref", "prop"}, Order: "asc"},
					{Path: []string{"ref", "prop"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument, " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument"),
		},
		{
			name: "reference properties path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"ref_prop"}, Order: "asc"},
					{Path: []string{"ref", "prop"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\", " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument"),
		},
		{
			name: "reference properties path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"ref_prop"}, Order: "asc"},
					{Path: []string{"ref", "prop"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 0: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\", " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument"),
		},
	}

	oneOfTwoSortFilters := []testData{
		{
			name: "invalid order parameter",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
					{Path: nil, Order: "desce"},
				},
			},
			expectedError: errors.New(`invalid 'sort' filter: ` +
				`sort parameter at position 1: ` +
				`invalid order parameter, possible values are: ["asc", "desc"] not: "desce"`),
		},
		{
			name: "empty path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
					{Path: []string{}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 1: path parameter cannot be empty"),
		},
		{
			name: "non-existent property in class",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
					{Path: []string{"nonexistentproperty2"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 1: " +
				"no such prop with name 'nonexistentproperty2' found in class 'ClassOne' in the schema. " +
				"Check your schema files for which properties in this class are available"),
		},
		{
			name: "reference property in class",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
					{Path: []string{"ref_prop"}, Order: "desc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\""),
		},
		{
			name: "reference property path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
					{Path: []string{"ref", "prop"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument"),
		},
		{
			name: "reference properties path",
			params: GetParams{
				ClassName: "ClassOne",
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
					{Path: []string{"ref_prop"}, Order: "asc"},
					{Path: []string{"ref", "prop"}, Order: "asc"},
				},
			},
			expectedError: errors.New("invalid 'sort' filter: " +
				"sort parameter at position 1: " +
				"sorting by reference not supported, " +
				"property \"ref_prop\" is a ref prop to the class \"ClassTwo\", " +
				"sort parameter at position 2: " +
				"sorting by reference not supported, " +
				"path must have exactly one argument"),
		},
	}

	properSortFilters := []testData{
		{
			name: "sort by string_prop",
			params: GetParams{
				ClassName: "ClassOne",
				NearVector: &searchparams.NearVector{
					Vector: []float32{0.8, 0.2, 0.7},
				},
				Sort: []filters.Sort{
					{Path: []string{"string_prop"}, Order: "asc"},
				},
			},
		},
	}

	testCases := []struct {
		name     string
		testData []testData
	}{
		{
			name:     "one sort filter broken",
			testData: oneSortFilter,
		},
		{
			name:     "two sort filters broken",
			testData: twoSortFilters,
		},
		{
			name:     "one of two sort filters broken",
			testData: oneOfTwoSortFilters,
		},
		{
			name:     "proper sort filters",
			testData: properSortFilters,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, td := range tc.testData {
				t.Run(td.name, func(t *testing.T) {
					params := td.params
					searchResults := []search.Result{
						{
							ID: "id1",
							Schema: map[string]interface{}{
								"name": "Foo",
							},
						},
					}

					search := &fakeVectorSearcher{}
					sg := &fakeSchemaGetter{
						schema: schemaForFiltersValidation(),
					}
					log, _ := testLogger.NewNullLogger()
					explorer := NewExplorer(search, log, getFakeModulesProvider(), nil)
					explorer.SetSchemaGetter(sg)

					if td.expectedError == nil {
						search.
							On("VectorClassSearch", mock.Anything).
							Return(searchResults, nil)
						res, err := explorer.GetClass(context.Background(), params)
						assert.Nil(t, err)
						search.AssertExpectations(t)
						require.Len(t, res, 1)
					} else {
						_, err := explorer.GetClass(context.Background(), params)
						require.NotNil(t, err)
						assert.Equal(t, err.Error(), td.expectedError.Error())
					}
				})
			}
		})
	}
}
