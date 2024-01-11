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

package vectorizer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
)

// These are mostly copy/pasted (with minimal additions) from the
// text2vec-contextionary module
func TestVectorizingObjects(t *testing.T) {
	type testCase struct {
		name                    string
		input                   *models.Object
		expectedClientCall      string
		expectedPoolingStrategy string
		noindex                 string
		excludedProperty        string // to simulate a schema where property names aren't vectorized
		excludedClass           string // to simulate a schema where class names aren't vectorized
		poolingStrategy         string
	}

	tests := []testCase{
		{
			name: "empty object",
			input: &models.Object{
				Class: "Car",
			},
			poolingStrategy:         "cls",
			expectedPoolingStrategy: "cls",
			expectedClientCall:      "car",
		},
		{
			name: "object with one string prop",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand": "Mercedes",
				},
			},
			expectedClientCall: "car brand mercedes",
		},

		{
			name: "object with one non-string prop",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"power": 300,
				},
			},
			expectedClientCall: "car",
		},

		{
			name: "object with a mix of props",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":  "best brand",
					"power":  300,
					"review": "a very great car",
				},
			},
			expectedClientCall: "car brand best brand review a very great car",
		},
		{
			name:    "with a noindexed property",
			noindex: "review",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":  "best brand",
					"power":  300,
					"review": "a very great car",
				},
			},
			expectedClientCall: "car brand best brand",
		},

		{
			name:          "with the class name not vectorized",
			excludedClass: "Car",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":  "best brand",
					"power":  300,
					"review": "a very great car",
				},
			},
			expectedClientCall: "brand best brand review a very great car",
		},

		{
			name:             "with a property name not vectorized",
			excludedProperty: "review",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":  "best brand",
					"power":  300,
					"review": "a very great car",
				},
			},
			expectedClientCall: "car brand best brand a very great car",
		},

		{
			name:             "with no schema labels vectorized",
			excludedProperty: "review",
			excludedClass:    "Car",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"review": "a very great car",
				},
			},
			expectedClientCall: "a very great car",
		},

		{
			name:             "with string/text arrays without propname or classname",
			excludedProperty: "reviews",
			excludedClass:    "Car",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			expectedClientCall: "a very great car you should consider buying one",
		},

		{
			name: "with string/text arrays with propname and classname",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			expectedClientCall: "car reviews a very great car reviews you should consider buying one",
		},

		{
			name: "with compound class and prop names",
			input: &models.Object{
				Class: "SuperCar",
				Properties: map[string]interface{}{
					"brandOfTheCar": "best brand",
					"power":         300,
					"review":        "a very great car",
				},
			},
			expectedClientCall: "super car brand of the car best brand review a very great car",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}

			v := New(client)

			ic := &fakeClassConfig{
				excludedProperty:      test.excludedProperty,
				skippedProperty:       test.noindex,
				vectorizeClassName:    test.excludedClass != "Car",
				poolingStrategy:       test.poolingStrategy,
				vectorizePropertyName: true,
			}
			err := v.Object(context.Background(), test.input, nil, ic)

			require.Nil(t, err)
			assert.Equal(t, models.C11yVector{0, 1, 2, 3}, test.input.Vector)
			expected := strings.Split(test.expectedClientCall, " ")
			actual := strings.Split(client.lastInput, " ")
			assert.Equal(t, expected, actual)
			assert.Equal(t, client.lastConfig.PoolingStrategy, test.expectedPoolingStrategy)
		})
	}
}

func TestVectorizingObjectsWithDiff(t *testing.T) {
	type testCase struct {
		name              string
		input             *models.Object
		skipped           string
		diff              *moduletools.ObjectDiff
		expectedVectorize bool
	}

	tests := []testCase{
		{
			name: "no diff",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":       "best brand",
					"power":       300,
					"description": "a very great car",
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			diff:              nil,
			expectedVectorize: true,
		},
		{
			name: "diff all props unchanged",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":       "best brand",
					"power":       300,
					"description": "a very great car",
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("brand", "best brand", "best brand").
				WithProp("power", 300, 300).
				WithProp("description", "a very great car", "a very great car").
				WithProp("reviews", []interface{}{
					"a very great car",
					"you should consider buying one",
				}, []interface{}{
					"a very great car",
					"you should consider buying one",
				}),
			expectedVectorize: false,
		},
		{
			name: "diff one vectorizable prop changed (1)",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":       "best brand",
					"power":       300,
					"description": "a very great car",
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("brand", "old best brand", "best brand"),
			expectedVectorize: true,
		},
		{
			name: "diff one vectorizable prop changed (2)",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":       "best brand",
					"power":       300,
					"description": "a very great car",
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("description", "old a very great car", "a very great car"),
			expectedVectorize: true,
		},
		{
			name: "diff one vectorizable prop changed (3)",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":       "best brand",
					"power":       300,
					"description": "a very great car",
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("reviews", []interface{}{
					"old a very great car",
					"you should consider buying one",
				}, []interface{}{
					"a very great car",
					"you should consider buying one",
				}),
			expectedVectorize: true,
		},
		{
			name:    "all non-vectorizable props changed",
			skipped: "description",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":       "best brand",
					"power":       300,
					"description": "a very great car",
					"reviews": []interface{}{
						"a very great car",
						"you should consider buying one",
					},
				},
			},
			diff: newObjectDiffWithVector().
				WithProp("power", 123, 300).
				WithProp("description", "old a very great car", "a very great car"),
			expectedVectorize: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ic := &fakeClassConfig{
				skippedProperty: test.skipped,
			}

			client := &fakeClient{}
			v := New(client)

			err := v.Object(context.Background(), test.input, test.diff, ic)

			require.Nil(t, err)
			if test.expectedVectorize {
				assert.Equal(t, models.C11yVector{0, 1, 2, 3}, test.input.Vector)
				assert.NotEmpty(t, client.lastInput)
			} else {
				assert.Equal(t, models.C11yVector{0, 0, 0, 0}, test.input.Vector)
				assert.Empty(t, client.lastInput)
			}
		})
	}
}

func newObjectDiffWithVector() *moduletools.ObjectDiff {
	return moduletools.NewObjectDiff([]float32{0, 0, 0, 0})
}
