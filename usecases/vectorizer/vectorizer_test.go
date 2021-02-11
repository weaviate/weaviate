//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package vectorizer

import (
	"context"
	"strings"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVectorizingObjects(t *testing.T) {
	type testCase struct {
		name               string
		input              *models.Object
		expectedClientCall []string
		noindex            string
		excludedProperty   string // to simulate a schema where property names aren't vectorized
		excludedClass      string // to simulate a schema where class names aren't vectorized
	}

	tests := []testCase{
		testCase{
			name: "empty object",
			input: &models.Object{
				Class: "Car",
			},
			expectedClientCall: []string{"car"},
		},
		testCase{
			name: "object with one string prop",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand": "Mercedes",
				},
			},
			expectedClientCall: []string{"car brand mercedes"},
		},

		testCase{
			name: "object with one non-string prop",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"power": 300,
				},
			},
			expectedClientCall: []string{"car"},
		},

		testCase{
			name: "object with a mix of props",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand":  "best brand",
					"power":  300,
					"review": "a very great car",
				},
			},
			expectedClientCall: []string{"car brand best brand review a very great car"},
		},
		testCase{
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
			expectedClientCall: []string{"car brand best brand"},
		},

		testCase{
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
			expectedClientCall: []string{"brand best brand review a very great car"},
		},

		testCase{
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
			expectedClientCall: []string{"car brand best brand a very great car"},
		},

		testCase{
			name:             "with no schema labels vectorized",
			excludedProperty: "review",
			excludedClass:    "Car",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"review": "a very great car",
				},
			},
			expectedClientCall: []string{"a very great car"},
		},

		testCase{
			name: "with compound class and prop names",
			input: &models.Object{
				Class: "SuperCar",
				Properties: map[string]interface{}{
					"brandOfTheCar": "best brand",
					"power":         300,
					"review":        "a very great car",
				},
			},
			expectedClientCall: []string{"super car brand of the car best brand review a very great car"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			indexer := &propertyIndexer{test.noindex, test.excludedClass, test.excludedProperty}

			v := New(client, indexer)

			err := v.Object(context.Background(), test.input)

			require.Nil(t, err)
			assert.Equal(t, models.C11yVector{0, 1, 2, 3}, test.input.Vector)
			expected := strings.Split(test.expectedClientCall[0], " ")
			actual := strings.Split(client.lastInput[0], " ")
			assert.ElementsMatch(t, expected, actual)
		})
	}
}

type propertyIndexer struct {
	noIndex          string
	excludedClass    string
	excludedProperty string
}

func (p *propertyIndexer) IndexedInverted(className, property string) bool {
	panic("IndexedInverted should not matter to the vectorizer")
}

func (p *propertyIndexer) IndexedContextionary(className, property string) bool {
	return property != p.noIndex
}

func (p *propertyIndexer) VectorizeClassName(class string) bool {
	return p.excludedClass != class
}

func (p *propertyIndexer) VectorizePropertyName(class, prop string) bool {
	return p.excludedProperty != prop
}

func TestVectorizingActions(t *testing.T) {
	type testCase struct {
		name               string
		input              *models.Object
		expectedClientCall []string
		noindex            string
		excludedProperty   string // to simulate a schema where property names aren't vectorized
		excludedClass      string // to simulate a schema where class names aren't vectorized
	}

	tests := []testCase{
		testCase{
			name: "empty object",
			input: &models.Object{
				Class: "Flight",
			},
			expectedClientCall: []string{"flight"},
		},
		testCase{
			name: "object with one string prop",
			input: &models.Object{
				Class: "Flight",
				Properties: map[string]interface{}{
					"brand": "Mercedes",
				},
			},
			expectedClientCall: []string{"flight brand mercedes"},
		},

		testCase{
			name: "object with one non-string prop",
			input: &models.Object{
				Class: "Flight",
				Properties: map[string]interface{}{
					"length": 300,
				},
			},
			expectedClientCall: []string{"flight"},
		},

		testCase{
			name: "object with a mix of props",
			input: &models.Object{
				Class: "Flight",
				Properties: map[string]interface{}{
					"brand":  "best brand",
					"length": 300,
					"review": "a very great flight",
				},
			},
			expectedClientCall: []string{"flight brand best brand review a very great flight"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			indexer := &propertyIndexer{test.noindex, test.excludedClass, test.excludedProperty}
			v := New(client, indexer)

			err := v.Object(context.Background(), test.input)

			require.Nil(t, err)
			assert.Equal(t, models.C11yVector{0, 1, 2, 3}, test.input.Vector)
			expected := strings.Split(test.expectedClientCall[0], " ")
			actual := strings.Split(client.lastInput[0], " ")
			assert.ElementsMatch(t, expected, actual)
		})
	}
}

func TestVectorizingSearchTerms(t *testing.T) {
	type testCase struct {
		name               string
		input              []string
		expectedClientCall []string
		noindex            string
	}

	tests := []testCase{
		testCase{
			name:               "single word",
			input:              []string{"car"},
			expectedClientCall: []string{"car"},
		},
		testCase{
			name:               "multiple entries with multiple words",
			input:              []string{"car", "car brand"},
			expectedClientCall: []string{"car", "car brand"},
		},
		testCase{
			name:               "multiple entries with upper casing",
			input:              []string{"Car", "Car Brand"},
			expectedClientCall: []string{"car", "car brand"},
		},
		testCase{
			name:               "with camel cased words",
			input:              []string{"Car", "CarBrand"},
			expectedClientCall: []string{"car", "car brand"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			indexer := &propertyIndexer{test.noindex, "", ""}
			v := New(client, indexer)

			res, err := v.Corpi(context.Background(), test.input)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, res)
			assert.ElementsMatch(t, test.expectedClientCall, client.lastInput)
		})
	}
}
