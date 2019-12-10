//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package vectorizer

import "context"

// TODO: gh-975 reenable once decided

// func TestVectorizingThings(t *testing.T) {
// 	type testCase struct {
// 		name               string
// 		input              *models.Thing
// 		expectedClientCall []string
// 		noindex            string
// 	}

// 	tests := []testCase{
// 		testCase{
// 			name: "empty thing",
// 			input: &models.Thing{
// 				Class: "Car",
// 			},
// 			expectedClientCall: []string{"car"},
// 		},
// 		testCase{
// 			name: "thing with one string prop",
// 			input: &models.Thing{
// 				Class: "Car",
// 				Schema: map[string]interface{}{
// 					"brand": "Mercedes",
// 				},
// 			},
// 			expectedClientCall: []string{"car brand mercedes"},
// 		},

// 		testCase{
// 			name: "thing with one non-string prop",
// 			input: &models.Thing{
// 				Class: "Car",
// 				Schema: map[string]interface{}{
// 					"power": 300,
// 				},
// 			},
// 			expectedClientCall: []string{"car"},
// 		},

// 		testCase{
// 			name: "thing with a mix of props",
// 			input: &models.Thing{
// 				Class: "Car",
// 				Schema: map[string]interface{}{
// 					"brand":  "best brand",
// 					"power":  300,
// 					"review": "a very great car",
// 				},
// 			},
// 			expectedClientCall: []string{"car brand best brand review a very great car"},
// 		},
// 		testCase{
// 			name:    "with a noindexed property",
// 			noindex: "review",
// 			input: &models.Thing{
// 				Class: "Car",
// 				Schema: map[string]interface{}{
// 					"brand":  "best brand",
// 					"power":  300,
// 					"review": "a very great car",
// 				},
// 			},
// 			expectedClientCall: []string{"car brand best brand"},
// 		},

// 		testCase{
// 			name: "with compound class and prop names",
// 			input: &models.Thing{
// 				Class: "SuperCar",
// 				Schema: map[string]interface{}{
// 					"brandOfTheCar": "best brand",
// 					"power":         300,
// 					"review":        "a very great car",
// 				},
// 			},
// 			expectedClientCall: []string{"super car brand of the car best brand review a very great car"},
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			client := &fakeClient{}
// 			indexer := &propertyIndexer{test.noindex}

// 			v := New(client, indexer)

// 			res, err := v.Thing(context.Background(), test.input)

// 			require.Nil(t, err)
// 			assert.Equal(t, []float32{0, 1, 2, 3}, res)
// 			expected := strings.Split(test.expectedClientCall[0], " ")
// 			actual := strings.Split(client.lastInput[0], " ")
// 			assert.ElementsMatch(t, expected, actual)
// 		})

// 	}
// }

type propertyIndexer struct {
	noIndex string
}

func (p *propertyIndexer) Indexed(className, property string) bool {
	if property == p.noIndex {
		return false
	}

	return true
}

// func TestVectorizingActions(t *testing.T) {
// 	type testCase struct {
// 		name               string
// 		input              *models.Action
// 		expectedClientCall []string
// 		noindex            string
// 	}

// 	tests := []testCase{
// 		testCase{
// 			name: "empty thing",
// 			input: &models.Action{
// 				Class: "Flight",
// 			},
// 			expectedClientCall: []string{"flight"},
// 		},
// 		testCase{
// 			name: "thing with one string prop",
// 			input: &models.Action{
// 				Class: "Flight",
// 				Schema: map[string]interface{}{
// 					"brand": "Mercedes",
// 				},
// 			},
// 			expectedClientCall: []string{"flight brand mercedes"},
// 		},

// 		testCase{
// 			name: "thing with one non-string prop",
// 			input: &models.Action{
// 				Class: "Flight",
// 				Schema: map[string]interface{}{
// 					"length": 300,
// 				},
// 			},
// 			expectedClientCall: []string{"flight"},
// 		},

// 		testCase{
// 			name: "thing with a mix of props",
// 			input: &models.Action{
// 				Class: "Flight",
// 				Schema: map[string]interface{}{
// 					"brand":  "best brand",
// 					"length": 300,
// 					"review": "a very great flight",
// 				},
// 			},
// 			expectedClientCall: []string{"flight brand best brand review a very great flight"},
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			client := &fakeClient{}
// 			indexer := &propertyIndexer{test.noindex}
// 			v := New(client, indexer)

// 			res, err := v.Action(context.Background(), test.input)

// 			require.Nil(t, err)
// 			assert.Equal(t, []float32{0, 1, 2, 3}, res)
// 			expected := strings.Split(test.expectedClientCall[0], " ")
// 			actual := strings.Split(client.lastInput[0], " ")
// 			assert.ElementsMatch(t, expected, actual)

// 		})

// 	}
// }

// func TestVectorizingSearchTerms(t *testing.T) {
// 	type testCase struct {
// 		name               string
// 		input              []string
// 		expectedClientCall []string
// 		noindex            string
// 	}

// 	tests := []testCase{
// 		testCase{
// 			name:               "single word",
// 			input:              []string{"car"},
// 			expectedClientCall: []string{"car"},
// 		},
// 		testCase{
// 			name:               "multiple entries with multiple words",
// 			input:              []string{"car", "car brand"},
// 			expectedClientCall: []string{"car", "car brand"},
// 		},
// 		testCase{
// 			name:               "multiple entries with upper casing",
// 			input:              []string{"Car", "Car Brand"},
// 			expectedClientCall: []string{"car", "car brand"},
// 		},
// 		testCase{
// 			name:               "with camel cased words",
// 			input:              []string{"Car", "CarBrand"},
// 			expectedClientCall: []string{"car", "car brand"},
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			client := &fakeClient{}
// 			indexer := &propertyIndexer{test.noindex}
// 			v := New(client, indexer)

// 			res, err := v.Corpi(context.Background(), test.input)

// 			require.Nil(t, err)
// 			assert.Equal(t, []float32{0, 1, 2, 3}, res)
// 			assert.ElementsMatch(t, test.expectedClientCall, client.lastInput)
// 		})
// 	}
// }

type fakeClient struct {
	lastInput []string
}

func (c *fakeClient) VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error) {
	c.lastInput = corpi
	return []float32{0, 1, 2, 3}, nil
}
