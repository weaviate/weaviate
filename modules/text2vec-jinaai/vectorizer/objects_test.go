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
		name                string
		input               *models.Object
		expectedClientCall  string
		expectedJinaAIModel string
		noindex             string
		excludedProperty    string // to simulate a schema where property names aren't vectorized
		excludedClass       string // to simulate a schema where class names aren't vectorized
		jinaAIModel         string
	}

	tests := []testCase{
		{
			name: "empty object",
			input: &models.Object{
				Class: "Car",
			},
			jinaAIModel:         "jina-embedding-v2",
			expectedJinaAIModel: "jina-embedding-v2",
			expectedClientCall:  "car",
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
					"reviews": []string{
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
					"reviews": []string{
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
				jinaAIModel:           test.jinaAIModel,
				vectorizePropertyName: true,
			}
			vector, _, err := v.Object(context.Background(), test.input, ic)

			require.Nil(t, err)
			assert.Equal(t, []float32{0, 1, 2, 3}, vector)
			assert.Equal(t, []string{test.expectedClientCall}, client.lastInput)
			assert.Equal(t, client.lastConfig.Model, test.expectedJinaAIModel)
		})
	}
}

func TestClassSettings(t *testing.T) {
	type testCase struct {
		expectedBaseURL string
		cfg             moduletools.ClassConfig
	}
	tests := []testCase{
		{
			cfg: fakeClassConfig{
				classConfig: make(map[string]interface{}),
			},
			expectedBaseURL: DefaultBaseURL,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL": "https://proxy.weaviate.dev",
				},
			},
			expectedBaseURL: "https://proxy.weaviate.dev",
		},
	}

	for _, tt := range tests {
		ic := NewClassSettings(tt.cfg)
		assert.Equal(t, tt.expectedBaseURL, ic.BaseURL())
	}
}
