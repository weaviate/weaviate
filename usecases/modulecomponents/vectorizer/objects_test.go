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
	"github.com/weaviate/weaviate/entities/models"
)

func TestVectorizingObjects(t *testing.T) {
	type testCase struct {
		name                string
		input               *models.Object
		expectedClientCall  string
		expectedOpenAIType  string
		expectedOpenAIModel string
		noindex             string
		excludedProperty    string // to simulate a schema where property names aren't vectorized
		excludedClass       string // to simulate a schema where class names aren't vectorized
		openAIType          string
		openAIModel         string
		openAIModelVersion  string
		lowerCase           bool
	}

	tests := []testCase{
		{
			name: "empty object",
			input: &models.Object{
				Class: "Car",
			},
			openAIType:          "text",
			openAIModel:         "ada",
			expectedOpenAIType:  "text",
			expectedOpenAIModel: "ada",
			expectedClientCall:  "car",
			lowerCase:           true,
		},
		{
			name: "object with one string prop",
			input: &models.Object{
				Class: "Car",
				Properties: map[string]interface{}{
					"brand": "Mercedes",
				},
			},
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
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
			lowerCase:          true,
			expectedClientCall: "super car brand of the car best brand review a very great car",
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
			lowerCase:          false,
			expectedClientCall: "Super Car brand Of The Car best brand review a very great car",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := New()

			cfg := &fakeClassConfig{
				classConfig: map[string]interface{}{
					"vectorizeClassName": test.excludedClass != "Car",
					"type":               test.openAIType,
					"model":              test.openAIModel,
					"modelVersion":       test.openAIModelVersion,
				},
				vectorizePropertyName: true,
				skippedProperty:       test.noindex,
				excludedProperty:      test.excludedProperty,
				lowerCase:             test.lowerCase,
			}
			text := v.Texts(context.Background(), test.input, cfg)
			assert.Equal(t, test.expectedClientCall, text)
		})
	}
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
	lowerCase             bool
}

func (f fakeClassConfig) LowerCaseInput() bool {
	return f.lowerCase
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	if propName == f.skippedProperty {
		return map[string]interface{}{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]interface{}{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]interface{}{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertyIndexed(property string) bool {
	return f.skippedProperty != property
}

func (f fakeClassConfig) VectorizePropertyName(property string) bool {
	if f.excludedProperty == property {
		return false
	}
	return f.vectorizePropertyName
}

func (f fakeClassConfig) VectorizeClassName() bool {
	vectorizeClassName, ok := f.classConfig["vectorizeClassName"]
	if !ok {
		return false
	}
	return vectorizeClassName.(bool)
}

func (f fakeClassConfig) Properties() []string {
	return nil
}
