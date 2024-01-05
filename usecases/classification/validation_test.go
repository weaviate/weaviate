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

package classification

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_ValidateUserInput(t *testing.T) {
	type testcase struct {
		name          string
		input         models.Classification
		expectedError error
	}

	// knn or general
	tests := []testcase{
		{
			name: "missing class",
			input: models.Classification{
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: class must be set"),
		},

		{
			name: "missing basedOnProperty (nil)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  nil,
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties must have at least one property"),
		},
		{
			name: "missing basedOnProperty (len=0)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties must have at least one property"),
		},

		{
			name: "more than one basedOnProperty",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description", "name"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: only a single property in basedOnProperties " +
				"supported at the moment, got [description name]"),
		},

		{
			name: "basedOnProperty does not exist",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"doesNotExist"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties: property 'doesNotExist' does not exist"),
		},

		{
			name: "basedOnProperty is not of type text",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"words"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties: property 'words' must be of type 'text'"),
		},

		{
			name: "missing classifyProperties (nil)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: nil,
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties must have at least one property"),
		},

		{
			name: "missing classifyProperties (len=0)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{},
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties must have at least one property"),
		},

		{
			name: "classifyProperties does not exist",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"doesNotExist"},
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties: property 'doesNotExist' does not exist"),
		},

		{
			name: "classifyProperties is not of reference type",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"name"},
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties: property 'name' must be of reference type (cref)"),
		},

		{
			name:          "multiple missing fields (aborts early as we can't validate properties if class is not set)",
			input:         models.Classification{},
			expectedError: fmt.Errorf("invalid classification: class must be set"),
		},

		// specific for knn
		{
			name: "targetWhere is set",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
				Filters: &models.ClassificationFilters{
					TargetWhere: &models.WhereFilter{Operator: "Equal", Path: []string{"foo"}, ValueText: ptString("bar")},
				},
				Type: "knn",
			},
			expectedError: fmt.Errorf("invalid classification: type is 'knn', but 'targetWhere' filter is set, for 'knn' you cannot limit target data directly, instead limit training data through setting 'trainingSetWhere'"),
		},

		// specific for text2vec-contextionary-contextual
		{
			name: "classifyProperty has more than one target class",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"anyCategory"},
				Type:               "text2vec-contextionary-contextual",
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties: property 'anyCategory' has more than one target class, classification of type 'text2vec-contextionary-contextual' requires exactly one target class"),
		},

		{
			name: "trainingSetWhere is set",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
				Filters: &models.ClassificationFilters{
					TrainingSetWhere: &models.WhereFilter{Operator: "Equal", Path: []string{"foo"}, ValueText: ptString("bar")},
				},
				Type: "text2vec-contextionary-contextual",
			},
			expectedError: fmt.Errorf("invalid classification: type is 'text2vec-contextionary-contextual', but 'trainingSetWhere' filter is set, for 'text2vec-contextionary-contextual' there is no training data, instead limit possible target data directly through setting 'targetWhere'"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := NewValidator(&fakeSchemaGetter{testSchema()}, test.input)
			err := validator.Do()
			assert.Equal(t, test.expectedError, err)
		})
	}
}

func ptString(in string) *string {
	return &in
}
