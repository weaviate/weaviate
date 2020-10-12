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

package classification

import (
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

func Test_ValidateUserInput(t *testing.T) {
	type testcase struct {
		name          string
		input         models.Classification
		expectedError error
	}

	// knn or general
	tests := []testcase{
		testcase{
			name: "missing class",
			input: models.Classification{
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: class must be set"),
		},

		testcase{
			name: "missing basedOnProperty (nil)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  nil,
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties must have at least one property"),
		},
		testcase{
			name: "missing basedOnProperty (len=0)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties must have at least one property"),
		},

		testcase{
			name: "more than one basedOnProperty",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description", "name"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: only a single property in basedOnProperties " +
				"supported at the moment, got [description name]"),
		},

		testcase{
			name: "basedOnProperty does not exist",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"doesNotExist"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties: property 'doesNotExist' does not exist"),
		},

		testcase{
			name: "basedOnProperty is not of type text",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"name"},
				ClassifyProperties: []string{"exactCategory"},
			},
			expectedError: fmt.Errorf("invalid classification: basedOnProperties: property 'name' must be of type 'text'"),
		},

		testcase{
			name: "missing classifyProperties (nil)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: nil,
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties must have at least one property"),
		},

		testcase{
			name: "missing classifyProperties (len=0)",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{},
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties must have at least one property"),
		},

		testcase{
			name: "classifyProperties does not exist",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"doesNotExist"},
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties: property 'doesNotExist' does not exist"),
		},

		testcase{
			name: "classifyProperties is not of reference type",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"name"},
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties: property 'name' must be of reference type (cref)"),
		},

		testcase{
			name:          "multiple missing fields (aborts early as we can't validate properties if class is not set)",
			input:         models.Classification{},
			expectedError: fmt.Errorf("invalid classification: class must be set"),
		},

		// specific for knn
		testcase{
			name: "targetWhere is set",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
				TargetWhere:        &models.WhereFilter{Operator: "Equal", Path: []string{"foo"}, ValueString: ptString("bar")},
				Type:               ptString("knn"),
			},
			expectedError: fmt.Errorf("invalid classification: type is 'knn', but 'targetWhere' filter is set, for 'knn' you cannot limit target data directly, instead limit training data through setting 'trainingSetWhere'"),
		},

		// specific for contextual
		testcase{
			name: "classifyProperty has more than one target class",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"anyCategory"},
				Type:               ptString("contextual"),
			},
			expectedError: fmt.Errorf("invalid classification: classifyProperties: property 'anyCategory' has more than one target class, classification of type 'contextual' requires exactly one target class"),
		},

		testcase{
			name: "type is contextual, but k is set",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
				Type:               ptString("contextual"),
				K:                  ptInt(7),
			},
			expectedError: fmt.Errorf("invalid classification: field 'k' can only be set for type 'knn', but got type 'contextual'"),
		},
		testcase{
			name: "trainingSetWhere is set",
			input: models.Classification{
				Class:              "Article",
				BasedOnProperties:  []string{"description"},
				ClassifyProperties: []string{"exactCategory"},
				TrainingSetWhere:   &models.WhereFilter{Operator: "Equal", Path: []string{"foo"}, ValueString: ptString("bar")},
				Type:               ptString("contextual"),
			},
			expectedError: fmt.Errorf("invalid classification: type is 'contextual', but 'trainingSetWhere' filter is set, for 'contextual' there is no training data, instead limit possible target data directly through setting 'targetWhere'"),
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

func ptInt(in int) *int32 {
	a := int32(in)
	return &a
}

func ptString(in string) *string {
	return &in
}
