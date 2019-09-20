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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := NewValidator(&fakeSchemaGetter{testSchema()}, test.input)
			err := validator.Do()
			assert.Equal(t, test.expectedError, err)
		})
	}
}
