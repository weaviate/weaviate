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

package traverser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

type schemaSearchTest struct {
	name          string
	searchParams  SearchParams
	expectedError error
}

type schemaSearchTests []schemaSearchTest

func Test__SchemaSearch_Validation(t *testing.T) {
	tests := schemaSearchTests{
		{
			name: "valid params",
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "foo",
				Certainty:  1.0,
			},
			expectedError: nil,
		},
		{
			name: "missing search name",
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "",
				Certainty:  0.0,
			},
			expectedError: errors.New("Name cannot be empty"),
		},
		{
			name: "certainty too low",
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "bestName",
				Certainty:  -4,
			},
			expectedError: errors.New("invalid Certainty: must be between 0 and 1, but got '-4.000000'"),
		},
		{
			name: "certainty too high",
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "bestName",
				Certainty:  4,
			},
			expectedError: errors.New("invalid Certainty: must be between 0 and 1, but got '4.000000'"),
		},
		{
			name: "invalid search type",
			searchParams: SearchParams{
				SearchType: SearchType("invalid"),
				Name:       "bestName",
				Certainty:  0.5,
			},
			expectedError: errors.New("SearchType must be SearchTypeClass or SearchTypeProperty, but got 'invalid'"),
		},
		{
			name: "missing kind on class search",
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "bestName",
				Certainty:  0.5,
			},
			expectedError: nil,
		},
		{
			name: "valid keywords",
			searchParams: SearchParams{
				SearchType: SearchTypeClass,
				Name:       "foo",
				Certainty:  1.0,
			},
			expectedError: nil,
		},
	}

	tests.AssertValidation(t)
}

func (s schemaSearchTests) AssertValidation(t *testing.T) {
	for _, test := range s {
		t.Run(test.name, func(t *testing.T) {
			err := test.searchParams.Validate()

			// assert error
			assert.Equal(t, test.expectedError, err, "should match the expected error")
		})
	}
}
