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

package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractVersionAndCompare(t *testing.T) {
	type test struct {
		input           string
		requiredMinimum string
		expectedIsMet   bool
		expectedErr     error
	}

	tests := []test{
		{
			input:           "notavalidversiontag",
			requiredMinimum: "1.2.3",
			expectedIsMet:   false,
			expectedErr:     fmt.Errorf("unexpected input version tag: notavalidversiontag"),
		},
		{
			input:           "abc-v0.1.2",
			requiredMinimum: "invalidrequired",
			expectedIsMet:   false,
			expectedErr:     fmt.Errorf("unexpected threshold version tag: invalidrequired"),
		},

		// valid matches

		// exact match
		{
			input:           "abc-v0.1.2",
			requiredMinimum: "0.1.2",
			expectedIsMet:   true,
			expectedErr:     nil,
		},

		// every digit bigger
		{
			input:           "abc-v1.2.3",
			requiredMinimum: "0.1.2",
			expectedIsMet:   true,
			expectedErr:     nil,
		},

		// only major bigger
		{
			input:           "abc-v1.0.0",
			requiredMinimum: "0.1.2",
			expectedIsMet:   true,
			expectedErr:     nil,
		},

		// only minor bigger
		{
			input:           "abc-v0.2.0",
			requiredMinimum: "0.1.2",
			expectedIsMet:   true,
			expectedErr:     nil,
		},

		// only patch bigger
		{
			input:           "abc-v0.1.3",
			requiredMinimum: "0.1.2",
			expectedIsMet:   true,
			expectedErr:     nil,
		},

		// invalid requirements

		// only patch smaller
		{
			input:           "abc-v0.1.1",
			requiredMinimum: "0.1.2",
			expectedIsMet:   false,
			expectedErr:     nil,
		},

		// only minor smaller
		{
			input:           "abc-v0.0.9",
			requiredMinimum: "0.1.2",
			expectedIsMet:   false,
			expectedErr:     nil,
		},

		// only major smaller
		{
			input:           "abc-v0.9.9",
			requiredMinimum: "1.1.2",
			expectedIsMet:   false,
			expectedErr:     nil,
		},
	}

	for _, test := range tests {
		ok, err := extractVersionAndCompare(test.input, test.requiredMinimum)
		assert.Equal(t, test.expectedIsMet, ok)
		assert.Equal(t, test.expectedErr, err)
	}
}
