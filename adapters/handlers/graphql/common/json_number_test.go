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

package common

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

type testCase struct {
	input          interface{}
	expectedOutput float64
}

func TestJSONNumberResolver(t *testing.T) {
	tests := []testCase{
		{
			input:          json.Number("10"),
			expectedOutput: 10.0,
		},
		{
			input:          int(10),
			expectedOutput: 10.0,
		},
		{
			input:          int64(10),
			expectedOutput: 10.0,
		},
		{
			input:          float64(10),
			expectedOutput: 10.0,
		},
	}

	for _, test := range tests {
		name := fmt.Sprintf("%#v -> %#v", test.input, test.expectedOutput)
		t.Run(name, func(t *testing.T) {
			result, err := JSONNumberResolver(resolveParams(test.input))
			assert.Nil(t, err, "should not error")
			assert.Equal(t, test.expectedOutput, result)
		})
	}
}

func resolveParams(input interface{}) graphql.ResolveParams {
	return graphql.ResolveParams{
		Source: map[string]interface{}{
			"myField": input,
		},
		Info: graphql.ResolveInfo{FieldName: "myField"},
	}
}

func TestNumberFieldNotPresent(t *testing.T) {
	// shouldn't return anything, but also not error. This can otherwise lead to
	// odd behavior when no entries are present, yet we asked for int props and
	// type, see https://github.com/weaviate/weaviate/issues/775
	params := graphql.ResolveParams{
		Source: map[string]interface{}{},
		Info:   graphql.ResolveInfo{FieldName: "myField"},
	}

	result, err := JSONNumberResolver(params)
	assert.Nil(t, err)
	assert.Equal(t, nil, result)
}
