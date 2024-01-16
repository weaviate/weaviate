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

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOperators(t *testing.T) {
	type test struct {
		op              Operator
		expectedName    string
		expectedOnValue bool
	}

	tests := []test{
		{op: OperatorEqual, expectedName: "Equal", expectedOnValue: true},
		{op: OperatorNotEqual, expectedName: "NotEqual", expectedOnValue: true},
		{op: OperatorGreaterThan, expectedName: "GreaterThan", expectedOnValue: true},
		{op: OperatorGreaterThanEqual, expectedName: "GreaterThanEqual", expectedOnValue: true},
		{op: OperatorLessThanEqual, expectedName: "LessThanEqual", expectedOnValue: true},
		{op: OperatorLessThan, expectedName: "LessThan", expectedOnValue: true},
		{op: OperatorWithinGeoRange, expectedName: "WithinGeoRange", expectedOnValue: true},
		{op: OperatorLike, expectedName: "Like", expectedOnValue: true},
		{op: OperatorAnd, expectedName: "And", expectedOnValue: false},
		{op: OperatorOr, expectedName: "Or", expectedOnValue: false},
	}

	for _, test := range tests {
		t.Run(test.expectedName, func(t *testing.T) {
			assert.Equal(t, test.expectedName, test.op.Name(), "name must match")
			assert.Equal(t, test.expectedOnValue, test.op.OnValue(), "onValue must match")
		})
	}
}
