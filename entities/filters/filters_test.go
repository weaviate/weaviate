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
		test{op: OperatorEqual, expectedName: "Equal", expectedOnValue: true},
		test{op: OperatorNotEqual, expectedName: "NotEqual", expectedOnValue: true},
		test{op: OperatorGreaterThan, expectedName: "GreaterThan", expectedOnValue: true},
		test{op: OperatorGreaterThanEqual, expectedName: "GreaterThanEqual", expectedOnValue: true},
		test{op: OperatorLessThanEqual, expectedName: "LessThanEqual", expectedOnValue: true},
		test{op: OperatorLessThan, expectedName: "LessThan", expectedOnValue: true},
		test{op: OperatorWithinGeoRange, expectedName: "WithinGeoRange", expectedOnValue: true},
		test{op: OperatorLike, expectedName: "Like", expectedOnValue: true},
		test{op: OperatorAnd, expectedName: "And", expectedOnValue: false},
		test{op: OperatorOr, expectedName: "Or", expectedOnValue: false},
		test{op: OperatorNot, expectedName: "Not", expectedOnValue: false},
	}

	for _, test := range tests {
		t.Run(test.expectedName, func(t *testing.T) {
			assert.Equal(t, test.expectedName, test.op.Name(), "name must match")
			assert.Equal(t, test.expectedOnValue, test.op.OnValue(), "onValue must match")

		})
	}

}
