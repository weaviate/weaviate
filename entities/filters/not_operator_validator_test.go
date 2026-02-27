//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// Test helper functions to reduce code duplication

func newMockAuthorizedGetClass() func(string) (*models.Class, error) {
	return func(string) (*models.Class, error) {
		return &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{Name: "status", DataType: schema.DataTypeText.PropString()},
				{Name: "count", DataType: schema.DataTypeInt.PropString()},
				{Name: "active", DataType: schema.DataTypeBoolean.PropString()},
			},
		}, nil
	}
}

func newSimpleClause(op Operator, prop string, dataType schema.DataType, value interface{}) Clause {
	return Clause{
		Operator: op,
		On:       &Path{Class: "TestClass", Property: schema.PropertyName(prop)},
		Value:    &Value{Type: dataType, Value: value},
	}
}

func newNotClause(operands ...Clause) *Clause {
	// Ensure operands is never nil, always a slice (even if empty)
	if operands == nil {
		operands = []Clause{}
	}
	return &Clause{
		Operator: OperatorNot,
		Operands: operands,
	}
}

func newLogicalClause(op Operator, operands ...Clause) Clause {
	return Clause{Operator: op, Operands: operands}
}

func TestValidateNotOperator(t *testing.T) {
	getClass := newMockAuthorizedGetClass()

	tests := []struct {
		name           string
		clause         *Clause
		expectError    bool
		errorContains  []string
	}{
		{
			name:          "no operands",
			clause:        &Clause{Operator: OperatorNot, Operands: []Clause{}},
			expectError:   true,
			errorContains: []string{"no children for operator", "Not"},
		},
		{
			name: "one valid operand",
			clause: newNotClause(
				newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
			),
			expectError: false,
		},
		{
			name: "two operands",
			clause: newNotClause(
				newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
				newSimpleClause(OperatorGreaterThan, "count", schema.DataTypeInt, 10),
			),
			expectError:   true,
			errorContains: []string{"too many children for operator", "Not", "Expected 1, given 2"},
		},
		{
			name: "three operands",
			clause: newNotClause(
				newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
				newSimpleClause(OperatorGreaterThan, "count", schema.DataTypeInt, 10),
				newSimpleClause(OperatorLessThan, "count", schema.DataTypeInt, 100),
			),
			expectError:   true,
			errorContains: []string{"too many children for operator", "Not", "Expected 1, given 3"},
		},
		{
			name: "nested And operand",
			clause: newNotClause(
				newLogicalClause(OperatorAnd,
					newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
					newSimpleClause(OperatorGreaterThan, "count", schema.DataTypeInt, 10),
				),
			),
			expectError: false,
		},
		{
			name: "nested Or operand",
			clause: newNotClause(
				newLogicalClause(OperatorOr,
					newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
					newSimpleClause(OperatorEqual, "active", schema.DataTypeBoolean, true),
				),
			),
			expectError: false,
		},
		{
			name: "nested Not operand",
			clause: newNotClause(
				*newNotClause(
					newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
				),
			),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNotOperator(getClass, newClauseWrapper(tt.clause))

			if tt.expectError {
				require.Error(t, err)
				for _, substr := range tt.errorContains {
					assert.Contains(t, err.Error(), substr)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIntegrationWithValidateClause(t *testing.T) {
	getClass := newMockAuthorizedGetClass()

	t.Run("ValidateClause routes Not operator correctly", func(t *testing.T) {
		clause := &Clause{Operator: OperatorNot, Operands: []Clause{}}
		err := validateClause(getClass, newClauseWrapper(clause))

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no children for operator")
		assert.Contains(t, err.Error(), "Not")
	})

	t.Run("ValidateFilters with valid Not operator", func(t *testing.T) {
		filter := &LocalFilter{
			Root: newNotClause(
				newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
			),
		}

		err := ValidateFilters(getClass, filter)
		assert.NoError(t, err)
	})

	t.Run("ValidateFilters with invalid Not operator", func(t *testing.T) {
		filter := &LocalFilter{
			Root: newNotClause(
				newSimpleClause(OperatorEqual, "status", schema.DataTypeText, "active"),
				newSimpleClause(OperatorGreaterThan, "count", schema.DataTypeInt, 10),
			),
		}

		err := ValidateFilters(getClass, filter)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many children for operator")
		assert.Contains(t, err.Error(), "Not")
		assert.Contains(t, err.Error(), "Expected 1, given 2")
	})
}
