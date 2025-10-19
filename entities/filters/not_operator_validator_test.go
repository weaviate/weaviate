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

func TestValidateNotOperator(t *testing.T) {
	// Helper function to create a mock authorizedGetClass
	authorizedGetClass := func(string) (*models.Class, error) {
		return &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{Name: "status", DataType: schema.DataTypeText.PropString()},
				{Name: "count", DataType: schema.DataTypeInt.PropString()},
				{Name: "active", DataType: schema.DataTypeBoolean.PropString()},
			},
		}, nil
	}

	t.Run("Not operator with no operands", func(t *testing.T) {
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{}, // No operands
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no children for operator")
		assert.Contains(t, err.Error(), "Not")
	})

	t.Run("Not operator with one valid operand", func(t *testing.T) {
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{
				{
					Operator: OperatorEqual,
					On: &Path{
						Class:    "TestClass",
						Property: "status",
					},
					Value: &Value{
						Type:  schema.DataTypeText,
						Value: "active",
					},
				},
			},
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		assert.NoError(t, err)
	})

	t.Run("Not operator with two operands", func(t *testing.T) {
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{
				{
					Operator: OperatorEqual,
					On: &Path{
						Class:    "TestClass",
						Property: "status",
					},
					Value: &Value{
						Type:  schema.DataTypeText,
						Value: "active",
					},
				},
				{
					Operator: OperatorGreaterThan,
					On: &Path{
						Class:    "TestClass",
						Property: "count",
					},
					Value: &Value{
						Type:  schema.DataTypeInt,
						Value: 10,
					},
				},
			},
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many children for operator")
		assert.Contains(t, err.Error(), "Not")
		assert.Contains(t, err.Error(), "Expected 1, given 2")
	})

	t.Run("Not operator with multiple operands", func(t *testing.T) {
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{
				{
					Operator: OperatorEqual,
					On: &Path{
						Class:    "TestClass",
						Property: "status",
					},
					Value: &Value{
						Type:  schema.DataTypeText,
						Value: "active",
					},
				},
				{
					Operator: OperatorGreaterThan,
					On: &Path{
						Class:    "TestClass",
						Property: "count",
					},
					Value: &Value{
						Type:  schema.DataTypeInt,
						Value: 10,
					},
				},
				{
					Operator: OperatorLessThan,
					On: &Path{
						Class:    "TestClass",
						Property: "count",
					},
					Value: &Value{
						Type:  schema.DataTypeInt,
						Value: 100,
					},
				},
			},
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many children for operator")
		assert.Contains(t, err.Error(), "Not")
		assert.Contains(t, err.Error(), "Expected 1, given 3")
	})

	t.Run("Not operator with nested And operand", func(t *testing.T) {
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{
				{
					Operator: OperatorAnd,
					Operands: []Clause{
						{
							Operator: OperatorEqual,
							On: &Path{
								Class:    "TestClass",
								Property: "status",
							},
							Value: &Value{
								Type:  schema.DataTypeText,
								Value: "active",
							},
						},
						{
							Operator: OperatorGreaterThan,
							On: &Path{
								Class:    "TestClass",
								Property: "count",
							},
							Value: &Value{
								Type:  schema.DataTypeInt,
								Value: 10,
							},
						},
					},
				},
			},
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		assert.NoError(t, err)
	})

	t.Run("Not operator with nested Or operand", func(t *testing.T) {
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{
				{
					Operator: OperatorOr,
					Operands: []Clause{
						{
							Operator: OperatorEqual,
							On: &Path{
								Class:    "TestClass",
								Property: "status",
							},
							Value: &Value{
								Type:  schema.DataTypeText,
								Value: "active",
							},
						},
						{
							Operator: OperatorEqual,
							On: &Path{
								Class:    "TestClass",
								Property: "active",
							},
							Value: &Value{
								Type:  schema.DataTypeBoolean,
								Value: true,
							},
						},
					},
				},
			},
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		assert.NoError(t, err)
	})

	t.Run("Not operator with nested Not operand", func(t *testing.T) {
		// Not(Not(condition)) - while logically redundant, should validate
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{
				{
					Operator: OperatorNot,
					Operands: []Clause{
						{
							Operator: OperatorEqual,
							On: &Path{
								Class:    "TestClass",
								Property: "status",
							},
							Value: &Value{
								Type:  schema.DataTypeText,
								Value: "active",
							},
						},
					},
				},
			},
		}

		err := validateNotOperator(authorizedGetClass, newClauseWrapper(clause))
		assert.NoError(t, err)
	})
}

func TestIntegrationWithValidateClause(t *testing.T) {
	// Helper function to create a mock authorizedGetClass
	authorizedGetClass := func(string) (*models.Class, error) {
		return &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{Name: "status", DataType: schema.DataTypeText.PropString()},
			},
		}, nil
	}

	t.Run("ValidateClause routes Not operator correctly", func(t *testing.T) {
		// Test that validateClause properly routes to validateNotOperator
		clause := &Clause{
			Operator: OperatorNot,
			Operands: []Clause{}, // No operands - should fail
		}

		err := validateClause(authorizedGetClass, newClauseWrapper(clause))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no children for operator")
		assert.Contains(t, err.Error(), "Not")
	})

	t.Run("ValidateFilters with Not operator", func(t *testing.T) {
		filter := &LocalFilter{
			Root: &Clause{
				Operator: OperatorNot,
				Operands: []Clause{
					{
						Operator: OperatorEqual,
						On: &Path{
							Class:    "TestClass",
							Property: "status",
						},
						Value: &Value{
							Type:  schema.DataTypeText,
							Value: "active",
						},
					},
				},
			},
		}

		err := ValidateFilters(authorizedGetClass, filter)
		assert.NoError(t, err)
	})

	t.Run("ValidateFilters with invalid Not operator", func(t *testing.T) {
		filter := &LocalFilter{
			Root: &Clause{
				Operator: OperatorNot,
				Operands: []Clause{
					{
						Operator: OperatorEqual,
						On: &Path{
							Class:    "TestClass",
							Property: "status",
						},
						Value: &Value{
							Type:  schema.DataTypeText,
							Value: "active",
						},
					},
					{
						Operator: OperatorGreaterThan,
						On: &Path{
							Class:    "TestClass",
							Property: "count",
						},
						Value: &Value{
							Type:  schema.DataTypeInt,
							Value: 10,
						},
					},
				},
			},
		}

		err := ValidateFilters(authorizedGetClass, filter)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many children for operator")
		assert.Contains(t, err.Error(), "Not")
		assert.Contains(t, err.Error(), "Expected 1, given 2")
	})
}