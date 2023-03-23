//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestValidateIsNullOperator(t *testing.T) {
	tests := []struct {
		name       string
		schemaType schema.DataType
		valid      bool
	}{
		{
			name:       "Valid datatype",
			schemaType: schema.DataTypeBoolean,
			valid:      true,
		},
		{
			name:       "Invalid datatype (array)",
			schemaType: schema.DataTypeBooleanArray,
			valid:      false,
		},
		{
			name:       "Invalid datatype (text)",
			schemaType: schema.DataTypeText,
			valid:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sch := schema.Schema{Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{Name: "modelName", DataType: []string{"string"}},
							{Name: "manufacturerName", DataType: []string{"string"}},
							{Name: "horsepower", DataType: []string{"int"}},
						},
					},
				},
			}}
			cl := Clause{
				Operator: OperatorIsNull,
				Value:    &Value{Value: true, Type: tt.schemaType},
				On:       &Path{Class: "Car", Property: "horsepower"},
			}
			err := validateClause(sch, &cl)
			if tt.valid {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestValidatePropertyLength(t *testing.T) {
	tests := []struct {
		name       string
		schemaType schema.DataType
		valid      bool
		operator   Operator
		value      int
	}{
		{
			name:       "Valid datatype and operator",
			schemaType: schema.DataTypeInt,
			valid:      true,
			operator:   OperatorEqual,
			value:      0,
		},
		{
			name:       "Invalid datatype (array)",
			schemaType: schema.DataTypeBooleanArray,
			valid:      false,
			operator:   OperatorEqual,
			value:      1,
		},
		{
			name:       "Invalid datatype (text)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorEqual,
			value:      2,
		},
		{
			name:       "Invalid operator (Or)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorOr,
			value:      10,
		},
		{
			name:       "Invalid value (negative)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorEqual,
			value:      -5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sch := schema.Schema{Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{Name: "horsepower", DataType: []string{"int"}},
						},
					},
				},
			}}
			cl := Clause{
				Operator: OperatorEqual,
				Value:    &Value{Value: tt.value, Type: tt.schemaType},
				On:       &Path{Class: "Car", Property: "len(horsepower)"},
			}
			err := validateClause(sch, &cl)
			if tt.valid {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

func TestValidateUUIDFilter(t *testing.T) {
	tests := []struct {
		name       string
		schemaType schema.DataType
		valid      bool
		operator   Operator
		value      int
	}{
		{
			name:       "Valid datatype and operator",
			schemaType: schema.DataTypeString,
			valid:      true,
			operator:   OperatorEqual,
			value:      0,
		},
		{
			name:       "Wrong data type (text)",
			schemaType: schema.DataTypeText,
			valid:      false,
			operator:   OperatorEqual,
			value:      0,
		},
		{
			name:       "Wrong operator (Like)",
			schemaType: schema.DataTypeString,
			valid:      false,
			operator:   OperatorLike,
			value:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sch := schema.Schema{Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{Name: "my_id", DataType: []string{string(schema.DataTypeUUID)}},
							{Name: "my_idz", DataType: []string{string(schema.DataTypeUUIDArray)}},
						},
					},
				},
			}}
			for _, prop := range []schema.PropertyName{"my_id", "my_idz"} {
				cl := Clause{
					Operator: tt.operator,
					Value:    &Value{Value: tt.value, Type: tt.schemaType},
					On:       &Path{Class: "Car", Property: prop},
				}
				err := validateClause(sch, &cl)
				if tt.valid {
					require.Nil(t, err)
				} else {
					require.NotNil(t, err)
				}
			}
		})
	}
}
