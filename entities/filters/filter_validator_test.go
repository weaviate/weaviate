package filters

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/stretchr/testify/require"
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
