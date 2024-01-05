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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestSortValidation(t *testing.T) {
	tests := []struct {
		name  string
		prop  string
		valid bool
	}{
		{
			name:  "existing prop - string",
			valid: true,
			prop:  "modelName",
		},
		{
			name:  "existing prop - int",
			valid: true,
			prop:  "horsepower",
		},
		{
			name:  "invalid prop",
			valid: false,
			prop:  "idontexist",
		},
		{
			name:  "uuid prop",
			valid: false,
			prop:  "my_id",
		},
		{
			name:  "uuid[] prop",
			valid: false,
			prop:  "my_idz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sch := schema.Schema{Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: "Car",
						Properties: []*models.Property{
							{Name: "modelName", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
							{Name: "manufacturerName", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
							{Name: "horsepower", DataType: []string{"int"}},
							{Name: "my_id", DataType: []string{"uuid"}},
							{Name: "my_idz", DataType: []string{"uuid[]"}},
						},
					},
				},
			}}

			sort := []Sort{{
				Path:  []string{tt.prop},
				Order: "asc",
			}}

			err := ValidateSort(sch, schema.ClassName("Car"), sort)
			if tt.valid {
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}
