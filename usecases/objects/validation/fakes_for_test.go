//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package validation

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func testSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Person",
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "phone",
							DataType: []string{"phoneNumber"},
						},
					},
				},
			},
		},
	}
}

func fakeExists(context.Context, string, strfmt.UUID) (bool, error) {
	return true, nil
}
