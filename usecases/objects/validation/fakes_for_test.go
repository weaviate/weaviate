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

package validation

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/liutizhong/weaviate/entities/additional"
	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/schema"
)

func testSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Person",
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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

func fakeExists(context.Context, string, strfmt.UUID, *additional.ReplicationProperties, string) (bool, error) {
	return true, nil
}
