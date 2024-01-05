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

package helper

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

var SimpleSchema = CreateSimpleSchema(config.VectorizerModuleText2VecContextionary)

func CreateSimpleSchema(vectorizer string) schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:      "SomeThing",
					Vectorizer: vectorizer,
					Properties: []*models.Property{
						{
							Name:     "intField",
							DataType: []string{"int"},
						},
					},
				},
				{
					Class:      "CustomVectorClass",
					Vectorizer: config.VectorizerModuleNone,
					Properties: []*models.Property{
						{
							Name:     "intField",
							DataType: []string{"int"},
						},
					},
				},
				{
					Vectorizer: vectorizer,
					Class:      "SomeAction",
					Properties: []*models.Property{
						{
							Name:     "intField",
							DataType: []string{"int"},
						},
						{
							Name:     "uuidField",
							DataType: []string{"uuid"},
						},
						{
							Name:     "uuidArrayField",
							DataType: []string{"uuid[]"},
						},
						{
							Name:     "location",
							DataType: []string{"geoCoordinates"},
						},
						{
							Name:     "phone",
							DataType: []string{"phoneNumber"},
						},
						{
							Name:     "hasAction",
							DataType: []string{"SomeAction"},
						},
						{
							Name:     "hasActions",
							DataType: []string{"SomeAction"},
						},
					},
				},
			},
		},
	}
}

// CarSchema contains a car which has every primitive field and a ref field there is
var CarSchema = schema.Schema{
	Objects: &models.Schema{
		Classes: []*models.Class{
			{
				Class: "Manufacturer",
				Properties: []*models.Property{
					{
						Name:         "name",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
				},
			},
			{
				Class: "Car",
				Properties: []*models.Property{
					{
						Name:     "horsepower",
						DataType: []string{"int"},
					},
					{
						Name:     "weight",
						DataType: []string{"number"},
					},
					{
						Name:         "modelName",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
					{
						Name:     "madeBy",
						DataType: []string{"Manufacturer"},
					},
					{
						Name:     "startOfProduction",
						DataType: []string{"date"},
					},
					{
						Name:     "stillInProduction",
						DataType: []string{"boolean"},
					},
				},
			},
		},
	},
}
