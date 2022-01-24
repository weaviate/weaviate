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

package helper

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
)

var SimpleSchema = CreateSimpleSchema(config.VectorizerModuleText2VecContextionary)

func CreateSimpleSchema(vectorizer string) schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				&models.Class{
					Class:      "SomeThing",
					Vectorizer: vectorizer,
					Properties: []*models.Property{
						&models.Property{
							Name:     "intField",
							DataType: []string{"int"},
						},
					},
				},
				&models.Class{
					Class:      "CustomVectorClass",
					Vectorizer: config.VectorizerModuleNone,
					Properties: []*models.Property{
						&models.Property{
							Name:     "intField",
							DataType: []string{"int"},
						},
					},
				},
				&models.Class{
					Vectorizer: vectorizer,
					Class:      "SomeAction",
					Properties: []*models.Property{
						&models.Property{
							Name:     "intField",
							DataType: []string{"int"},
						},
						&models.Property{
							Name:     "location",
							DataType: []string{"geoCoordinates"},
						},
						&models.Property{
							Name:     "phone",
							DataType: []string{"phoneNumber"},
						},
						&models.Property{
							Name:     "hasAction",
							DataType: []string{"SomeAction"},
						},
						&models.Property{
							Name:     "hasActions",
							DataType: []string{"SomeAction"},
						},
					},
				},
			},
		},
	}
}

// CarSchema contains a car which has every primtive field and a ref field there is
var CarSchema = schema.Schema{
	Objects: &models.Schema{
		Classes: []*models.Class{
			&models.Class{
				Class: "Manufacturer",
				Properties: []*models.Property{
					&models.Property{
						Name:     "name",
						DataType: []string{"string"},
					},
				},
			},
			&models.Class{
				Class: "Car",
				Properties: []*models.Property{
					&models.Property{
						Name:     "horsepower",
						DataType: []string{"int"},
					},
					&models.Property{
						Name:     "weight",
						DataType: []string{"number"},
					},
					&models.Property{
						Name:     "modelName",
						DataType: []string{"string"},
					},
					&models.Property{
						Name:     "madeBy",
						DataType: []string{"Manufacturer"},
					},
					&models.Property{
						Name:     "startOfProduction",
						DataType: []string{"date"},
					},
					&models.Property{
						Name:     "stillInProduction",
						DataType: []string{"boolean"},
					},
				},
			},
		},
	},
}
