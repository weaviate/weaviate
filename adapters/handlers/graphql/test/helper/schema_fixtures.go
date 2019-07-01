/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package helper

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

var many = "many"

var SimpleSchema = schema.Schema{
	Things: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "SomeThing",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:     "intField",
						DataType: []string{"int"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "NetworkRefField",
						DataType: []string{"OtherInstance/SomeRemoteClass"},
					},
				},
			},
		},
	},
	Actions: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "SomeAction",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:     "intField",
						DataType: []string{"int"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "location",
						DataType: []string{"geoCoordinates"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "hasAction",
						DataType: []string{"SomeAction"},
					},
					&models.SemanticSchemaClassProperty{
						Name:        "hasActions",
						DataType:    []string{"SomeAction"},
						Cardinality: &many,
					},
				},
			},
		},
	},
}

// CarSchema contains a car which has every primtive field and a ref field there is
var CarSchema = schema.Schema{
	Things: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "Manufacturer",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:     "name",
						DataType: []string{"string"},
					},
				},
			},
			&models.SemanticSchemaClass{
				Class: "Car",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:     "horsepower",
						DataType: []string{"int"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "weight",
						DataType: []string{"number"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "modelName",
						DataType: []string{"string"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "madeBy",
						DataType: []string{"Manufacturer"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "startOfProduction",
						DataType: []string{"date"},
					},
					&models.SemanticSchemaClassProperty{
						Name:     "stillInProduction",
						DataType: []string{"boolean"},
					},
				},
			},
		},
	},
	Actions: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{},
	},
}
