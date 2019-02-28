/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package helper

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/models"
)

var many = "many"

var SimpleSchema = schema.Schema{
	Things: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{
			&models.SemanticSchemaClass{
				Class: "SomeThing",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:       "NetworkRefField",
						AtDataType: []string{"OtherInstance/SomeRemoteClass"},
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
						Name:       "intField",
						AtDataType: []string{"int"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "location",
						AtDataType: []string{"geoCoordinate"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "hasAction",
						AtDataType: []string{"SomeAction"},
					},
					&models.SemanticSchemaClassProperty{
						Name:        "hasActions",
						AtDataType:  []string{"SomeAction"},
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
						Name:       "name",
						AtDataType: []string{"string"},
					},
				},
			},
			&models.SemanticSchemaClass{
				Class: "Car",
				Properties: []*models.SemanticSchemaClassProperty{
					&models.SemanticSchemaClassProperty{
						Name:       "horsepower",
						AtDataType: []string{"int"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "weight",
						AtDataType: []string{"number"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "modelName",
						AtDataType: []string{"string"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "madeBy",
						AtDataType: []string{"Manufacturer"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "startOfProduction",
						AtDataType: []string{"date"},
					},
					&models.SemanticSchemaClassProperty{
						Name:       "stillInProduction",
						AtDataType: []string{"boolean"},
					},
				},
			},
		},
	},
	Actions: &models.SemanticSchema{
		Classes: []*models.SemanticSchemaClass{},
	},
}
