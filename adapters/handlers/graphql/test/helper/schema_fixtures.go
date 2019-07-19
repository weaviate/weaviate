//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package helper

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

var many = "many"

var SimpleSchema = schema.Schema{
	Things: &models.Schema{
		Classes: []*models.Class{
			&models.Class{
				Class: "SomeThing",
				Properties: []*models.Property{
					&models.Property{
						Name:     "intField",
						DataType: []string{"int"},
					},
					&models.Property{
						Name:     "NetworkRefField",
						DataType: []string{"OtherInstance/SomeRemoteClass"},
					},
				},
			},
		},
	},
	Actions: &models.Schema{
		Classes: []*models.Class{
			&models.Class{
				Class: "SomeAction",
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
						Name:     "hasAction",
						DataType: []string{"SomeAction"},
					},
					&models.Property{
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
	Things: &models.Schema{
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
	Actions: &models.Schema{
		Classes: []*models.Class{},
	},
}
