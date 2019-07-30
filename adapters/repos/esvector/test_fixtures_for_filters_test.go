//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package esvector

import (
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

var carClass = &models.Class{
	Class: "FilterTestCar",
	Properties: []*models.Property{
		&models.Property{
			DataType: []string{string(schema.DataTypeString)},
			Name:     "modelName",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeString)},
			Name:     "contact",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeText)},
			Name:     "description",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeInt)},
			Name:     "horsepower",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeNumber)},
			Name:     "weight",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeGeoCoordinates)},
			Name:     "parkedAt",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeDate)},
			Name:     "released",
		},
	},
}

var (
	carSprinterID strfmt.UUID = "d4c48788-7798-4bdd-bca9-5cd5012a5271"
	carE63sID     strfmt.UUID = "62906c61-f92f-4f2c-874f-842d4fb9d80b"
	carPoloID     strfmt.UUID = "b444e1d8-d73a-4d53-a417-8d6501c27f2e"
)

var cars = []models.Thing{
	models.Thing{
		Class: carClass.Class,
		ID:    carSprinterID,
		Schema: map[string]interface{}{
			"modelName":  "sprinter",
			"horsepower": "130",
			"weight":     "3499.90",
			"released":   "1995-08-17T12:47:00+02:00",
			"parkedAt": &models.GeoCoordinates{
				Latitude:  34.052235,
				Longitude: -118.243683,
			},
			"contact":     "john@heavycars.example.com",
			"description": "This car resembles a large van that can still be driven with a regular license. Contact john@heavycars.example.com for details",
		},
	},
	models.Thing{
		Class: carClass.Class,
		ID:    carE63sID,
		Schema: map[string]interface{}{
			"modelName":  "e63s",
			"horsepower": "612",
			"weight":     "2069.5",
			"released":   "2017-02-17T09:47:00+02:00",
			"parkedAt": &models.GeoCoordinates{
				Latitude:  40.730610,
				Longitude: -73.935242,
			},
			"contact":     "jessica@fastcars.example.com",
			"description": "This car has a huge motor, but it's also not exactly lightweight.",
		},
	},
	models.Thing{
		Class: carClass.Class,
		ID:    carPoloID,
		Schema: map[string]interface{}{
			"released":    "1975-01-01T10:12:00+02:00",
			"modelName":   "polo",
			"horsepower":  "100",
			"weight":      "1200",
			"contact":     "sandra@efficientcars.example.com",
			"description": "This small car has a small engine, but it's very light, so it feels fater than it is.",
		},
	},
}

var carVectors = [][]float32{
	{1.1, 0, 0, 0, 0},
	{0, 1.1, 0, 0, 0},
	{0, 0, 1.1, 0, 0},
	{0, 0, 0, 1.1, 0},
	{0, 0, 0, 0, 1.1},
}
