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
			DataType: []string{string(schema.DataTypeInt)},
			Name:     "horsepower",
		},
		&models.Property{
			DataType: []string{string(schema.DataTypeNumber)},
			Name:     "weight",
		},
	},
}

var (
	carSprinterID strfmt.UUID = "d4c48788-7798-4bdd-bca9-5cd5012a5271"
	carE63sID     strfmt.UUID = "62906c61-f92f-4f2c-874f-842d4fb9d80b"
)

var cars = []models.Thing{
	models.Thing{
		Class: carClass.Class,
		ID:    carSprinterID,
		Schema: map[string]interface{}{
			"modelName":  "sprinter",
			"horsepower": "130",
			"weight":     "3499.90",
		},
	},
	models.Thing{
		Class: carClass.Class,
		ID:    carE63sID,
		Schema: map[string]interface{}{
			"modelName":  "e63s",
			"horsepower": "612",
			"weight":     "2069.5",
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
