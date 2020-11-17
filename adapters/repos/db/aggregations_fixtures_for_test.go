//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package db

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

var productClass = &models.Class{
	Class: "AggregationsTestProduct",
	Properties: []*models.Property{
		&models.Property{
			Name:     "name",
			DataType: []string{"string"},
		},
	},
}

var companyClass = &models.Class{
	Class: "AggregationsTestCompany",
	Properties: []*models.Property{
		&models.Property{
			Name:     "sector",
			DataType: []string{"string"},
		},
		&models.Property{
			Name:     "location",
			DataType: []string{"text"},
		},
		&models.Property{
			Name:     "dividendYield",
			DataType: []string{"number"},
		},
		&models.Property{
			Name:     "price",
			DataType: []string{"int"}, // unrealistic for this to be an int, but
			// we've already tested another number prop ;-)
		},
		&models.Property{
			Name:     "listedInIndex",
			DataType: []string{"boolean"},
		},
		&models.Property{
			Name:     "makesProduct",
			DataType: []string{"AggregationsTestProduct"},
		},
	},
}

var products = []map[string]interface{}{
	{
		"name": "Superbread",
	},
}

var productsIds = []strfmt.UUID{
	"1295c052-263d-4aae-99dd-920c5a370d06",
}

var companies = []map[string]interface{}{
	{
		"sector":        "Financials",
		"location":      "New York",
		"dividendYield": 1.3,
		"price":         int64(150),
		"listedInIndex": true,
	},
	{
		"sector":        "Financials",
		"location":      "New York",
		"dividendYield": 4.0,
		"price":         int64(600),
		"listedInIndex": true,
	},
	{
		"sector":        "Financials",
		"location":      "San Francisco",
		"dividendYield": 1.3,
		"price":         int64(47),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "Atlanta",
		"dividendYield": 1.3,
		"price":         int64(160),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "Atlanta",
		"dividendYield": 2.0,
		"price":         int64(70),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "Los Angeles",
		"dividendYield": 0.0,
		"price":         int64(800),
		"listedInIndex": false,
	},
	{
		"sector":        "Food",
		"location":      "Detroit",
		"dividendYield": 8.0,
		"price":         int64(10),
		"listedInIndex": true,
		"makesProduct": models.MultipleRef{
			&models.SingleRef{
				Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", productsIds[0])),
			},
		},
	},
	{
		"sector":        "Food",
		"location":      "San Francisco",
		"dividendYield": 0.0,
		"price":         int64(200),
		"listedInIndex": true,
	},
	{
		"sector":        "Food",
		"location":      "New York",
		"dividendYield": 1.1,
		"price":         int64(70),
		"listedInIndex": true,
	},
}
