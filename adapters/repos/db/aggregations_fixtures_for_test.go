//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
)

var productClass = &models.Class{
	Class:               "AggregationsTestProduct",
	VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:     "name",
			DataType: []string{"string"},
		},
	},
}

var companyClass = &models.Class{
	Class:               "AggregationsTestCompany",
	VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:     "sector",
			DataType: []string{"string"},
		},
		{
			Name:     "location",
			DataType: []string{"text"},
		},
		{
			Name:     "dividendYield",
			DataType: []string{"number"},
		},
		{
			Name:     "price",
			DataType: []string{"int"}, // unrealistic for this to be an int, but
			// we've already tested another number prop ;-)
		},
		{
			Name:     "listedInIndex",
			DataType: []string{"boolean"},
		},
		{
			Name:     "makesProduct",
			DataType: []string{"AggregationsTestProduct"},
		},
	},
}

var arrayTypesClass = &models.Class{
	Class:               "AggregationsTestArrayTypes",
	VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
	InvertedIndexConfig: invertedConfig(),
	Properties: []*models.Property{
		{
			Name:     "strings",
			DataType: []string{"string[]"},
		},
		{
			Name:     "numbers",
			DataType: []string{"number[]"},
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
				Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", productsIds[0])),
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

var arrayTypes = []map[string]interface{}{
	{
		"strings": []string{"a", "b", "c"},
		"numbers": []float64{1.0, 2.0, 3.0},
	},
	{
		"strings": []string{"a"},
		"numbers": []float64{1.0, 2.0},
	},
}
