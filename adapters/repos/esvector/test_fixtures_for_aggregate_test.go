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

package esvector

import "github.com/semi-technologies/weaviate/entities/models"

var productClass = &models.Class{
	Class: "AggregationsTestProduct",
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
			DataType: []string{"string"},
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

var companies = []map[string]interface{}{
	{"sector": "Financials", "location": "New York", "dividendYield": 1.3, "price": 150, "listedInIndex": true},
	{"sector": "Financials", "location": "New York", "dividendYield": 4, "price": 600, "listedInIndex": true},
	{"sector": "Financials", "location": "San Francisco", "dividendYield": 1.3, "price": 47, "listedInIndex": true},
	{"sector": "Food", "location": "Atlanta", "dividendYield": 1.3, "price": 160, "listedInIndex": true},
	{"sector": "Food", "location": "Atlanta", "dividendYield": 2.0, "price": 70, "listedInIndex": true},
	{"sector": "Food", "location": "Los Angeles", "dividendYield": 0, "price": 800, "listedInIndex": false},
	{"sector": "Food", "location": "Detroit", "dividendYield": 8, "price": 10, "listedInIndex": true},
	{"sector": "Food", "location": "San Francisco", "dividendYield": 0, "price": 200, "listedInIndex": true},
	{"sector": "Food", "location": "New York", "dividendYield": 1.1, "price": 70, "listedInIndex": true},
}
