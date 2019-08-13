// +build integrationTest

package esvector

import "github.com/semi-technologies/weaviate/entities/models"

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
	},
}

var companies = []map[string]interface{}{
	{"sector": "Financials", "location": "New York", "dividendYield": 1.3, "price": 150},
	{"sector": "Financials", "location": "New York", "dividendYield": 4, "price": 600},
	{"sector": "Financials", "location": "San Francisco", "dividendYield": 1.3, "price": 47},
	{"sector": "Food", "location": "Atlanta", "dividendYield": 1.3, "price": 160},
	{"sector": "Food", "location": "Atlanta", "dividendYield": 2.0, "price": 70},
	{"sector": "Food", "location": "Los Angeles", "dividendYield": 0, "price": 800},
	{"sector": "Food", "location": "Detroit", "dividendYield": 8, "price": 10},
	{"sector": "Food", "location": "San Francisco", "dividendYield": 0, "price": 200},
	{"sector": "Food", "location": "New York", "dividendYield": 1.1, "price": 70},
}
