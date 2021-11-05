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
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

func parkingGaragesSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "MultiRefParkingGarage",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "location",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
					},
				},
				{
					Class:               "MultiRefParkingLot",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
				{
					Class:               "MultiRefCar",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "parkedAt",
							DataType: []string{"MultiRefParkingGarage", "MultiRefParkingLot"},
						},
					},
				},
				{
					Class:               "MultiRefDriver",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "drives",
							DataType: []string{"MultiRefCar"},
						},
					},
				},
				{
					Class:               "MultiRefPerson",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "friendsWith",
							DataType: []string{"MultiRefDriver"},
						},
					},
				},
				{
					Class:               "MultiRefSociety",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
						{
							Name:     "hasMembers",
							DataType: []string{"MultiRefPerson"},
						},
					},
				},

				// for classifications test
				{
					Class:               "ExactCategory",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
				{
					Class:               "MainCategory",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:     "name",
							DataType: []string{string(schema.DataTypeString)},
						},
					},
				},
			},
		},
	}
}

func cityCountryAirportSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "Country",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{Name: "name", DataType: []string{"string"}},
					},
				},
				{
					Class:               "City",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{Name: "name", DataType: []string{"string"}},
						{Name: "inCountry", DataType: []string{"Country"}},
						{Name: "population", DataType: []string{"int"}},
						{Name: "location", DataType: []string{"geoCoordinates"}},
					},
				},
				{
					Class:               "Airport",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{Name: "code", DataType: []string{"string"}},
						{Name: "phone", DataType: []string{"phoneNumber"}},
						{Name: "inCity", DataType: []string{"City"}},
					},
				},
			},
		},
	}
}
