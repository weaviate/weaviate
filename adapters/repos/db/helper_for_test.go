//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/sirupsen/logrus"
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
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
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
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
						},
					},
				},
				{
					Class:               "MultiRefCar",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
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
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
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
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
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
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
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
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
						},
					},
				},
				{
					Class:               "MainCategory",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     []string{string(schema.DataTypeString)},
							Tokenization: "word",
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
						{Name: "name", DataType: []string{"string"}, Tokenization: "word"},
					},
				},
				{
					Class:               "City",
					VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{Name: "name", DataType: []string{"string"}, Tokenization: "word"},
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
						{Name: "code", DataType: []string{"string"}, Tokenization: "word"},
						{Name: "phone", DataType: []string{"phoneNumber"}},
						{Name: "inCity", DataType: []string{"City"}},
					},
				},
			},
		},
	}
}

func testCtx() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
}

func testShard(ctx context.Context) *Shard {
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{shardState: shardState}

	idx := &Index{
		Config:                IndexConfig{RootPath: dirName},
		invertedIndexConfig:   schema.InvertedIndexConfig{CleanupIntervalSeconds: 1},
		vectorIndexUserConfig: hnsw.UserConfig{CleanupIntervalSeconds: 1},
		logger:                logrus.New(),
		getSchema:             schemaGetter,
	}

	shd, err := NewShard(ctx, "testshard", idx)
	if err != nil {
		panic(err)
	}

	return shd
}

func testObject() *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object:            models.Object{ID: strfmt.UUID(uuid.NewString())},
		Vector:            []float32{1, 2, 3},
	}
}
