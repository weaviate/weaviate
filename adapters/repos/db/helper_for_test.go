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
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/sirupsen/logrus"

	"github.com/semi-technologies/weaviate/usecases/config"
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

func testShard(t *testing.T, ctx context.Context, className string, indexOpts ...func(*Index)) (*Shard, *Index) {
	rand.Seed(time.Now().UnixNano())
	tmpDir := t.TempDir()

	shardState := singleShardState()
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{{Class: className}},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: sch}

	idx := &Index{
		Config:                IndexConfig{RootPath: tmpDir, ClassName: schema.ClassName(className), MaxImportGoroutinesFactor: 1.5},
		invertedIndexConfig:   schema.InvertedIndexConfig{CleanupIntervalSeconds: 1},
		vectorIndexUserConfig: hnsw.UserConfig{Skip: true},
		logger:                logrus.New(),
		getSchema:             schemaGetter,
		Shards:                map[string]*Shard{},
	}

	for _, opt := range indexOpts {
		opt(idx)
	}

	shardName := shardState.AllPhysicalShards()[0]

	shd, err := NewShard(ctx, nil, shardName, idx, config.Config{})
	if err != nil {
		panic(err)
	}

	idx.Shards[shardName] = shd

	return shd, idx
}

func withVectorIndexing(affirmative bool) func(*Index) {
	if affirmative {
		return func(i *Index) {
			i.vectorIndexUserConfig = hnsw.NewDefaultUserConfig()
		}
	}

	return func(i *Index) {
		i.vectorIndexUserConfig = hnsw.UserConfig{Skip: true}
	}
}

func testObject(className string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
		},
		Vector: []float32{1, 2, 3},
	}
}

func createRandomObjects(className string, numObj int) []*storobj.Object {
	obj := make([]*storobj.Object, numObj)

	for i := 0; i < numObj; i++ {
		obj[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
			},
			Vector: []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()},
		}
	}
	return obj
}
