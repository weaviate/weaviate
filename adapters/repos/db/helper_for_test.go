//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func parkingGaragesSchema() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:               "MultiRefParkingGarage",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{Name: "name", DataType: []string{"string"}, Tokenization: "word"},
					},
				},
				{
					Class:               "City",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
	//nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
}

func testShard(t *testing.T, ctx context.Context, className string, indexOpts ...func(*Index)) (*Shard, *Index) {
	rand.Seed(time.Now().UnixNano())
	tmpDir := t.TempDir()
	repo, err := New(logrus.New(), Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)

	shardState := singleShardState()
	class := models.Class{Class: className}
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{&class},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: sch}
	queue := make(chan job, 100000)
	idx := &Index{
		Config:                IndexConfig{RootPath: tmpDir, ClassName: schema.ClassName(className)},
		invertedIndexConfig:   schema.InvertedIndexConfig{},
		vectorIndexUserConfig: enthnsw.UserConfig{Skip: true},
		logger:                logrus.New(),
		getSchema:             schemaGetter,
		Shards:                map[string]*Shard{},
		centralJobQueue:       queue,
	}

	for _, opt := range indexOpts {
		opt(idx)
	}

	shardName := shardState.AllPhysicalShards()[0]

	shd, err := NewShard(ctx, nil, shardName, idx, &class, repo.jobQueueCh)
	if err != nil {
		panic(err)
	}

	idx.Shards[shardName] = shd

	return shd, idx
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
