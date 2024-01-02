//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
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
						{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
					},
				},
				{
					Class:               "City",
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					Properties: []*models.Property{
						{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
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
						{Name: "code", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
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

func getRandomSeed() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func testShard(t *testing.T, ctx context.Context, className string, indexOpts ...func(*Index)) (ShardLike, *Index) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()

	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)

	shardState := singleShardState()
	class := &models.Class{Class: className}
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: sch}

	idx := &Index{
		Config:                IndexConfig{RootPath: tmpDir, ClassName: schema.ClassName(className)},
		invertedIndexConfig:   schema.InvertedIndexConfig{},
		vectorIndexUserConfig: enthnsw.UserConfig{Skip: true},
		logger:                logger,
		getSchema:             schemaGetter,
		centralJobQueue:       repo.jobQueueCh,
	}
	idx.closingCtx, idx.closingCancel = context.WithCancel(context.Background())

	if err = os.Mkdir(idx.path(), os.ModePerm); err != nil {
		panic(err)
	}
	idx.initCycleCallbacksNoop()

	for _, opt := range indexOpts {
		opt(idx)
	}

	shardName := shardState.AllPhysicalShards()[0]

	shard, err := idx.initShard(ctx, shardName, class, nil)
	if err != nil {
		panic(err)
	}
	idx.shards.Store(shardName, shard)

	return shard, idx
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

func createRandomObjects(r *rand.Rand, className string, numObj int) []*storobj.Object {
	obj := make([]*storobj.Object, numObj)

	for i := 0; i < numObj; i++ {
		obj[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
			},
			Vector: []float32{r.Float32(), r.Float32(), r.Float32(), r.Float32()},
		}
	}
	return obj
}
