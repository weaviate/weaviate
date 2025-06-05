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

package db

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
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
	return testShardWithSettings(t, ctx, &models.Class{Class: className}, enthnsw.UserConfig{Skip: true},
		false, false, indexOpts...)
}

func testShardWithSettings(t *testing.T, ctx context.Context, class *models.Class,
	vic schemaConfig.VectorIndexConfig, withStopwords, withCheckpoints bool, indexOpts ...func(*Index),
) (ShardLike, *Index) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	maxResults := int64(10_000)

	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  tmpDir,
		QueryMaximumResults:       maxResults,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)

	shardState := singleShardState()
	sch := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}
	schemaGetter := &fakeSchemaGetter{shardState: shardState, schema: sch}

	iic := schema.InvertedIndexConfig{}
	if class.InvertedIndexConfig != nil {
		iic = inverted.ConfigFromModel(class.InvertedIndexConfig)
	}
	var sd *stopwords.Detector
	if withStopwords {
		sd, err = stopwords.NewDetectorFromConfig(iic.Stopwords)
		require.NoError(t, err)
	}
	var checkpts *indexcheckpoint.Checkpoints
	if withCheckpoints {
		checkpts, err = indexcheckpoint.New(tmpDir, logger)
		require.NoError(t, err)
	}

	idx := &Index{
		Config: IndexConfig{
			RootPath:            tmpDir,
			ClassName:           schema.ClassName(class.Class),
			QueryMaximumResults: maxResults,
			ReplicationFactor:   1,
		},
		metrics:                NewMetrics(logger, nil, class.Class, ""),
		partitioningEnabled:    shardState.PartitioningEnabled,
		invertedIndexConfig:    iic,
		vectorIndexUserConfig:  vic,
		vectorIndexUserConfigs: map[string]schemaConfig.VectorIndexConfig{},
		logger:                 logger,
		getSchema:              schemaGetter,
		centralJobQueue:        repo.jobQueueCh,
		stopwords:              sd,
		indexCheckpoints:       checkpts,
		allocChecker:           memwatch.NewDummyMonitor(),
		shardCreateLocks:       esync.NewKeyLocker(),
		scheduler:              repo.scheduler,
		shardLoadLimiter:       NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
		shardReindexer:         NewShardReindexerV3Noop(),
	}
	idx.closingCtx, idx.closingCancel = context.WithCancel(context.Background())
	idx.initCycleCallbacksNoop()
	for _, opt := range indexOpts {
		opt(idx)
	}

	shardName := shardState.AllPhysicalShards()[0]

	shard, err := idx.initShard(ctx, shardName, class, nil, idx.Config.DisableLazyLoadShards, true)
	require.NoError(t, err)

	idx.shards.Store(shardName, shard)

	return idx.shards.Load(shardName), idx
}

func testObject(className string) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
		},
	}
}

func createRandomObjects(r *rand.Rand, className string, numObj int, vectorDim int) []*storobj.Object {
	obj := make([]*storobj.Object, numObj)

	for i := 0; i < numObj; i++ {
		obj[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
			},
			Vector: make([]float32, vectorDim),
		}

		for d := 0; d < vectorDim; d++ {
			obj[i].Vector[d] = r.Float32()
		}
	}
	return obj
}

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
		UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
	}
}
