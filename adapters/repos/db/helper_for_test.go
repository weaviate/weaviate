//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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

func testShardMultiTenant(t *testing.T, ctx context.Context, className string, indexOpts ...func(*Index)) (ShardLike, *Index) {
	return testShardWithMultiTenantSettings(t, ctx, &models.Class{Class: className}, enthnsw.UserConfig{Skip: true},
		false, false, indexOpts...)
}

func createTestDatabaseWithClass(t *testing.T, metrics *monitoring.PrometheusMetrics, classes ...*models.Class) *DB {
	t.Helper()

	require.NotNil(t, metrics, "metrics parameter cannot be nil")
	metricsCopy := *metrics
	metricsCopy.Registerer = monitoring.NoopRegisterer

	shardState := singleShardState()
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		for _, class := range classes {
			if className == class.Class {
				return readFunc(class, shardState)
			}
		}
		return nil
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	db, err := New(logrus.New(), "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &FakeRemoteClient{}, &FakeNodeResolver{}, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, &metricsCopy, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)

	db.SetSchemaGetter(&fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: classes}},
		shardState: shardState,
	})

	require.Nil(t, db.WaitForStartup(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, db.Shutdown(context.Background()))
	})

	return db
}

func publishVectorMetricsFromDB(t *testing.T, db *DB) {
	t.Helper()

	if !db.config.TrackVectorDimensions {
		t.Logf("Vector dimensions tracking is disabled, returning 0")
		return
	}
	db.metricsObserver.publishVectorMetrics(t.Context())
}

func getSingleShardNameFromRepo(repo *DB, className string) string {
	shardName := ""
	if !repo.config.TrackVectorDimensions {
		log.Printf("Vector dimensions tracking is disabled, returning 0")
		return shardName
	}
	index := repo.GetIndex(schema.ClassName(className))
	index.ForEachShard(func(name string, shard ShardLike) error {
		shardName = shard.Name()
		return nil
	})
	return shardName
}

func setupTestShardWithSettings(t *testing.T, ctx context.Context, class *models.Class,
	vic schemaConfig.VectorIndexConfig, withStopwords, withCheckpoints, multiTenant bool, indexOpts ...func(*Index),
) (ShardLike, *Index) {
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	maxResults := int64(10_000)

	var shardState *sharding.State
	if multiTenant {
		shardState = NewMultiTenantShardingStateBuilder().
			WithIndexName("multi-tenant-index").
			WithNodePrefix("node").
			WithReplicationFactor(1).
			WithTenant("foo-tenant", "HOT").
			Build()
	} else {
		shardState = singleShardState()
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlyClass(mock.Anything).RunAndReturn(func(name string) *models.Class {
		return &models.Class{Class: name}
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  tmpDir,
		QueryMaximumResults:       maxResults,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, &FakeNodeResolver{}, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
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

	metrics, err := NewMetrics(logger, nil, class.Class, "")
	require.NoError(t, err)

	idx := &Index{
		Config: IndexConfig{
			RootPath:            tmpDir,
			ClassName:           schema.ClassName(class.Class),
			QueryMaximumResults: maxResults,
			ReplicationFactor:   1,
		},
		metrics:                metrics,
		partitioningEnabled:    shardState.PartitioningEnabled,
		invertedIndexConfig:    iic,
		vectorIndexUserConfig:  vic,
		vectorIndexUserConfigs: map[string]schemaConfig.VectorIndexConfig{},
		logger:                 logger,
		getSchema:              schemaGetter,
		schemaReader:           mockSchemaReader,
		centralJobQueue:        repo.jobQueueCh,
		stopwords:              sd,
		indexCheckpoints:       checkpts,
		allocChecker:           memwatch.NewDummyMonitor(),
		shardCreateLocks:       esync.NewKeyRWLocker(),
		backupLock:             esync.NewKeyRWLocker(),
		scheduler:              repo.scheduler,
		shardLoadLimiter:       loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
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

// Simplified functions that delegate to the common helper
func testShardWithMultiTenantSettings(t *testing.T, ctx context.Context, class *models.Class,
	vic schemaConfig.VectorIndexConfig, withStopwords, withCheckpoints bool, indexOpts ...func(*Index),
) (ShardLike, *Index) {
	return setupTestShardWithSettings(t, ctx, class, vic, withStopwords, withCheckpoints, true, indexOpts...)
}

func testShardWithSettings(t *testing.T, ctx context.Context, class *models.Class,
	vic schemaConfig.VectorIndexConfig, withStopwords, withCheckpoints bool, indexOpts ...func(*Index),
) (ShardLike, *Index) {
	return setupTestShardWithSettings(t, ctx, class, vic, withStopwords, withCheckpoints, false, indexOpts...)
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

func createRandomMultiVectorObjects(r *rand.Rand, className string, numObj int, numTokens int, vectorDim int) []*storobj.Object {
	obj := make([]*storobj.Object, numObj)

	for i := 0; i < numObj; i++ {
		obj[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
			},
			MultiVectors: make(map[string][][]float32),
		}

		for t := 0; t < numTokens; t++ {
			obj[i].MultiVectors["default"] = make([][]float32, vectorDim)
			for d := 0; d < vectorDim; d++ {
				obj[i].MultiVectors["default"][d] = make([]float32, vectorDim)
				for d2 := 0; d2 < vectorDim; d2++ {
					obj[i].MultiVectors["default"][d][d2] = r.Float32()
				}
			}
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
