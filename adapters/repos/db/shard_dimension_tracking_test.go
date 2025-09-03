//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func Benchmark_Migration(b *testing.B) {
	fmt.Printf("Running benchmark %v times\n", b.N)
	for i := 0; i < b.N; i++ {
		func() {
			r := getRandomSeed()
			dirName := b.TempDir()

			shardState := singleShardState()
			logger := logrus.New()
			schemaGetter := &fakeSchemaGetter{
				schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
				shardState: shardState,
			}
			repo, err := New(logger, Config{
				RootPath:                  dirName,
				QueryMaximumResults:       1000,
				MaxImportGoroutinesFactor: 1,
				TrackVectorDimensions:     true,
			}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
			require.Nil(b, err)
			repo.SetSchemaGetter(schemaGetter)
			require.Nil(b, repo.WaitForStartup(testCtx()))
			defer repo.Shutdown(context.Background())

			migrator := NewMigrator(repo, logger)

			class := &models.Class{
				Class:               "Test",
				VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
				InvertedIndexConfig: invertedConfig(),
			}
			schema := schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{class},
				},
			}

			migrator.AddClass(context.Background(), class, schemaGetter.shardState)

			schemaGetter.schema = schema

			repo.config.TrackVectorDimensions = false

			dim := 128
			for i := 0; i < 100; i++ {
				vec := make([]float32, dim)
				for j := range vec {
					vec[j] = r.Float32()
				}

				id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
				obj := &models.Object{Class: "Test", ID: id}
				err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
				if err != nil {
					b.Fatal(err)
				}
			}

			fmt.Printf("Added vectors, now migrating\n")

			repo.config.TrackVectorDimensions = true
			migrator.RecalculateVectorDimensions(context.TODO())
			fmt.Printf("Benchmark complete")
		}()
	}
}

// Rebuild dimensions at startup
func Test_Migration(t *testing.T) {
	r := getRandomSeed()
	dirName := t.TempDir()

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       1000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)

	t.Run("set schema", func(t *testing.T) {
		class := &models.Class{
			Class:               "Test",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
		schema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		}

		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))

		schemaGetter.schema = schema
	})

	repo.config.TrackVectorDimensions = false

	t.Run("import objects with d=128", func(t *testing.T) {
		dim := 128
		for i := 0; i < 100; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = r.Float32()
			}

			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, 0, dimAfter, "dimensions should not have been calculated")
	})

	dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
	require.Equal(t, 0, dimBefore, "dimensions should not have been calculated")
	repo.config.TrackVectorDimensions = true
	migrator.RecalculateVectorDimensions(context.TODO())
	dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
	require.Equal(t, 12800, dimAfter, "dimensions should be counted now")
}

func Test_DimensionTracking(t *testing.T) {
	r := getRandomSeed()
	dirName := t.TempDir()

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, monitoring.GetMetrics(), memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)

	t.Run("set schema", func(t *testing.T) {
		class := &models.Class{
			Class:               "Test",
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
		schema := schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{class},
			},
		}

		require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))

		schemaGetter.schema = schema
	})

	t.Run("import objects with d=128", func(t *testing.T) {
		dim := 128
		for i := 0; i < 100; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = r.Float32()
			}

			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, 12800, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, 6400, quantDimAfter, "quantized dimensions should not have changed")
	})

	t.Run("import objects with d=0", func(t *testing.T) {
		dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		for i := 100; i < 200; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			err := repo.PutObject(context.Background(), obj, nil, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, dimBefore, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, quantDimBefore, quantDimAfter, "quantized dimensions should not have changed")
	})

	t.Run("verify dimensions after initial import", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			dim, err := shard.Dimensions(context.Background(), "")
			assert.NoError(t, err)
			assert.Equal(t, 12800, dim)
			assert.Equal(t, 6400, shard.QuantizedDimensions(context.Background(), "", 64))
			return nil
		})
	})

	t.Run("delete 10 objects with d=128", func(t *testing.T) {
		dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		for i := 0; i < 10; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			err := repo.DeleteObject(context.Background(), "Test", id, time.Now(), nil, "", 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, dimBefore, dimAfter+10*128, "dimensions should have decreased")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, quantDimBefore, quantDimAfter+10*64, "dimensions should have decreased")
	})

	t.Run("verify dimensions after delete", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			dim, err := shard.Dimensions(context.Background(), "")
			assert.NoError(t, err)
			assert.Equal(t, 11520, dim)
			assert.Equal(t, 5760, shard.QuantizedDimensions(context.Background(), "", 64))
			return nil
		})
	})

	t.Run("update some of the d=128 objects with a new vector", func(t *testing.T) {
		dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		dim := 128
		for i := 0; i < 50; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = rand.Float32()
			}

			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinstert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, dimBefore+10*128, dimAfter, "dimensions should have been restored")
		require.Equal(t, quantDimBefore+10*64, quantDimAfter, "dimensions should have been restored")
	})

	t.Run("update some of the d=128 objects with a nil vector", func(t *testing.T) {
		dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 32)
		for i := 50; i < 100; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinsert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, nil, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 32)
		require.Equal(t, dimBefore, dimAfter+50*128, "dimensions should decrease")
		require.Equal(t, quantDimBefore, quantDimAfter+50*32, "dimensions should decrease")
	})

	t.Run("verify dimensions after first set of updates", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			dim, err := shard.Dimensions(context.Background(), "")
			assert.NoError(t, err)
			assert.Equal(t, 6400, dim)
			assert.Equal(t, 3200, shard.QuantizedDimensions(context.Background(), "", 64))
			assert.Equal(t, 1600, shard.QuantizedDimensions(context.Background(), "", 32))
			assert.Equal(t, 3200, shard.QuantizedDimensions(context.Background(), "", 0))
			return nil
		})
	})

	t.Run("update some of the origin nil vector objects with a d=128 vector", func(t *testing.T) {
		dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		dim := 128
		for i := 100; i < 150; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = rand.Float32()
			}

			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinsert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, dimBefore+50*128, dimAfter, "dimensions should increase")
		require.Equal(t, quantDimBefore+50*64, quantDimAfter, "dimensions should increase")
	})

	t.Run("update some of the nil objects with another nil vector", func(t *testing.T) {
		dimBefore := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		for i := 150; i < 200; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinstert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, nil, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := getDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, dimBefore, dimAfter, "dimensions should not have changed")
		require.Equal(t, quantDimBefore, quantDimAfter, "dimensions should not have changed")
	})

	t.Run("verify dimensions after more updates", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			dim, err := shard.Dimensions(context.Background(), "")
			assert.NoError(t, err)
			assert.Equal(t, 12800, dim)
			assert.Equal(t, 6400, shard.QuantizedDimensions(context.Background(), "", 64))
			assert.Equal(t, 3200, shard.QuantizedDimensions(context.Background(), "", 32))
			// segments = 0, will use 128/2 = 64 segments and so value should be 6400
			assert.Equal(t, 6400, shard.QuantizedDimensions(context.Background(), "", 0))
			return nil
		})
	})
}

func TestTotalDimensionTrackingMetrics(t *testing.T) {
	const (
		objectCount         = 100
		multiVecCard        = 3
		dimensionsPerVector = 64
	)

	for _, tt := range []struct {
		name              string
		vectorConfig      func() enthnsw.UserConfig
		namedVectorConfig func() enthnsw.UserConfig
		multiVectorConfig func() enthnsw.UserConfig

		expectDimensions float64
		expectSegments   float64
	}{
		{
			name:         "legacy",
			vectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: dimensionsPerVector * objectCount,
		},
		{
			name:              "named",
			namedVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: dimensionsPerVector * objectCount,
		},
		{
			name:              "multi",
			multiVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: multiVecCard * dimensionsPerVector * objectCount,
		},
		{
			name:              "mixed",
			vectorConfig:      func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },
			namedVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },

			expectDimensions: 2 * dimensionsPerVector * objectCount,
		},
		{
			name: "named_with_bq",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.BQ.Enabled = true
				return cfg
			},

			expectSegments: (dimensionsPerVector / 8) * objectCount,
		},
		{
			name: "named_with_pq",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.PQ.Enabled = true
				cfg.PQ.Segments = 10
				return cfg
			},

			expectSegments: 10 * objectCount,
		},
		{
			name: "named_with_pq_zero_segments",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.PQ.Enabled = true
				return cfg
			},
			expectSegments: (dimensionsPerVector / 2) * objectCount,
		},
		{
			name: "multi_and_bq_named",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.BQ.Enabled = true
				return cfg
			},
			multiVectorConfig: func() enthnsw.UserConfig { return enthnsw.NewDefaultUserConfig() },
			expectDimensions:  multiVecCard * dimensionsPerVector * objectCount,
			expectSegments:    (dimensionsPerVector / 8) * objectCount,
		},
		{
			name: "named_with_rq_8bit",
			namedVectorConfig: func() enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.RQ.Enabled = true
				cfg.RQ.Bits = 8
				return cfg
			},

			expectDimensions: dimensionsPerVector * objectCount,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var (
				class = &models.Class{
					Class:               tt.name,
					InvertedIndexConfig: invertedConfig(),
					VectorConfig:        map[string]models.VectorConfig{},
				}

				namedVectorName = "namedVector"
				multiVectorName = "multiVector"

				legacyVec []float32
				namedVecs map[string][]float32
				multiVecs map[string][][]float32
			)

			if tt.vectorConfig != nil {
				class.VectorIndexConfig = tt.vectorConfig()
				legacyVec = randVector(dimensionsPerVector)
			}

			if tt.namedVectorConfig != nil {
				class.VectorConfig[namedVectorName] = models.VectorConfig{
					VectorIndexConfig: tt.namedVectorConfig(),
				}
				namedVecs = map[string][]float32{
					namedVectorName: randVector(dimensionsPerVector),
				}
			}

			if tt.multiVectorConfig != nil {
				config := tt.multiVectorConfig()
				config.Multivector = enthnsw.MultivectorConfig{Enabled: true}
				class.VectorConfig[multiVectorName] = models.VectorConfig{
					VectorIndexConfig: config,
				}

				multiVecs = map[string][][]float32{}
				for range multiVecCard {
					multiVecs[multiVectorName] = append(multiVecs[multiVectorName], randVector(dimensionsPerVector))
				}
			}

			var (
				db        = createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
				shardName = getSingleShardNameFromRepo(db, class.Class)

				insertData = func() {
					for i := range objectCount {
						obj := &models.Object{
							Class: tt.name,
							ID:    intToUUID(i),
						}
						err := db.PutObject(context.Background(), obj, legacyVec, namedVecs, multiVecs, nil, 0)
						require.Nil(t, err)
					}
					publishVectorMetricsFromDB(t, db)
				}

				removeData = func() {
					for i := range objectCount {
						err := db.DeleteObject(context.Background(), class.Class, intToUUID(i), time.Now(), nil, "", 0)
						require.NoError(t, err)
					}
					publishVectorMetricsFromDB(t, db)
				}

				assertTotalMetrics = func(expectDims, expectSegs float64) {
					metrics := monitoring.GetMetrics()
					metric, err := metrics.VectorDimensionsSum.GetMetricWithLabelValues(class.Class, shardName)
					require.NoError(t, err)
					require.Equal(t, expectDims, testutil.ToFloat64(metric))

					metric, err = metrics.VectorSegmentsSum.GetMetricWithLabelValues(class.Class, shardName)
					require.NoError(t, err)
					require.Equal(t, expectSegs, testutil.ToFloat64(metric))
				}
			)

			insertData()
			assertTotalMetrics(tt.expectDimensions, tt.expectSegments)
			removeData()
			assertTotalMetrics(0, 0)
			insertData()
			assertTotalMetrics(tt.expectDimensions, tt.expectSegments)
			require.NoError(t, db.DeleteIndex(schema.ClassName(class.Class)))
			assertTotalMetrics(0, 0)
		})
	}
}

func intToUUID(i int) strfmt.UUID {
	return strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
}

func TestDimensionTrackingWithGrouping(t *testing.T) {
	const (
		nClasses          = 2
		shardsPerClass    = 1 // createTestDatabaseWithClass does not support multi-tenancy
		objectCount       = 5
		dimPerVector      = 64
		expectDimPerShard = objectCount * dimPerVector
		expectTotalDim    = nClasses * shardsPerClass * expectDimPerShard
	)

	testCases := []struct {
		name               string
		groupingEnabled    bool
		expectedLabels     []string // class-shard label pairs
		expectedDimensions []int    // expectedDimensions for a label pair
	}{
		{
			name:               "with_grouping_enabled",
			groupingEnabled:    true,
			expectedLabels:     []string{"n/a", "n/a"},
			expectedDimensions: []int{expectTotalDim},
		},
		{
			name:            "with_grouping_disabled",
			groupingEnabled: false,
			// Will be set dynamically
			expectedLabels:     nil,
			expectedDimensions: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup metrics grouping (copy to avoid race condition)
			metrics := *monitoring.GetMetrics()
			metrics.Group = tc.groupingEnabled

			// Create test class and database
			classes := make([]*models.Class, nClasses)
			for i := range classes {
				classes[i] = &models.Class{
					Class:               fmt.Sprintf("%s_%d", tc.name, i),
					VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
					InvertedIndexConfig: invertedConfig(),
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled:              shardsPerClass > 1,
						AutoTenantCreation:   true,
						AutoTenantActivation: true,
					},
				}
			}

			db := createTestDatabaseWithClass(t, &metrics, classes...)

			// Insert test data
			for _, class := range classes {
				for range shardsPerClass {
					shardName := getSingleShardNameFromRepo(db, class.Class)

					for i := range objectCount {
						obj := &models.Object{
							Class: class.Class,
							ID:    intToUUID(i),
						}

						if shardsPerClass > 1 {
							obj.Tenant = shardName
						}

						vec := randVector(dimPerVector)
						err := db.PutObject(context.Background(), obj, vec, nil, nil, nil, 0)
						require.NoError(t, err, "put object")
					}

					// Set expected labels for non-grouping case
					if !tc.groupingEnabled {
						tc.expectedLabels = append(tc.expectedLabels, class.Class, shardName)
						tc.expectedDimensions = append(tc.expectedDimensions, expectDimPerShard)
					}
				}
			}

			// Publish metrics
			publishVectorMetricsFromDB(t, db)

			// Check expected dimensions for each pair of labels
			for i := 0; i < len(tc.expectedLabels); i += 2 {
				className, shardName := tc.expectedLabels[i], tc.expectedLabels[i+1]

				// Verify dimension metrics
				dim, err := metrics.VectorDimensionsSum.GetMetricWithLabelValues(className, shardName)
				require.NoError(t, err, "get vector_dimensions_sum metric")
				require.Equal(t, float64(tc.expectedDimensions[0]), testutil.ToFloat64(dim),
					"vector_dimensions_sum{class=%s,shard=%s}", className, shardName)

				// Verify segment metrics (should be 0 for standard vectors)
				segments, err := metrics.VectorSegmentsSum.GetMetricWithLabelValues(className, shardName)
				require.NoError(t, err, "get vector_segments_sum metric")
				require.Equal(t, float64(0), testutil.ToFloat64(segments),
					"vector_segments_sum{class=%s,shard=%s}", className, shardName)
			}
		})
	}
}
