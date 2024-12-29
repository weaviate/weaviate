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

package db

import (
	"context"
	"fmt"
	"log"
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
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, 0, dimAfter, "dimensions should not have been calculated")
	})

	dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
	require.Equal(t, 0, dimBefore, "dimensions should not have been calculated")
	repo.config.TrackVectorDimensions = true
	migrator.RecalculateVectorDimensions(context.TODO())
	dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
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
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
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
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, 12800, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, 6400, quantDimAfter, "quantized dimensions should not have changed")
	})

	t.Run("import objects with d=0", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		for i := 100; i < 200; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			err := repo.PutObject(context.Background(), obj, nil, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, dimBefore, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, quantDimBefore, quantDimAfter, "quantized dimensions should not have changed")
	})

	t.Run("verify dimensions after initial import", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 12800, shard.Dimensions(context.Background()))
			assert.Equal(t, 6400, shard.QuantizedDimensions(context.Background(), 64))
			return nil
		})
	})

	t.Run("delete 10 objects with d=128", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		for i := 0; i < 10; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			err := repo.DeleteObject(context.Background(), "Test", id, time.Now(), nil, "", 0)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, dimBefore, dimAfter+10*128, "dimensions should have decreased")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, quantDimBefore, quantDimAfter+10*64, "dimensions should have decreased")
	})

	t.Run("verify dimensions after delete", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 11520, shard.Dimensions(context.Background()))
			assert.Equal(t, 5760, shard.QuantizedDimensions(context.Background(), 64))
			return nil
		})
	})

	t.Run("update some of the d=128 objects with a new vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
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
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, dimBefore+10*128, dimAfter, "dimensions should have been restored")
		require.Equal(t, quantDimBefore+10*64, quantDimAfter, "dimensions should have been restored")
	})

	t.Run("update some of the d=128 objects with a nil vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 32)
		for i := 50; i < 100; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinsert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, nil, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 32)
		require.Equal(t, dimBefore, dimAfter+50*128, "dimensions should decrease")
		require.Equal(t, quantDimBefore, quantDimAfter+50*32, "dimensions should decrease")
	})

	t.Run("verify dimensions after first set of updates", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 6400, shard.Dimensions(context.Background()))
			assert.Equal(t, 3200, shard.QuantizedDimensions(context.Background(), 64))
			assert.Equal(t, 1600, shard.QuantizedDimensions(context.Background(), 32))
			assert.Equal(t, 3200, shard.QuantizedDimensions(context.Background(), 0))
			return nil
		})
	})

	t.Run("update some of the origin nil vector objects with a d=128 vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
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
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, dimBefore+50*128, dimAfter, "dimensions should increase")
		require.Equal(t, quantDimBefore+50*64, quantDimAfter, "dimensions should increase")
	})

	t.Run("update some of the nil objects with another nil vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		for i := 150; i < 200; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinstert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, nil, nil, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, dimBefore, dimAfter, "dimensions should not have changed")
		require.Equal(t, quantDimBefore, quantDimAfter, "dimensions should not have changed")
	})

	t.Run("verify dimensions after more updates", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 12800, shard.Dimensions(context.Background()))
			assert.Equal(t, 6400, shard.QuantizedDimensions(context.Background(), 64))
			assert.Equal(t, 3200, shard.QuantizedDimensions(context.Background(), 32))
			// segments = 0, will use 128/2 = 64 segments and so value should be 6400
			assert.Equal(t, 6400, shard.QuantizedDimensions(context.Background(), 0))
			return nil
		})
	})
}

func publishDimensionMetricsFromRepo(ctx context.Context, repo *DB, className string) {
	if !repo.config.TrackVectorDimensions {
		log.Printf("Vector dimensions tracking is disabled, returning 0")
		return
	}
	index := repo.GetIndex(schema.ClassName(className))
	index.ForEachShard(func(name string, shard ShardLike) error {
		shard.publishDimensionMetrics(ctx)
		return nil
	})
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

func Test_DimensionTrackingMetrics(t *testing.T) {
	type testConfig struct {
		name            string
		className       string
		vectorConfig    func() *enthnsw.UserConfig
		expectedDims    float64
		expectedSegs    float64
		importDimension int
	}

	tests := []testConfig{
		{
			name:      "HNSW",
			className: "HNSW",
			vectorConfig: func() *enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				return &cfg
			},
			expectedDims:    6400.0, // 100 objects * 64 dimensions
			expectedSegs:    0.0,
			importDimension: 64,
		},
		{
			name:      "BQ",
			className: "BQ",
			vectorConfig: func() *enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.BQ.Enabled = true
				return &cfg
			},
			expectedDims:    0.0,
			expectedSegs:    800.0, // 100 objects * 64 dimensions / 8 for BQ
			importDimension: 64,
		},
		{
			name:      "PQ",
			className: "PQ",
			vectorConfig: func() *enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.PQ.Enabled = true
				cfg.PQ.Segments = 10
				return &cfg
			},
			expectedDims:    0.0,
			expectedSegs:    1000.0, // 100 objects * 10 segments
			importDimension: 64,
		},
		{
			name:      "PQ with zero segments",
			className: "PQZ",
			vectorConfig: func() *enthnsw.UserConfig {
				cfg := enthnsw.NewDefaultUserConfig()
				cfg.PQ.Enabled = true
				cfg.PQ.Segments = 0
				return &cfg
			},
			expectedDims:    0.0,
			expectedSegs:    3200.0, // 100 objects * 32 segments (segments = 64/2 due to auto calculation)
			importDimension: 64,
		},
	}

	r := getRandomSeed()
	dirName := t.TempDir()
	var shardName string

	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	metrics := monitoring.GetMetrics()
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, metrics, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	defer repo.Shutdown(context.Background())

	migrator := NewMigrator(repo, logger)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Set schema
			t.Run("set schema", func(t *testing.T) {
				class := &models.Class{
					Class:             tc.className,
					VectorIndexConfig: *tc.vectorConfig(),
					VectorConfig: map[string]models.VectorConfig{
						"vec": {
							VectorIndexConfig: *tc.vectorConfig(),
						},
					},
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

			// Import objects and validate metrics
			t.Run("import objects and validate metrics", func(t *testing.T) {
				for i := 0; i < 100; i++ {
					vec := make([]float32, tc.importDimension)
					for j := range vec {
						vec[j] = r.Float32()
					}

					id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
					obj := &models.Object{Class: tc.className, ID: id}
					namedVectors := models.Vectors{
						"vec": vec,
					}
					err := repo.PutObject(context.Background(), obj, vec, namedVectors, nil, nil, 0)
					require.Nil(t, err)
				}

				publishDimensionMetricsFromRepo(context.Background(), repo, tc.className)

				shardName = getSingleShardNameFromRepo(repo, tc.className)

				// Check dimensions metric
				metric, err := metrics.VectorDimensionsSum.GetMetricWithLabelValues(tc.className, shardName)
				require.Nil(t, err)
				metricValue := testutil.ToFloat64(metric)
				require.Equal(t, tc.expectedDims, metricValue, "dimensions should match expected value")

				// Check segments metric
				metric, err = metrics.VectorSegmentsSum.GetMetricWithLabelValues(tc.className, shardName)
				require.Nil(t, err)
				metricValue = testutil.ToFloat64(metric)
				require.Equal(t, tc.expectedSegs, metricValue, "segments should match expected value")

				// Check named dimensions metric
				metric, err = metrics.VectorDimensionsSumByVector.GetMetricWithLabelValues(tc.className, shardName, "vec")
				require.Nil(t, err)
				metricValue = testutil.ToFloat64(metric)
				require.Equal(t, tc.expectedDims, metricValue, "named dimensions should match expected value")

				// Check named segments metric
				metric, err = metrics.VectorSegmentsSumByVector.GetMetricWithLabelValues(tc.className, shardName, "vec")
				require.Nil(t, err)
				metricValue = testutil.ToFloat64(metric)
				require.Equal(t, tc.expectedSegs, metricValue, "named segments should match expected value")
			})

			// Delete class and verify metrics are reset
			t.Run("delete class", func(t *testing.T) {
				err := migrator.DropClass(context.Background(), tc.className, false)
				require.Nil(t, err)

				// Verify dimensions metric is reset
				metric, err := metrics.VectorDimensionsSum.GetMetricWithLabelValues(tc.className, shardName)
				require.Nil(t, err)
				metricValue := testutil.ToFloat64(metric)
				require.Equal(t, 0.0, metricValue, "dimensions metric should be reset")

				// Verify segments metric is reset
				metric, err = metrics.VectorSegmentsSum.GetMetricWithLabelValues(tc.className, shardName)
				require.Nil(t, err)
				metricValue = testutil.ToFloat64(metric)
				require.Equal(t, 0.0, metricValue, "segments metric should be reset")

				// Verify dimensions metric is reset
				metric, err = metrics.VectorDimensionsSumByVector.GetMetricWithLabelValues(tc.className, shardName, "vec")
				require.Nil(t, err)
				metricValue = testutil.ToFloat64(metric)
				require.Equal(t, 0.0, metricValue, "named dimensions metric should be reset")

				// Verify segments metric is reset
				metric, err = metrics.VectorSegmentsSumByVector.GetMetricWithLabelValues(tc.className, shardName, "vec")
				require.Nil(t, err)
				metricValue = testutil.ToFloat64(metric)
				require.Equal(t, 0.0, metricValue, "named segments metric should be reset")
			})
		})
	}
}
