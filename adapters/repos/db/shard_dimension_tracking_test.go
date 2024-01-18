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
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
			}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
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
				err := repo.PutObject(context.Background(), obj, vec, nil)
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
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		require.Equal(t, 0, dimAfter, "dimensions should not have been calculated")
	})

	dimBefore := GetDimensionsFromRepo(repo, "Test")
	require.Equal(t, 0, dimBefore, "dimensions should not have been calculated")
	repo.config.TrackVectorDimensions = true
	migrator.RecalculateVectorDimensions(context.TODO())
	dimAfter := GetDimensionsFromRepo(repo, "Test")
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
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		require.Equal(t, 12800, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		require.Equal(t, 6400, quantDimAfter, "quantized dimensions should not have changed")
	})

	t.Run("import objects with d=0", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		for i := 100; i < 200; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			err := repo.PutObject(context.Background(), obj, nil, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		require.Equal(t, dimBefore, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		require.Equal(t, quantDimBefore, quantDimAfter, "quantized dimensions should not have changed")
	})

	t.Run("verify dimensions after initial import", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 12800, shard.Dimensions())
			assert.Equal(t, 6400, shard.QuantizedDimensions(64))
			return nil
		})
	})

	t.Run("delete 10 objects with d=128", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		for i := 0; i < 10; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			err := repo.DeleteObject(context.Background(), "Test", id, nil, "")
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		require.Equal(t, dimBefore, dimAfter+10*128, "dimensions should have decreased")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		require.Equal(t, quantDimBefore, quantDimAfter+10*64, "dimensions should have decreased")
	})

	t.Run("verify dimensions after delete", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 11520, shard.Dimensions())
			assert.Equal(t, 5760, shard.QuantizedDimensions(64))
			return nil
		})
	})

	t.Run("update some of the d=128 objects with a new vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		require.Equal(t, dimBefore+10*128, dimAfter, "dimensions should have been restored")
		require.Equal(t, quantDimBefore+10*64, quantDimAfter, "dimensions should have been restored")
	})

	t.Run("update some of the d=128 objects with a nil vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(repo, "Test", 32)
		for i := 50; i < 100; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinsert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, nil, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 32)
		require.Equal(t, dimBefore, dimAfter+50*128, "dimensions should decrease")
		require.Equal(t, quantDimBefore, quantDimAfter+50*32, "dimensions should decrease")
	})

	t.Run("verify dimensions after first set of updates", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 6400, shard.Dimensions())
			assert.Equal(t, 3200, shard.QuantizedDimensions(64))
			assert.Equal(t, 1600, shard.QuantizedDimensions(32))
			return nil
		})
	})

	t.Run("update some of the origin nil vector objects with a d=128 vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
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
			err := repo.PutObject(context.Background(), obj, vec, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		require.Equal(t, dimBefore+50*128, dimAfter, "dimensions should increase")
		require.Equal(t, quantDimBefore+50*64, quantDimAfter, "dimensions should increase")
	})

	t.Run("update some of the nil objects with another nil vector", func(t *testing.T) {
		dimBefore := GetDimensionsFromRepo(repo, "Test")
		quantDimBefore := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		for i := 150; i < 200; i++ {
			id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
			obj := &models.Object{Class: "Test", ID: id}
			// Put is idempotent, but since the IDs exist now, this is an update
			// under the hood and a "reinstert" for the already deleted ones
			err := repo.PutObject(context.Background(), obj, nil, nil)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(repo, "Test")
		quantDimAfter := GetQuantizedDimensionsFromRepo(repo, "Test", 64)
		require.Equal(t, dimBefore, dimAfter, "dimensions should not have changed")
		require.Equal(t, quantDimBefore, quantDimAfter, "dimensions should not have changed")
	})

	t.Run("verify dimensions after more updates", func(t *testing.T) {
		idx := repo.GetIndex("Test")
		idx.ForEachShard(func(name string, shard ShardLike) error {
			assert.Equal(t, 12800, shard.Dimensions())
			assert.Equal(t, 6400, shard.QuantizedDimensions(64))
			assert.Equal(t, 12800, shard.QuantizedDimensions(0))
			return nil
		})
	})
}
