package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestShardShutdownWithInUseRefTimeout(t *testing.T) {

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
			err := repo.PutObject(context.Background(), obj, vec, nil, nil, 0)
			require.Nil(t, err)
		}
		dimAfter := GetDimensionsFromRepo(context.Background(), repo, "Test")
		require.Equal(t, 12800, dimAfter, "dimensions should not have changed")
		quantDimAfter := GetQuantizedDimensionsFromRepo(context.Background(), repo, "Test", 64)
		require.Equal(t, 6400, quantDimAfter, "quantized dimensions should not have changed")
	})

	require.Nil(t, err)
	index := repo.GetIndex(schema.ClassName("Test"))
	var testShard string
	index.shards.Range(func(name string, shard ShardLike) error {
		testShard = name
		return nil
	})
	t.Logf("testShard: %s", testShard)
	shard, release, err := index.GetShard(context.TODO(), testShard)
	require.Nil(t, err)
	defer release()
	require.NotNil(t, shard)

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()

	shutdownErrCh := make(chan error)
	go func() {
		err := shard.Shutdown(ctx)
		shutdownErrCh <- err
	}()

	fmt.Printf("Waiting for shutdown to complete at %v\n", time.Now())
	// Wait for 33s which should be long enough for the shutdown timeout (30s)
	time.Sleep(33 * time.Second)
	fmt.Printf("Shutdown timeout reached, waiting for shutdown to complete...\n")

	select {
	case err := <-shutdownErrCh:
		fmt.Printf("Shutdown completed at %v\n", time.Now())
		fmt.Printf("Shutdown error: %v\n", err)
		fmt.Printf("Shard state: %v\n", shard.(*LazyLoadShard).shard.shut)
		require.NoError(t, err)
		require.True(t, shard.(*LazyLoadShard).shard.shut, "shard should be marked as shut down due to timeout")
	case <-time.After(5 * time.Second):
		fmt.Printf("Shutdown did not complete in time after timeout\n")
		// This is a failure case, we expect the shutdown to complete
		t.Fatal("shutdown did not complete in time after timeout")
	}
	fmt.Println("Shutdown test completed successfully")
}
