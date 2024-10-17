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
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestIndex_DropIndex(t *testing.T) {
	dirName := t.TempDir()
	class := &models.Class{Class: "deletetest"}
	index := emptyIdx(t, dirName, class)

	indexFilesBeforeDelete, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	err = index.drop()
	require.Nil(t, err)

	indexFilesAfterDelete, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	assert.Equal(t, 5, len(indexFilesBeforeDelete))
	assert.Equal(t, 0, len(indexFilesAfterDelete))
}

func TestIndex_DropEmptyAndRecreateEmptyIndex(t *testing.T) {
	dirName := t.TempDir()
	class := &models.Class{Class: "deletetest"}
	index := emptyIdx(t, dirName, class)

	indexFilesBeforeDelete, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	// drop the index
	err = index.drop()
	require.Nil(t, err)

	indexFilesAfterDelete, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	index = emptyIdx(t, dirName, class)

	indexFilesAfterRecreate, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	assert.Equal(t, 5, len(indexFilesBeforeDelete))
	assert.Equal(t, 0, len(indexFilesAfterDelete))
	assert.Equal(t, 5, len(indexFilesAfterRecreate))

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_DropWithDataAndRecreateWithDataIndex(t *testing.T) {
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		Class: "deletetest",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				class,
			},
		},
	}
	// create index with data
	shardState := singleShardState()
	index, err := NewIndex(testCtx(), IndexConfig{
		RootPath:          dirName,
		ClassName:         schema.ClassName(class.Class),
		ReplicationFactor: NewAtomicInt64(1),
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, &fakeSchemaGetter{
			schema: fakeSchema, shardState: shardState,
		}, nil, logger, nil, nil, nil, nil, class, nil, nil, nil)
	require.Nil(t, err)

	productsIds := []strfmt.UUID{
		"1295c052-263d-4aae-99dd-920c5a370d06",
		"1295c052-263d-4aae-99dd-920c5a370d07",
	}

	products := []map[string]interface{}{
		{"name": "one"},
		{"name": "two"},
	}

	err = index.addProperty(context.TODO(), &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	require.Nil(t, err)

	for i, p := range products {
		product := models.Object{
			Class:      class.Class,
			ID:         productsIds[i],
			Properties: p,
		}

		err := index.putObject(context.TODO(), storobj.FromObject(
			&product, []float32{0.1, 0.2, 0.01, 0.2}, nil), nil, 0)
		require.Nil(t, err)
	}

	indexFilesBeforeDelete, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	beforeDeleteObj1, err := index.objectByID(context.TODO(),
		productsIds[0], nil, additional.Properties{}, nil, "")
	require.Nil(t, err)

	beforeDeleteObj2, err := index.objectByID(context.TODO(),
		productsIds[1], nil, additional.Properties{}, nil, "")
	require.Nil(t, err)

	// drop the index
	err = index.drop()
	require.Nil(t, err)

	indexFilesAfterDelete, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	// recreate the index
	index, err = NewIndex(testCtx(), IndexConfig{
		RootPath:          dirName,
		ClassName:         schema.ClassName(class.Class),
		ReplicationFactor: NewAtomicInt64(1),
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, &fakeSchemaGetter{
			schema:     fakeSchema,
			shardState: shardState,
		}, nil, logger, nil, nil, nil, nil, class, nil, nil, nil)
	require.Nil(t, err)

	err = index.addProperty(context.TODO(), &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	require.Nil(t, err)

	indexFilesAfterRecreate, err := getIndexFilenames(dirName, class.Class)
	require.Nil(t, err)

	afterRecreateObj1, err := index.objectByID(context.TODO(),
		productsIds[0], nil, additional.Properties{}, nil, "")
	require.Nil(t, err)

	afterRecreateObj2, err := index.objectByID(context.TODO(),
		productsIds[1], nil, additional.Properties{}, nil, "")
	require.Nil(t, err)

	// insert some data in the recreated index
	for i, p := range products {
		thing := models.Object{
			Class:      class.Class,
			ID:         productsIds[i],
			Properties: p,
		}

		err := index.putObject(context.TODO(), storobj.FromObject(
			&thing, []float32{0.1, 0.2, 0.01, 0.2}, nil), nil, 0)
		require.Nil(t, err)
	}

	afterRecreateAndInsertObj1, err := index.objectByID(context.TODO(),
		productsIds[0], nil, additional.Properties{}, nil, "")
	require.Nil(t, err)

	afterRecreateAndInsertObj2, err := index.objectByID(context.TODO(),
		productsIds[1], nil, additional.Properties{}, nil, "")
	require.Nil(t, err)

	// update the index vectorIndexUserConfig
	beforeVectorConfig, ok := index.vectorIndexUserConfig.(hnsw.UserConfig)
	require.Equal(t, -1, beforeVectorConfig.EF)
	require.True(t, ok)
	beforeVectorConfig.EF = 99
	err = index.updateVectorIndexConfig(context.TODO(), beforeVectorConfig)
	require.Nil(t, err)
	afterVectorConfig, ok := index.vectorIndexUserConfig.(hnsw.UserConfig)
	require.True(t, ok)
	require.Equal(t, 99, afterVectorConfig.EF)

	assert.Equal(t, 5, len(indexFilesBeforeDelete))
	assert.Equal(t, 0, len(indexFilesAfterDelete))
	assert.Equal(t, 5, len(indexFilesAfterRecreate))
	assert.Equal(t, indexFilesBeforeDelete, indexFilesAfterRecreate)
	assert.NotNil(t, beforeDeleteObj1)
	assert.NotNil(t, beforeDeleteObj2)
	assert.Empty(t, afterRecreateObj1)
	assert.Empty(t, afterRecreateObj2)
	assert.NotNil(t, afterRecreateAndInsertObj1)
	assert.NotNil(t, afterRecreateAndInsertObj2)
}

func TestIndex_DropReadOnlyEmptyIndex(t *testing.T) {
	ctx := testCtx()
	class := &models.Class{Class: "deletetest"}
	shard, index := testShard(t, ctx, class.Class)

	err := index.updateShardStatus(ctx, shard.Name(), storagestate.StatusReadOnly.String(), 0)
	require.Nil(t, err)

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_DropReadOnlyIndexWithData(t *testing.T) {
	ctx := testCtx()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		Class: "deletetest",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				class,
			},
		},
	}

	shardState := singleShardState()
	index, err := NewIndex(ctx, IndexConfig{
		RootPath:          dirName,
		ClassName:         schema.ClassName(class.Class),
		ReplicationFactor: NewAtomicInt64(1),
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, &fakeSchemaGetter{
			schema: fakeSchema, shardState: shardState,
		}, nil, logger, nil, nil, nil, nil, class, nil, nil, nil)
	require.Nil(t, err)

	productsIds := []strfmt.UUID{
		"1295c052-263d-4aae-99dd-920c5a370d06",
		"1295c052-263d-4aae-99dd-920c5a370d07",
	}

	products := []map[string]interface{}{
		{"name": "one"},
		{"name": "two"},
	}

	err = index.addProperty(ctx, &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	require.Nil(t, err)

	for i, p := range products {
		product := models.Object{
			Class:      class.Class,
			ID:         productsIds[i],
			Properties: p,
		}

		err := index.putObject(ctx, storobj.FromObject(
			&product, []float32{0.1, 0.2, 0.01, 0.2}, nil), nil, 0)
		require.Nil(t, err)
	}

	// set all shards to readonly
	index.ForEachShard(func(name string, shard ShardLike) error {
		err = shard.UpdateStatus(storagestate.StatusReadOnly.String())
		require.Nil(t, err)
		return nil
	})

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_DropUnloadedShard(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		Class: "deletetest",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				class,
			},
		},
	}

	// create a checkpoint file
	cpFile, err := indexcheckpoint.New(dirName, logger)
	require.Nil(t, err)
	defer cpFile.Close()

	// create index
	shardState := singleShardState()
	index, err := NewIndex(testCtx(), IndexConfig{
		RootPath:  dirName,
		ClassName: schema.ClassName(class.Class),
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, &fakeSchemaGetter{
			schema: fakeSchema, shardState: shardState,
		}, nil, logger, nil, nil, nil, nil, class, nil, cpFile, nil)
	require.Nil(t, err)

	// at this point the shard is not loaded yet.
	// update the checkpoint file to simulate a previously loaded shard
	var shardName string
	for name := range shardState.Physical {
		shardName = name
		break
	}
	require.NotEmpty(t, shardName)
	shardID := fmt.Sprintf("%s_%s", index.ID(), shardName)
	err = cpFile.Update(shardID, "", 10)
	require.Nil(t, err)

	// drop the index before loading the shard
	err = index.drop()
	require.Nil(t, err)

	// ensure the checkpoint file is not deleted
	_, err = os.Stat(filepath.Join(dirName, "index.db"))
	require.Nil(t, err)

	// ensure the shard checkpoint is deleted
	v, ok, err := cpFile.Get(shardID, "")
	require.Nil(t, err)
	require.False(t, ok)
	require.Zero(t, v)
}

func TestIndex_DropLoadedShard(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()
	class := &models.Class{
		Class: "deletetest",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				class,
			},
		},
	}

	cpFile, err := indexcheckpoint.New(dirName, logger)
	require.Nil(t, err)
	defer cpFile.Close()

	// create index
	shardState := singleShardState()
	index, err := NewIndex(testCtx(), IndexConfig{
		RootPath:          dirName,
		ClassName:         schema.ClassName(class.Class),
		ReplicationFactor: NewAtomicInt64(1),
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, &fakeSchemaGetter{
			schema: fakeSchema, shardState: shardState,
		}, nil, logger, nil, nil, nil, nil, class, nil, cpFile, nil)
	require.Nil(t, err)

	// force the index to load the shard
	productsIds := []strfmt.UUID{
		"1295c052-263d-4aae-99dd-920c5a370d06",
		"1295c052-263d-4aae-99dd-920c5a370d07",
	}

	products := []map[string]interface{}{
		{"name": "one"},
		{"name": "two"},
	}

	err = index.addProperty(context.TODO(), &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	require.Nil(t, err)

	for i, p := range products {
		product := models.Object{
			Class:      class.Class,
			ID:         productsIds[i],
			Properties: p,
		}

		err := index.putObject(context.TODO(), storobj.FromObject(
			&product, []float32{0.1, 0.2, 0.01, 0.2}, nil), nil, 0)
		require.Nil(t, err)
	}

	// drop the index
	err = index.drop()
	require.Nil(t, err)

	// ensure the checkpoint file is not deleted
	_, err = os.Stat(filepath.Join(dirName, "index.db"))
	require.Nil(t, err)
}

func emptyIdx(t *testing.T, rootDir string, class *models.Class) *Index {
	logger, _ := test.NewNullLogger()
	shardState := singleShardState()

	idx, err := NewIndex(testCtx(), IndexConfig{
		RootPath:              rootDir,
		ClassName:             schema.ClassName(class.Class),
		DisableLazyLoadShards: true,
		ReplicationFactor:     NewAtomicInt64(1),
	}, shardState, inverted.ConfigFromModel(invertedConfig()),
		hnsw.NewDefaultUserConfig(), nil, &fakeSchemaGetter{
			shardState: shardState,
		}, nil, logger, nil, nil, nil, nil, class, nil, nil, nil)
	require.Nil(t, err)
	return idx
}

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
	}
}

func getIndexFilenames(rootDir, indexName string) ([]string, error) {
	var filenames []string
	indexRoot, err := os.ReadDir(path.Join(rootDir, indexName))
	if err != nil {
		if os.IsNotExist(err) {
			// index was dropped, or never existed
			return filenames, nil
		}
		return nil, err
	}
	if len(indexRoot) == 0 {
		return nil, fmt.Errorf("index root length is 0")
	}
	shardFiles, err := os.ReadDir(path.Join(rootDir, indexName, indexRoot[0].Name()))
	if err != nil {
		return filenames, err
	}
	for _, f := range shardFiles {
		filenames = append(filenames, f.Name())
	}
	return filenames, nil
}

func TestIndex_DebugResetVectorIndex(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")

	ctx := context.Background()
	class := &models.Class{Class: "reindextest"}
	shard, index := testShardWithSettings(t, ctx, &models.Class{Class: class.Class}, hnsw.UserConfig{}, false, true /* withCheckpoints */)

	// unknown shard
	err := index.DebugResetVectorIndex(ctx, "unknown", "")
	require.Error(t, err)

	// unknown target vector
	err = index.DebugResetVectorIndex(ctx, shard.Name(), "unknown")
	require.Error(t, err)

	amount := 1000

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject("reindextest")
		objs = append(objs, obj)
	}

	errs := shard.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	// wait until the queue is empty
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if shard.Queue().Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	shard.Queue().Wait()

	// make sure the new index contains all the objects
	for _, obj := range objs {
		if !shard.VectorIndex().ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.DebugResetVectorIndex(ctx, shard.Name(), "")
	require.Nil(t, err)

	// wait until the queue is empty
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if shard.Queue().Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	shard.Queue().Wait()

	// make sure the new index contains all the objects
	for _, obj := range objs {
		if !shard.VectorIndex().ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_DebugResetVectorIndexTargetVector(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")

	ctx := context.Background()
	class := &models.Class{Class: "reindextest"}
	shard, index := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: class.Class},
		hnsw.UserConfig{},
		false,
		true,
		func(i *Index) {
			i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
			i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{}
		},
	)

	// unknown shard
	err := index.DebugResetVectorIndex(ctx, "unknown", "")
	require.Error(t, err)

	// unknown target vector
	err = index.DebugResetVectorIndex(ctx, shard.Name(), "unknown")
	require.Error(t, err)

	// non-existing main vector
	err = index.DebugResetVectorIndex(ctx, shard.Name(), "")
	require.Error(t, err)

	amount := 1000

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject("reindextest")
		obj.Vectors = map[string][]float32{
			"foo": {1, 2, 3},
		}
		objs = append(objs, obj)
	}

	errs := shard.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	q := shard.Queues()["foo"]
	// wait until the queue is empty
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	q.Wait()

	// make sure the new index contains all the objects
	vidx := shard.VectorIndexes()["foo"]
	for _, obj := range objs {
		if !vidx.ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.DebugResetVectorIndex(ctx, shard.Name(), "foo")
	require.Nil(t, err)

	q = shard.Queues()["foo"]
	// wait until the queue is empty
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	q.Wait()

	// make sure the new index contains all the objects
	vidx = shard.VectorIndexes()["foo"]
	for _, obj := range objs {
		if !vidx.ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_DebugResetVectorIndexPQ(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")
	t.Setenv("ASYNC_INDEX_INTERVAL", "100ms")

	ctx := context.Background()
	var cfg hnsw.UserConfig
	cfg.SetDefaults()
	cfg.MaxConnections = 16
	cfg.PQ.Enabled = true
	cfg.PQ.Centroids = 6
	cfg.PQ.Segments = 4
	cfg.PQ.TrainingLimit = 32

	shard, index := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: "reindextest"},
		cfg,
		false,
		true,
	)

	// unknown shard
	err := index.DebugResetVectorIndex(ctx, "unknown", "")
	require.Error(t, err)

	// unknown target vector
	err = index.DebugResetVectorIndex(ctx, shard.Name(), "unknown")
	require.Error(t, err)

	amount := 1000

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject("reindextest")
		obj.Vector = randVector(16)
		objs = append(objs, obj)
	}

	errs := shard.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	// wait until the queue is empty
	for i := 0; i < 200; i++ {
		time.Sleep(500 * time.Millisecond)
		if shard.Queue().Size() == 0 {
			break
		}
	}

	shard.Queue().Wait()

	// wait until the index is compressed
	for i := 0; i < 200; i++ {
		time.Sleep(500 * time.Millisecond)
		if shard.VectorIndex().Compressed() {
			break
		}
	}

	err = index.DebugResetVectorIndex(ctx, shard.Name(), "")
	require.Nil(t, err)

	// wait until the queue is empty
	for i := 0; i < 200; i++ {
		time.Sleep(500 * time.Millisecond)
		if shard.Queue().Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	shard.Queue().Wait()

	// wait until the index is compressed
	for i := 0; i < 200; i++ {
		time.Sleep(500 * time.Millisecond)
		if shard.VectorIndex().Compressed() {
			break
		}
	}

	// make sure the new index contains all the objects
	for _, obj := range objs {
		if !shard.VectorIndex().ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_DebugResetVectorIndexFlat(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")
	t.Setenv("ASYNC_INDEX_INTERVAL", "100ms")

	ctx := context.Background()
	class := &models.Class{Class: "reindextest"}
	shard, index := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: class.Class, VectorIndexType: "flat"},
		flat.UserConfig{},
		false,
		true,
	)

	err := index.DebugResetVectorIndex(ctx, shard.Name(), "")
	require.Error(t, err)

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_PreloadQueue(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")

	ctx := context.Background()
	class := &models.Class{Class: "preloadtest"}
	shard, index := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: class.Class},
		hnsw.UserConfig{},
		false,
		true,
	)
	amount := 1000

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject("preloadtest")
		obj.Vector = randVector(16)
		objs = append(objs, obj)
	}

	errs := shard.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	// reset the queue
	q := shard.Queue()
	err := q.ResetWith(shard.VectorIndex())
	require.Nil(t, err)
	q.ResumeIndexing()

	err = shard.PreloadQueue("")
	require.Nil(t, err)

	// wait until the queue is empty
	for i := 0; i < 200; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	q.Wait()

	// make sure the index contains all the objects
	for _, obj := range objs {
		if !shard.VectorIndex().ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.drop()
	require.Nil(t, err)
}

func TestIndex_PreloadQueueTargetVector(t *testing.T) {
	t.Setenv("ASYNC_INDEXING", "true")

	ctx := context.Background()
	class := &models.Class{Class: "preloadtest"}
	shard, index := testShardWithSettings(
		t,
		ctx,
		&models.Class{Class: class.Class},
		hnsw.UserConfig{},
		false,
		true,
		func(i *Index) {
			i.vectorIndexUserConfigs = make(map[string]schemaConfig.VectorIndexConfig)
			i.vectorIndexUserConfigs["foo"] = hnsw.UserConfig{}
		},
	)
	amount := 1000

	var objs []*storobj.Object
	for i := 0; i < amount; i++ {
		obj := testObject("preloadtest")
		obj.Vectors = map[string][]float32{
			"foo": {1, 2, 3},
		}
		objs = append(objs, obj)
	}

	errs := shard.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}

	q := shard.Queues()["foo"]
	vectorIndex := shard.VectorIndexes()["foo"]

	// reset the queue
	q.PauseIndexing()
	err := q.ResetWith(vectorIndex)
	require.Nil(t, err)
	q.ResumeIndexing()

	err = shard.PreloadQueue("foo")
	require.Nil(t, err)

	// wait until the queue is empty
	for i := 0; i < 200; i++ {
		time.Sleep(500 * time.Millisecond)
		if q.Size() == 0 {
			break
		}
	}

	// wait for the in-flight indexing to finish
	q.Wait()

	// make sure the index contains all the objects
	for _, obj := range objs {
		if !vectorIndex.ContainsNode(obj.DocID) {
			t.Fatalf("node %d should be in the vector index", obj.DocID)
		}
	}

	err = index.drop()
	require.Nil(t, err)
}

func TestTenantsSliceInitialization(t *testing.T) {
	// Test function to mimic the behavior of putObjectBatch
	initializeAndPopulateTenants := func(objects []*storobj.Object) []string {
		// Initialize tenants slice using the first method
		tenants := make([]string, 0, len(objects))

		for _, obj := range objects {
			if obj.Object.Tenant == "" {
				continue
			}
			tenants = append(tenants, obj.Object.Tenant)
		}

		// Remove duplicates (simple implementation for testing purposes)
		uniqueTenants := make([]string, 0, len(tenants))
		seen := make(map[string]bool)
		for _, tenant := range tenants {
			if !seen[tenant] {
				seen[tenant] = true
				uniqueTenants = append(uniqueTenants, tenant)
			}
		}

		return uniqueTenants
	}

	testCases := []struct {
		name            string
		objects         []*storobj.Object
		expectedTenants []string
	}{
		{
			name:            "Empty objects",
			objects:         []*storobj.Object{},
			expectedTenants: []string{},
		},
		{
			name: "Objects with empty tenants",
			objects: []*storobj.Object{
				{Object: struct{ Tenant string }{Tenant: ""}},
				{Object: struct{ Tenant string }{Tenant: ""}},
			},
			expectedTenants: []string{},
		},
		{
			name: "Objects with unique tenants",
			objects: []*storobj.Object{
				{Object: struct{ Tenant string }{Tenant: "tenant1"}},
				{Object: struct{ Tenant string }{Tenant: "tenant2"}},
				{Object: struct{ Tenant string }{Tenant: "tenant3"}},
			},
			expectedTenants: []string{"tenant1", "tenant2", "tenant3"},
		},
		{
			name: "Objects with duplicate tenants",
			objects: []*storobj.Object{
				{Object: struct{ Tenant string }{Tenant: "tenant1"}},
				{Object: struct{ Tenant string }{Tenant: "tenant2"}},
				{Object: struct{ Tenant string }{Tenant: "tenant1"}},
				{Object: struct{ Tenant string }{Tenant: "tenant3"}},
				{Object: struct{ Tenant string }{Tenant: "tenant2"}},
			},
			expectedTenants: []string{"tenant1", "tenant2", "tenant3"},
		},
		{
			name: "Mixed objects",
			objects: []*storobj.Object{
				{Object: struct{ Tenant string }{Tenant: "tenant1"}},
				{Object: struct{ Tenant string }{Tenant: ""}},
				{Object: struct{ Tenant string }{Tenant: "tenant2"}},
				{Object: struct{ Tenant string }{Tenant: "tenant1"}},
				{Object: struct{ Tenant string }{Tenant: ""}},
				{Object: struct{ Tenant string }{Tenant: "tenant3"}},
			},
			expectedTenants: []string{"tenant1", "tenant2", "tenant3"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := initializeAndPopulateTenants(tc.objects)
			assert.Equal(t, tc.expectedTenants, result, "Tenants slice should match expected values")
			assert.LessOrEqual(t, len(result), len(tc.objects), "Number of unique tenants should not exceed number of objects")
		})
	}
}
