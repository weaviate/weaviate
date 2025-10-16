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
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestBatchPutObjects_ConcurrentBatches(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()

	migrator := NewMigrator(repo, logger)

	class := &models.Class{
		Class:               "ConcurrentBatch",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	numGoroutines := 10
	objectsPerGoroutine := 50

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	errorChan := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			batch := make(objects.BatchObjects, objectsPerGoroutine)
			for i := 0; i < objectsPerGoroutine; i++ {
				id := fmt.Sprintf("%08d-%04d-0000-0000-000000000000", goroutineID, i)
				batch[i] = objects.BatchObject{
					OriginalIndex: i,
					Object: &models.Object{
						Class: "ConcurrentBatch",
						Properties: map[string]interface{}{
							"stringProp": fmt.Sprintf("goroutine-%d-object-%d", goroutineID, i),
						},
						ID:     strfmt.UUID(id),
						Vector: []float32{float32(goroutineID), float32(i), 0.1, 0.2},
					},
					UUID: strfmt.UUID(id),
				}
			}

			batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
			if err != nil {
				errorChan <- err
				return
			}

			for _, res := range batchRes {
				if res.Err != nil {
					errorChan <- res.Err
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errorChan)

	for err := range errorChan {
		t.Errorf("goroutine error: %v", err)
	}

	params := dto.GetParams{
		ClassName:  "ConcurrentBatch",
		Pagination: &filters.Pagination{Limit: 1000},
	}
	res, err := repo.Search(context.Background(), params)
	require.Nil(t, err)

	expectedTotal := numGoroutines * objectsPerGoroutine
	assert.Equal(t, expectedTotal, len(res), "should have all objects")
}

func TestBatchPutObjects_LargeSequentialBatches(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()

	migrator := NewMigrator(repo, logger)

	class := &models.Class{
		Class:               "LargeBatch",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	numBatches := 5
	batchSize := 200

	for batchNum := 0; batchNum < numBatches; batchNum++ {
		batch := make(objects.BatchObjects, batchSize)
		for i := 0; i < batchSize; i++ {
			idx := batchNum*batchSize + i
			id := fmt.Sprintf("00000000-0000-0000-0000-%012d", idx)
			batch[i] = objects.BatchObject{
				OriginalIndex: i,
				Object: &models.Object{
					Class: "LargeBatch",
					Properties: map[string]interface{}{
						"stringProp": fmt.Sprintf("batch-%d-object-%d", batchNum, i),
					},
					ID:     strfmt.UUID(id),
					Vector: []float32{float32(idx), 0.1, 0.2, 0.3},
				},
				UUID: strfmt.UUID(id),
			}
		}

		batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
		require.Nil(t, err, "batch %d failed", batchNum)

		for _, res := range batchRes {
			assert.Nil(t, res.Err, "batch %d had error", batchNum)
		}
	}

	params := dto.GetParams{
		ClassName:  "LargeBatch",
		Pagination: &filters.Pagination{Limit: 2000},
	}
	res, err := repo.Search(context.Background(), params)
	require.Nil(t, err)

	expectedTotal := numBatches * batchSize
	assert.Equal(t, expectedTotal, len(res))
}

func TestBatchPutObjects_UpdatesSameObjectsMultipleTimes(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()

	migrator := NewMigrator(repo, logger)

	class := &models.Class{
		Class:               "UpdateTest",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "version",
				DataType:     schema.DataTypeInt.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	numObjects := 10
	numUpdates := 5

	for updateRound := 0; updateRound < numUpdates; updateRound++ {
		batch := make(objects.BatchObjects, numObjects)
		for i := 0; i < numObjects; i++ {
			id := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
			batch[i] = objects.BatchObject{
				OriginalIndex: i,
				Object: &models.Object{
					Class: "UpdateTest",
					Properties: map[string]interface{}{
						"version": int64(updateRound),
					},
					ID:     strfmt.UUID(id),
					Vector: []float32{float32(updateRound), float32(i), 0.1, 0.2},
				},
				UUID: strfmt.UUID(id),
			}
		}

		batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil, 0)
		require.Nil(t, err, "update round %d failed", updateRound)

		for _, res := range batchRes {
			assert.Nil(t, res.Err)
		}
	}

	params := dto.GetParams{
		ClassName:  "UpdateTest",
		Pagination: &filters.Pagination{Limit: 100},
	}
	res, err := repo.Search(context.Background(), params)
	require.Nil(t, err)

	assert.Equal(t, numObjects, len(res), "should have exactly numObjects, not duplicates")

	for _, obj := range res {
		version := obj.Schema.(map[string]interface{})["version"]
		assert.Equal(t, float64(numUpdates-1), version, "should have latest version")
	}
}

func TestBatchPutObjects_ContextCancellation(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))

	defer func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	}()

	migrator := NewMigrator(repo, logger)

	class := &models.Class{
		Class:               "ContextCancel",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}

	require.Nil(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batchSize := 100
	batch := make(objects.BatchObjects, batchSize)
	for i := 0; i < batchSize; i++ {
		id := fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
		batch[i] = objects.BatchObject{
			OriginalIndex: i,
			Object: &models.Object{
				Class: "ContextCancel",
				Properties: map[string]interface{}{
					"stringProp": fmt.Sprintf("object-%d", i),
				},
				ID:     strfmt.UUID(id),
				Vector: []float32{float32(i), 0.1, 0.2, 0.3},
			},
			UUID: strfmt.UUID(id),
		}
	}

	batchRes, err := repo.BatchPutObjects(ctx, batch, nil, 0)
	require.Nil(t, err)

	errorCount := 0
	for _, res := range batchRes {
		if res.Err != nil {
			errorCount++
		}
	}

	assert.Greater(t, errorCount, 0, "should have context errors")
}
