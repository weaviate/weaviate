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
	"math/rand"
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
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
)

type testRepo struct {
	*DB
	t *testing.T
}

func setupTestRepo(t *testing.T, className string, properties []*models.Property) *testRepo {
	t.Helper()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}

	asyncEnabled := rand.Int()%2 == 0

	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		AsyncIndexingEnabled:      asyncEnabled,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, memwatch.NewDummyMonitor())
	require.NoError(t, err)

	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))

	t.Cleanup(func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	})

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties:          properties,
	}

	migrator := NewMigrator(repo, logger)
	require.NoError(t, migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	schemaGetter.schema.Objects = &models.Schema{Classes: []*models.Class{class}}

	return &testRepo{DB: repo, t: t}
}

func (r *testRepo) awaitIndexing() {
	r.t.Helper()
	if r.AsyncIndexingEnabled {
		r.scheduler.WaitAll()
	}
}

func (r *testRepo) search(className string, limit int) []search.Result {
	r.t.Helper()
	r.awaitIndexing()

	res, err := r.Search(context.Background(), dto.GetParams{
		ClassName:  className,
		Pagination: &filters.Pagination{Limit: limit},
	})
	require.NoError(r.t, err)
	return res
}

func TestBatchPutObjects_ConcurrentBatches(t *testing.T) {
	repo := setupTestRepo(t, "ConcurrentBatch", []*models.Property{
		{
			Name:         "stringProp",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	})

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

	res := repo.search("ConcurrentBatch", 1000)
	assert.Equal(t, numGoroutines*objectsPerGoroutine, len(res), "should have all objects")
}

func TestBatchPutObjects_LargeSequentialBatches(t *testing.T) {
	repo := setupTestRepo(t, "LargeBatch", []*models.Property{
		{
			Name:         "stringProp",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	})

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
		require.NoError(t, err, "batch %d failed", batchNum)

		for _, res := range batchRes {
			assert.Nil(t, res.Err, "batch %d had error", batchNum)
		}
	}

	res := repo.search("LargeBatch", 2000)
	assert.Equal(t, numBatches*batchSize, len(res))
}

func TestBatchPutObjects_UpdatesSameObjectsMultipleTimes(t *testing.T) {
	repo := setupTestRepo(t, "UpdateTest", []*models.Property{
		{
			Name:         "version",
			DataType:     schema.DataTypeInt.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	})

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
		require.NoError(t, err, "update round %d failed", updateRound)

		for _, res := range batchRes {
			assert.Nil(t, res.Err)
		}
	}

	res := repo.search("UpdateTest", 100)
	assert.Equal(t, numObjects, len(res), "should have exactly numObjects, not duplicates")

	for _, result := range res {
		version := result.Schema.(map[string]interface{})["version"]
		assert.Equal(t, float64(numUpdates-1), version, "should have latest version")
	}
}

func TestBatchPutObjects_ContextCancellation(t *testing.T) {
	repo := setupTestRepo(t, "ContextCancel", []*models.Property{
		{
			Name:         "stringProp",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWhitespace,
		},
	})

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
	require.NoError(t, err)

	errorCount := 0
	for _, res := range batchRes {
		if res.Err != nil {
			errorCount++
		}
	}

	assert.Greater(t, errorCount, 0, "should have context errors")
}
