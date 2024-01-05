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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/objects"
)

func TestNodesAPI_Journey(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		ServerVersion:             "server-version",
		GitHash:                   "git-hash",
		MemtablesFlushIdleAfter:   60,
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

	// check nodes api response on empty DB
	nodeStatues, err := repo.GetNodeStatus(context.Background(), "", verbosity.OutputVerbose)
	require.Nil(t, err)
	require.NotNil(t, nodeStatues)

	require.Len(t, nodeStatues, 1)
	nodeStatus := nodeStatues[0]
	assert.NotNil(t, nodeStatus)
	assert.Equal(t, "node1", nodeStatus.Name)
	assert.Equal(t, "server-version", nodeStatus.Version)
	assert.Equal(t, "git-hash", nodeStatus.GitHash)
	assert.Len(t, nodeStatus.Shards, 0)
	assert.Equal(t, int64(0), nodeStatus.Stats.ObjectCount)
	assert.Equal(t, int64(0), nodeStatus.Stats.ShardCount)

	// import 2 objects
	class := &models.Class{
		Class:               "ClassNodesAPI",
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

	require.Nil(t,
		migrator.AddClass(context.Background(), class, schemaGetter.shardState))

	schemaGetter.schema.Objects = &models.Schema{
		Classes: []*models.Class{class},
	}

	batch := objects.BatchObjects{
		objects.BatchObject{
			OriginalIndex: 0,
			Err:           nil,
			Object: &models.Object{
				Class: "ClassNodesAPI",
				Properties: map[string]interface{}{
					"stringProp": "first element",
				},
				ID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
			},
			UUID: "8d5a3aa2-3c8d-4589-9ae1-3f638f506970",
		},
		objects.BatchObject{
			OriginalIndex: 1,
			Err:           nil,
			Object: &models.Object{
				Class: "ClassNodesAPI",
				Properties: map[string]interface{}{
					"stringProp": "second element",
				},
				ID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
			},
			UUID: "86a380e9-cb60-4b2a-bc48-51f52acd72d6",
		},
	}
	batchRes, err := repo.BatchPutObjects(context.Background(), batch, nil)
	require.Nil(t, err)

	assert.Nil(t, batchRes[0].Err)
	assert.Nil(t, batchRes[1].Err)

	// check nodes api after importing 2 objects to DB
	nodeStatues, err = repo.GetNodeStatus(context.Background(), "", verbosity.OutputVerbose)
	require.Nil(t, err)
	require.NotNil(t, nodeStatues)

	require.Len(t, nodeStatues, 1)
	nodeStatus = nodeStatues[0]
	assert.NotNil(t, nodeStatus)
	assert.Equal(t, "node1", nodeStatus.Name)
	assert.Equal(t, "server-version", nodeStatus.Version)
	assert.Equal(t, "git-hash", nodeStatus.GitHash)
	assert.Len(t, nodeStatus.Shards, 1)
	assert.Equal(t, "ClassNodesAPI", nodeStatus.Shards[0].Class)
	assert.True(t, len(nodeStatus.Shards[0].Name) > 0)
	assert.Equal(t, int64(2), nodeStatus.Shards[0].ObjectCount)
	assert.Equal(t, "READY", nodeStatus.Shards[0].VectorIndexingStatus)
	assert.Equal(t, int64(0), nodeStatus.Shards[0].VectorQueueLength)
	assert.Equal(t, int64(2), nodeStatus.Stats.ObjectCount)
	assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
}
