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

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/meta"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func Test_NodesAPI(t *testing.T) {
	t.Run("empty DB", func(t *testing.T) {
		meta, err := helper.Client(t).Meta.MetaGet(meta.NewMetaGetParams(), nil)
		require.Nil(t, err)
		assert.NotNil(t, meta.GetPayload())

		assertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
			assert.Equal(t, meta.Payload.Version, nodeStatus.Version)
			assert.Empty(t, nodeStatus.Shards)
			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(0), nodeStatus.Stats.ObjectCount)
			assert.Equal(t, int64(0), nodeStatus.Stats.ShardCount)
		}

		testStatusResponse(t, assertions, nil, "")
	})

	t.Run("DB with Books (1 class ,1 shard configuration, 1 node)", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}

		minimalAssertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
			assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
		}

		verboseAssertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
			require.Len(t, nodeStatus.Shards, 1)
			shard := nodeStatus.Shards[0]
			assert.True(t, len(shard.Name) > 0)
			assert.Equal(t, booksClass.Class, shard.Class)
			assert.Equal(t, int64(3), shard.ObjectCount)
		}

		testStatusResponse(t, minimalAssertions, verboseAssertions, "")
	})

	t.Run("DB with MultiShard (1 class, 2 shards configuration, 1 node)", func(t *testing.T) {
		multiShardClass := multishard.ClassContextionaryVectorizer()
		helper.CreateClass(t, multiShardClass)
		defer helper.DeleteClass(t, multiShardClass.Class)

		for _, multiShard := range multishard.Objects() {
			helper.CreateObject(t, multiShard)
			helper.AssertGetObjectEventually(t, multiShard.Class, multiShard.ID)
		}

		minimalAssertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
			assert.Equal(t, int64(2), nodeStatus.Stats.ShardCount)
		}

		verboseAsssertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
			assert.Len(t, nodeStatus.Shards, 2)
			for _, shard := range nodeStatus.Shards {
				assert.True(t, len(shard.Name) > 0)
				assert.Equal(t, multiShardClass.Class, shard.Class)
				assert.GreaterOrEqual(t, shard.ObjectCount, int64(0))
			}
		}

		testStatusResponse(t, minimalAssertions, verboseAsssertions, "")
	})

	t.Run("with class name: DB with Books and Documents, 1 shard, 1 node", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("insert and check books", func(t *testing.T) {
			for _, book := range books.Objects() {
				helper.CreateObject(t, book)
				helper.AssertGetObjectEventually(t, book.Class, book.ID)
			}

			assertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
				require.NotNil(t, nodeStatus.Stats)
				assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
				assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
			}

			testStatusResponse(t, assertions, nil, "")
		})

		t.Run("insert and check documents", func(t *testing.T) {
			docsClasses := documents.ClassesContextionaryVectorizer(false)
			helper.CreateClass(t, docsClasses[0])
			helper.CreateClass(t, docsClasses[1])
			defer helper.DeleteClass(t, docsClasses[0].Class)
			defer helper.DeleteClass(t, docsClasses[1].Class)

			for _, doc := range documents.Objects() {
				helper.CreateObject(t, doc)
				helper.AssertGetObjectEventually(t, doc.Class, doc.ID)
			}

			docsClass := docsClasses[0]

			minimalAssertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
				assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
				assert.True(t, len(nodeStatus.Name) > 0)
				assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
				require.NotNil(t, nodeStatus.Stats)
				assert.Equal(t, int64(2), nodeStatus.Stats.ObjectCount)
				assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
			}

			verboseAssertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
				assert.Len(t, nodeStatus.Shards, 1)
				shard := nodeStatus.Shards[0]
				assert.True(t, len(shard.Name) > 0)
				assert.Equal(t, docsClass.Class, shard.Class)
				assert.Equal(t, int64(2), shard.ObjectCount)
			}

			testStatusResponse(t, minimalAssertions, verboseAssertions, docsClass.Class)
		})
	})

	// This test prevents a regression of
	// https://github.com/weaviate/weaviate/issues/2454
	t.Run("validate count with updates", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		_, err := helper.BatchClient(t).BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{
						ID:    strfmt.UUID("2D0D3E3B-54B2-48D4-BFE0-4BE2C060110E"),
						Class: booksClass.Class,
						Properties: map[string]interface{}{
							"title":       "A book that changes",
							"description": "First iteration",
						},
					},
				},
			}), nil)
		require.Nil(t, err)

		// Note that this is the same ID as before, so this is an update!!
		_, err = helper.BatchClient(t).BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{
						ID:    strfmt.UUID("2D0D3E3B-54B2-48D4-BFE0-4BE2C060110E"),
						Class: booksClass.Class,
						Properties: map[string]interface{}{
							"title":       "A book that changes",
							"description": "A new (second) iteration",
						},
					},
				},
			}), nil)
		require.Nil(t, err)

		assertions := func(t *testing.T, nodeStatus *models.NodeStatus) {
			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(1), nodeStatus.Stats.ObjectCount)
		}

		testStatusResponse(t, assertions, nil, "")
	})
}

func TestNodesApi_Compression_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("validate flat compression status", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		booksClass.VectorIndexType = "flat"
		booksClass.VectorIndexConfig = map[string]interface{}{
			"bq": map[string]interface{}{
				"enabled": true,
			},
		}
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("check compressed true", func(t *testing.T) {
			verbose := "verbose"
			params := nodes.NewNodesGetParams().WithOutput(&verbose)
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			require.NotNil(t, nodes)
			require.Len(t, nodes, 1)

			nodeStatus := nodes[0]
			require.NotNil(t, nodeStatus)

			require.True(t, nodeStatus.Shards[0].Compressed)
		})
	})

	t.Run("validate hnsw pq async compression", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		booksClass.VectorIndexConfig = map[string]interface{}{
			"pq": map[string]interface{}{
				"trainingLimit": 256,
				"enabled":       true,
				"segments":      1,
			},
		}
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("check compressed initially false", func(t *testing.T) {
			verbose := "verbose"
			params := nodes.NewNodesGetParams().WithOutput(&verbose)
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			require.NotNil(t, nodes)
			require.Len(t, nodes, 1)

			nodeStatus := nodes[0]
			require.NotNil(t, nodeStatus)

			require.False(t, nodeStatus.Shards[0].Compressed)
		})

		t.Run("load data for pq", func(t *testing.T) {
			num := 1024
			objects := make([]*models.Object, num)

			for i := 0; i < num; i++ {
				objects[i] = &models.Object{
					Class:  booksClass.Class,
					Vector: []float32{float32(i % 32), float32(i), 3.0, 4.0},
				}
			}

			_, err := helper.BatchClient(t).BatchObjectsCreate(
				batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
					Objects: objects,
				},
				), nil)
			require.Nil(t, err)
		})

		t.Run("check eventually compressed if async enabled", func(t *testing.T) {
			checkThunk := func() interface{} {
				verbose := "verbose"
				params := nodes.NewNodesGetParams().WithOutput(&verbose)
				resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
				require.Nil(t, err)

				nodeStatusResp := resp.GetPayload()
				require.NotNil(t, nodeStatusResp)

				nodes := nodeStatusResp.Nodes
				require.NotNil(t, nodes)
				require.Len(t, nodes, 1)

				nodeStatus := nodes[0]
				require.NotNil(t, nodeStatus)
				return nodeStatus.Shards[0].Compressed
			}

			helper.AssertEventuallyEqualWithFrequencyAndTimeout(t, true, checkThunk, 100*time.Millisecond, 10*time.Second)
		})
	})
}

func TestNodesApi_Compression_SyncIndexing(t *testing.T) {
	t.Run("validate flat compression status", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		booksClass.VectorIndexType = "flat"
		booksClass.VectorIndexConfig = map[string]interface{}{
			"bq": map[string]interface{}{
				"enabled": true,
			},
		}
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		t.Run("check compressed true", func(t *testing.T) {
			verbose := "verbose"
			params := nodes.NewNodesGetParams().WithOutput(&verbose)
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			require.NotNil(t, nodes)
			require.Len(t, nodes, 1)

			nodeStatus := nodes[0]
			require.NotNil(t, nodeStatus)

			require.True(t, nodeStatus.Shards[0].Compressed)
		})
	})
}

func testStatusResponse(t *testing.T, minimalAssertions, verboseAssertions func(*testing.T, *models.NodeStatus),
	class string,
) {
	minimal, verbose := verbosity.OutputMinimal, verbosity.OutputVerbose

	commonTests := func(resp *nodes.NodesGetOK) {
		require.NotNil(t, resp.Payload)
		nodes := resp.Payload.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)
		minimalAssertions(t, nodes[0])
	}

	t.Run("minimal", func(t *testing.T) {
		payload, err := getNodesStatus(t, minimal, class)
		require.Nil(t, err)
		commonTests(&nodes.NodesGetOK{Payload: payload})
	})

	if verboseAssertions != nil {
		t.Run("verbose", func(t *testing.T) {
			payload, err := getNodesStatus(t, verbose, class)
			require.Nil(t, err)
			commonTests(&nodes.NodesGetOK{Payload: payload})
			// If commonTests pass, resp.Nodes[0] != nil
			verboseAssertions(t, payload.Nodes[0])
		})
	}
}

func getNodesStatus(t *testing.T, output, class string) (payload *models.NodesStatusResponse, err error) {
	if class != "" {
		params := nodes.NewNodesGetClassParams().WithOutput(&output).WithClassName(class)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		payload, err = body.Payload, clientErr
	} else {
		params := nodes.NewNodesGetParams().WithOutput(&output)
		body, clientErr := helper.Client(t).Nodes.NodesGet(params, nil)
		payload, err = body.Payload, clientErr
	}
	return
}
