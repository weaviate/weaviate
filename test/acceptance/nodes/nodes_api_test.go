//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/meta"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
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

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.Nil(t, err)

		nodeStatusResp := resp.GetPayload()
		require.NotNil(t, nodeStatusResp)

		nodes := nodeStatusResp.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)

		nodeStatus := nodes[0]
		require.NotNil(t, nodeStatus)
		assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
		assert.True(t, len(nodeStatus.Name) > 0)
		assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
		assert.Equal(t, meta.Payload.Version, nodeStatus.Version)
		assert.Empty(t, nodeStatus.Shards)
		require.NotNil(t, nodeStatus.Stats)
		assert.Equal(t, int64(0), nodeStatus.Stats.ObjectCount)
		assert.Equal(t, int64(0), nodeStatus.Stats.ShardCount)
	})

	t.Run("DB with Books (1 class ,1 shard configuration, 1 node)", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.Nil(t, err)

		nodeStatusResp := resp.GetPayload()
		require.NotNil(t, nodeStatusResp)

		nodes := nodeStatusResp.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)

		nodeStatus := nodes[0]
		require.NotNil(t, nodeStatus)
		assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
		assert.True(t, len(nodeStatus.Name) > 0)
		assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
		assert.Len(t, nodeStatus.Shards, 1)
		shard := nodeStatus.Shards[0]
		assert.True(t, len(shard.Name) > 0)
		assert.Equal(t, booksClass.Class, shard.Class)
		assert.Equal(t, int64(3), shard.ObjectCount)

		require.NotNil(t, nodeStatus.Stats)
		assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
		assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
	})

	t.Run("DB with MultiShard (1 class, 2 shards configuration, 1 node)", func(t *testing.T) {
		multiShardClass := multishard.ClassContextionaryVectorizer()
		helper.CreateClass(t, multiShardClass)
		defer helper.DeleteClass(t, multiShardClass.Class)

		for _, multiShard := range multishard.Objects() {
			helper.CreateObject(t, multiShard)
			helper.AssertGetObjectEventually(t, multiShard.Class, multiShard.ID)
		}

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.Nil(t, err)

		nodeStatusResp := resp.GetPayload()
		require.NotNil(t, nodeStatusResp)

		nodes := nodeStatusResp.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)

		nodeStatus := nodes[0]
		require.NotNil(t, nodeStatus)
		assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
		assert.True(t, len(nodeStatus.Name) > 0)
		assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
		assert.Len(t, nodeStatus.Shards, 2)
		for _, shard := range nodeStatus.Shards {
			assert.True(t, len(shard.Name) > 0)
			assert.Equal(t, multiShardClass.Class, shard.Class)
			assert.GreaterOrEqual(t, shard.ObjectCount, int64(0))
		}
		require.NotNil(t, nodeStatus.Stats)
		assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
		assert.Equal(t, int64(2), nodeStatus.Stats.ShardCount)
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

			resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
			require.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			respNodes := nodeStatusResp.Nodes
			require.NotNil(t, respNodes)
			require.Len(t, respNodes, 1)

			nodeStatus := respNodes[0]

			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
			assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
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
			params := nodes.NewNodesGetClassParams().WithClassName(docsClass.Class)
			classResp, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
			require.Nil(t, err)

			nodeStatusResp := classResp.GetPayload()
			require.NotNil(t, nodeStatusResp)

			respNodes := nodeStatusResp.Nodes
			require.NotNil(t, respNodes)
			require.Len(t, respNodes, 1)

			nodeStatus := respNodes[0]
			require.NotNil(t, nodeStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
			assert.True(t, len(nodeStatus.Name) > 0)
			assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
			assert.Len(t, nodeStatus.Shards, 1)
			shard := nodeStatus.Shards[0]
			assert.True(t, len(shard.Name) > 0)
			assert.Equal(t, docsClass.Class, shard.Class)
			assert.Equal(t, int64(2), shard.ObjectCount)

			require.NotNil(t, nodeStatus.Stats)
			assert.Equal(t, int64(2), nodeStatus.Stats.ObjectCount)
			assert.Equal(t, int64(1), nodeStatus.Stats.ShardCount)
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

		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
		require.Nil(t, err)

		nodeStatusResp := resp.GetPayload()
		require.NotNil(t, nodeStatusResp)

		nodes := nodeStatusResp.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)

		nodeStatus := nodes[0]
		require.NotNil(t, nodeStatus)

		require.NotNil(t, nodeStatus.Stats)
		assert.Equal(t, int64(1), nodeStatus.Stats.ObjectCount)
	})

	t.Run("validate compression status", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		objects := make([]*models.Object, 256)

		for i := 0; i < 256; i++ {
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

		t.Run("check disabled without pq", func(t *testing.T) {
			resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
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

		t.Run("check enabled with pq", func(t *testing.T) {
			class := helper.GetClass(t, booksClass.Class)
			cfg := class.VectorIndexConfig.(map[string]interface{})
			cfg["pq"] = map[string]interface{}{
				"enabled":  true,
				"segments": 1,
			}
			class.VectorIndexConfig = cfg
			helper.UpdateClass(t, class)

			checkThunk := func() interface{} {
				resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
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
