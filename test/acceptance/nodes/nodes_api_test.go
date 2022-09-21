//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/meta"
	"github.com/semi-technologies/weaviate/client/nodes"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/books"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/multishard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			assert.Greater(t, shard.ObjectCount, int64(0))
		}
		require.NotNil(t, nodeStatus.Stats)
		assert.Equal(t, int64(3), nodeStatus.Stats.ObjectCount)
		assert.Equal(t, int64(2), nodeStatus.Stats.ShardCount)
	})
}
