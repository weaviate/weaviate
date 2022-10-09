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
	"os"
	"testing"

	"github.com/semi-technologies/weaviate/client/nodes"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/books"
	"github.com/semi-technologies/weaviate/test/helper/sample-schema/multishard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_WeaviateCluster_NodesAPI(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateNode1Endpoint))
	booksClass := books.ClassContextionaryVectorizer()
	multiShardClass := multishard.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	helper.CreateClass(t, multiShardClass)
	defer helper.DeleteClass(t, booksClass.Class)
	defer helper.DeleteClass(t, multiShardClass.Class)

	t.Run("import data", func(t *testing.T) {
		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}
		for _, multishard := range multishard.Objects() {
			helper.CreateObject(t, multishard)
			helper.AssertGetObjectEventually(t, multishard.Class, multishard.ID)
		}
	})

	t.Run("check nodes api", func(t *testing.T) {
		for _, endpoint := range []string{weaviateNode1Endpoint, weaviateNode2Endpoint} {
			t.Run(endpoint, func(t *testing.T) {
				helper.SetupClient(os.Getenv(endpoint))
				resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
				require.Nil(t, err)

				nodeStatusResp := resp.GetPayload()
				require.NotNil(t, nodeStatusResp)

				nodes := nodeStatusResp.Nodes
				require.NotNil(t, nodes)
				require.Len(t, nodes, 2)

				assert.Equal(t, "node1", nodes[0].Name)
				assert.Equal(t, "node2", nodes[1].Name)

				for i, nodeStatus := range nodes {
					require.NotNil(t, nodeStatus)
					assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeStatus.Status)
					if i == 0 {
						assert.Equal(t, "node1", nodeStatus.Name)
					} else {
						assert.Equal(t, "node2", nodeStatus.Name)
					}
					assert.True(t, nodeStatus.GitHash != "" && nodeStatus.GitHash != "unknown")
					assert.Len(t, nodeStatus.Shards, 2)
					var objectCount int64
					var shardCount int64
					for _, shard := range nodeStatus.Shards {
						assert.True(t, len(shard.Name) > 0)
						assert.True(t, shard.Class == multiShardClass.Class || shard.Class == booksClass.Class)
						assert.GreaterOrEqual(t, shard.ObjectCount, int64(0))
						objectCount += shard.ObjectCount
						shardCount++
					}
					require.NotNil(t, nodeStatus.Stats)
					assert.Equal(t, objectCount, nodeStatus.Stats.ObjectCount)
					assert.Equal(t, shardCount, nodeStatus.Stats.ShardCount)
				}
			})
		}
	})
}
