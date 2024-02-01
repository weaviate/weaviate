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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/meta"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func Test_NodesBatchStatusAPI(t *testing.T) {
	t.Run("empty DB", func(t *testing.T) {
		meta, err := helper.Client(t).Meta.MetaGet(meta.NewMetaGetParams(), nil)
		require.Nil(t, err)
		assert.NotNil(t, meta.GetPayload())

		assertions := func(t *testing.T, nodeBatchStatus *models.NodeBatchStatus) {
			require.NotNil(t, nodeBatchStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeBatchStatus.Status)
			assert.True(t, len(nodeBatchStatus.Name) > 0)
			require.NotNil(t, nodeBatchStatus.BatchStats)
		}

		testBatchStatusResponse(t, assertions)
	})

	t.Run("DB with Books (1 class ,1 shard configuration, 1 node)", func(t *testing.T) {
		booksClass := books.ClassContextionaryVectorizer()
		helper.CreateClass(t, booksClass)
		defer helper.DeleteClass(t, booksClass.Class)

		for _, book := range books.Objects() {
			helper.CreateObject(t, book)
			helper.AssertGetObjectEventually(t, book.Class, book.ID)
		}

		assertions := func(t *testing.T, nodeBatchStatus *models.NodeBatchStatus) {
			require.NotNil(t, nodeBatchStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeBatchStatus.Status)
			assert.True(t, len(nodeBatchStatus.Name) > 0)
			require.NotNil(t, nodeBatchStatus.BatchStats)
		}

		testBatchStatusResponse(t, assertions)
	})

	t.Run("DB with MultiShard (1 class, 2 shards configuration, 1 node)", func(t *testing.T) {
		multiShardClass := multishard.ClassContextionaryVectorizer()
		helper.CreateClass(t, multiShardClass)
		defer helper.DeleteClass(t, multiShardClass.Class)

		for _, multiShard := range multishard.Objects() {
			helper.CreateObject(t, multiShard)
			helper.AssertGetObjectEventually(t, multiShard.Class, multiShard.ID)
		}

		assertions := func(t *testing.T, nodeBatchStatus *models.NodeBatchStatus) {
			require.NotNil(t, nodeBatchStatus)
			assert.Equal(t, models.NodeStatusStatusHEALTHY, *nodeBatchStatus.Status)
			assert.True(t, len(nodeBatchStatus.Name) > 0)
			require.NotNil(t, nodeBatchStatus.BatchStats)
		}

		testBatchStatusResponse(t, assertions)
	})
}

func testBatchStatusResponse(t *testing.T, assertions func(*testing.T, *models.NodeBatchStatus)) {
	commonTests := func(resp *nodes.NodesBatchStatusGetOK) {
		require.NotNil(t, resp.Payload)
		nodes := resp.Payload.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 1)
		assertions(t, nodes[0])
	}

	t.Run("assertions", func(t *testing.T) {
		payload, err := getNodesBatchStatus(t)
		require.Nil(t, err)
		commonTests(&nodes.NodesBatchStatusGetOK{Payload: payload})
	})
}

func getNodesBatchStatus(t *testing.T) (*models.NodesBatchStatusResponse, error) {
	params := nodes.NewNodesBatchStatusGetParams()
	body, clientErr := helper.Client(t).Nodes.NodesBatchStatusGet(params, nil)
	return body.Payload, clientErr
}
