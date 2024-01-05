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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func Test_Objects(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		helper.AssertCreateObjectClass(t, &models.Class{
			Class: "ObjectTestThing",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "testString",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
		helper.AssertCreateObjectClass(t, &models.Class{
			Class: "TestObject",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "testString",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:     "testWholeNumber",
					DataType: []string{"int"},
				},
				{
					Name:     "testNumber",
					DataType: []string{"number"},
				},
				{
					Name:     "testDateTime",
					DataType: []string{"date"},
				},
				{
					Name:     "testTrueFalse",
					DataType: []string{"boolean"},
				},
				{
					Name:     "testReference",
					DataType: []string{"ObjectTestThing"},
				},
			},
		})
		helper.AssertCreateObjectClass(t, &models.Class{
			Class: "TestObjectTwo",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "testReference",
					DataType: []string{"TestObject"},
				},
				{
					Name:     "testReferences",
					DataType: []string{"TestObject"},
				},
				{
					Name:         "testString",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
	})

	// tests
	t.Run("adding objects", addingObjects)
	t.Run("removing objects", removingObjects)
	t.Run("object references", objectReferences)
	t.Run("updating objects deprecated", updateObjectsDeprecated)

	// tear down
	helper.AssertDeleteObjectClass(t, "ObjectTestThing")
	helper.AssertDeleteObjectClass(t, "TestObject")
	helper.AssertDeleteObjectClass(t, "TestObjectTwo")
}

func Test_Delete_ReadOnly_Classes(t *testing.T) {
	className := "DeleteReadonlyClassTest"

	t.Run("setup", func(t *testing.T) {
		helper.AssertCreateObjectClass(t, &models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "stringProp",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})

		batchSize := 1000
		batch := make([]*models.Object, batchSize)
		for i := 0; i < batchSize; i++ {
			batch[i] = &models.Object{
				Class: className,
				Properties: map[string]interface{}{
					"stringProp": fmt.Sprintf("obj#%d", i+1),
				},
			}
		}
		helper.CreateObjectsBatch(t, batch)
	})

	t.Run("assert data exists", func(t *testing.T) {
		res := graphqlhelper.AssertGraphQL(t, helper.RootAuth,
			fmt.Sprintf("{Aggregate {%s {meta {count}}}}", className))
		count := res.Get("Aggregate", className).AsSlice()[0].(map[string]interface{})["meta"].(map[string]interface{})["count"]
		require.EqualValues(t, json.Number("1000"), count)
	})

	t.Run("set shard to readonly", func(t *testing.T) {
		verbose := verbosity.OutputVerbose
		nodesResp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(&verbose), nil)
		require.Nil(t, err)
		require.NotNil(t, nodesResp.Payload)
		require.Len(t, nodesResp.Payload.Nodes, 1)
		require.Len(t, nodesResp.Payload.Nodes[0].Shards, 1)
		require.Equal(t, className, nodesResp.Payload.Nodes[0].Shards[0].Class)
		targetShard := nodesResp.Payload.Nodes[0].Shards[0].Name

		params := clschema.NewSchemaObjectsShardsUpdateParams().
			WithBody(&models.ShardStatus{Status: storagestate.StatusReadOnly.String()}).
			WithClassName(className).
			WithShardName(targetShard)
		shardsResp, err := helper.Client(t).Schema.SchemaObjectsShardsUpdate(params, nil)
		require.Nil(t, err)
		require.NotNil(t, shardsResp.Payload)
		require.Equal(t, storagestate.StatusReadOnly.String(), shardsResp.Payload.Status)
	})

	t.Run("delete class with readonly shard", func(t *testing.T) {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		resp, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		require.Nil(t, err)
		require.True(t, resp.IsCode(http.StatusOK))
	})

	t.Run("assert class is deleted", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Equal(t, err, &clschema.SchemaObjectsGetNotFound{})
	})
}
