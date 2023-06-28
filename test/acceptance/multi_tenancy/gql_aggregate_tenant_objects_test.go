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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestGQLAggregateTenantObjects(t *testing.T) {
	tenantKey := "tenantName"
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			},
		},
	}
	tenantName1 := "Tenant1"
	tenantName2 := "Tenant2"
	numTenantObjs1 := 5
	numTenantObjs2 := 3

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	t.Run("setup test data", func(t *testing.T) {
		t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
			helper.CreateClass(t, &testClass)
		})

		t.Run("create tenants", func(t *testing.T) {
			tenants := []*models.Tenant{
				{tenantName1},
				{tenantName2},
			}
			helper.CreateTenants(t, testClass.Class, tenants)
		})

		t.Run("add tenant objects", func(t *testing.T) {
			batch1 := makeTenantBatch(batchParams{
				className:  testClass.Class,
				tenantName: tenantName1,
				tenantKey:  tenantKey,
				batchSize:  numTenantObjs1,
			})
			batch2 := makeTenantBatch(batchParams{
				className:  testClass.Class,
				tenantName: tenantName2,
				tenantKey:  tenantKey,
				batchSize:  numTenantObjs2,
			})

			helper.CreateObjectsBatch(t, batch1)
			helper.CreateObjectsBatch(t, batch2)
		})
	})

	t.Run("GQL Aggregate tenant objects", func(t *testing.T) {
		testAggregateTenantSuccess(t, testClass.Class, tenantName1, numTenantObjs1)
		testAggregateTenantSuccess(t, testClass.Class, tenantName2, numTenantObjs2)
	})

	t.Run("Get global tenant objects count", func(t *testing.T) {
		params := nodes.NewNodesGetClassParams().WithClassName(testClass.Class)
		resp, err := helper.Client(t).Nodes.NodesGetClass(params, nil)
		require.Nil(t, err)

		payload := resp.GetPayload()
		require.NotNil(t, payload)
		require.NotNil(t, payload.Nodes)
		require.Len(t, payload.Nodes, 1)

		node := payload.Nodes[0]
		require.NotNil(t, node)
		assert.Equal(t, models.NodeStatusStatusHEALTHY, *node.Status)
		assert.True(t, len(node.Name) > 0)
		assert.True(t, node.GitHash != "" && node.GitHash != "unknown")
		assert.Len(t, node.Shards, 2)

		shardCount := map[string]int64{
			tenantName1: int64(numTenantObjs1),
			tenantName2: int64(numTenantObjs2),
		}

		for _, shard := range node.Shards {
			count, ok := shardCount[shard.Name]
			require.True(t, ok, "expected shard %q to be in %+v",
				shard.Name, []string{tenantName1, tenantName2})

			assert.Equal(t, testClass.Class, shard.Class)
			assert.Equal(t, count, shard.ObjectCount)
		}

		require.NotNil(t, node.Stats)
		assert.Equal(t, int64(numTenantObjs1+numTenantObjs2), node.Stats.ObjectCount)
		assert.Equal(t, int64(2), node.Stats.ShardCount)
	})
}

func TestGQLAggregateTenantObjects_InvalidTenantKey(t *testing.T) {
	tenantKey := "tenantName"
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			},
		},
	}
	tenantName := "Tenant1"
	numTenantObjs := 5

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	t.Run("setup test data", func(t *testing.T) {
		t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
			helper.CreateClass(t, &testClass)
		})

		t.Run("create tenants", func(t *testing.T) {
			tenants := []*models.Tenant{
				{tenantName},
			}
			helper.CreateTenants(t, testClass.Class, tenants)
		})

		t.Run("add tenant objects", func(t *testing.T) {
			batch := makeTenantBatch(batchParams{
				className:  testClass.Class,
				tenantName: tenantName,
				tenantKey:  tenantKey,
				batchSize:  numTenantObjs,
			})
			helper.CreateObjectsBatch(t, batch)
		})
	})

	t.Run("non-existent tenant key", func(t *testing.T) {
		query := fmt.Sprintf(`{Aggregate{%s(tenantKey:"DNE"){meta{count}}}}`, testClass.Class)
		expected := `shard DNE: class MultiTenantClass has no physical shard "DNE"`
		testAggregateTenantFailure(t, testClass.Class, query, expected)
	})
}

type batchParams struct {
	className  string
	tenantName string
	tenantKey  string
	batchSize  int
}

func makeTenantBatch(params batchParams) []*models.Object {
	batch := make([]*models.Object, params.batchSize)
	for i := range batch {
		batch[i] = &models.Object{
			Class: params.className,
			Properties: map[string]interface{}{
				params.tenantKey: params.tenantName,
			},
			TenantName: params.tenantName,
		}
	}
	return batch
}

func testAggregateTenantSuccess(t *testing.T, className, tenantName string, expectedCount int) {
	query := fmt.Sprintf(`{Aggregate{%s(tenantKey:%q){meta{count}}}}`, className, tenantName)
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	result := resp.Get("Aggregate", className).AsSlice()
	require.Len(t, result, 1)
	count := result[0].(map[string]any)["meta"].(map[string]any)["count"].(json.Number)
	assert.Equal(t, json.Number(fmt.Sprint(expectedCount)), count)
}

func testAggregateTenantFailure(t *testing.T, className, query, expectedMsg string) {
	resp, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
	require.Nil(t, err)
	assert.Nil(t, resp.Data["Aggregate"].(map[string]interface{})[className])
	require.Len(t, resp.Errors, 1)
	assert.Equal(t, expectedMsg, resp.Errors[0].Message)
}
