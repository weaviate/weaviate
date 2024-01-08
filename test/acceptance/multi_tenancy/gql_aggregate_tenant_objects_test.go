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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestGQLAggregateTenantObjects(t *testing.T) {
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
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
	helper.CreateClass(t, &testClass)

	tenants := []*models.Tenant{
		{Name: tenantName1},
		{Name: tenantName2},
	}
	helper.CreateTenants(t, testClass.Class, tenants)

	batch1 := makeTenantBatch(batchParams{
		className:  testClass.Class,
		tenantName: tenantName1,
		batchSize:  numTenantObjs1,
	})
	batch2 := makeTenantBatch(batchParams{
		className:  testClass.Class,
		tenantName: tenantName2,
		batchSize:  numTenantObjs2,
	})

	helper.CreateObjectsBatch(t, batch1)
	helper.CreateObjectsBatch(t, batch2)

	t.Run("GQL Aggregate tenant objects", func(t *testing.T) {
		testAggregateTenantSuccess(t, testClass.Class, tenantName1, numTenantObjs1, "")
		testAggregateTenantSuccess(t, testClass.Class, tenantName2, numTenantObjs2, "")
	})

	t.Run("GQL Aggregate tenant objects near object", func(t *testing.T) {
		testAggregateTenantSuccess(t, testClass.Class, tenantName1, numTenantObjs1, string(batch1[0].ID))
		testAggregateTenantSuccess(t, testClass.Class, tenantName2, numTenantObjs2, string(batch2[0].ID))
	})

	t.Run("Get global tenant objects count", func(t *testing.T) {
		params := nodes.NewNodesGetClassParams().WithClassName(testClass.Class).WithOutput(&verbose)
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

func TestGQLAggregateTenantObjects_InvalidTenant(t *testing.T) {
	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
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
				{Name: tenantName},
			}
			helper.CreateTenants(t, testClass.Class, tenants)
		})

		t.Run("add tenant objects", func(t *testing.T) {
			batch := makeTenantBatch(batchParams{
				className:  testClass.Class,
				tenantName: tenantName,
				batchSize:  numTenantObjs,
			})
			helper.CreateObjectsBatch(t, batch)
		})
	})

	t.Run("non-existent tenant key", func(t *testing.T) {
		query := fmt.Sprintf(`{Aggregate{%s(tenant:"DNE"){meta{count}}}}`, testClass.Class)
		expected := `"DNE"`
		resp, err := graphqlhelper.QueryGraphQL(t, helper.RootAuth, "", query, nil)
		require.Nil(t, err)
		assert.Nil(t, resp.Data["Aggregate"].(map[string]interface{})[testClass.Class])
		assert.Len(t, resp.Errors, 1)
		assert.Contains(t, resp.Errors[0].Message, expected)
	})
}

type batchParams struct {
	className  string
	tenantName string
	batchSize  int
}

func makeTenantBatch(params batchParams) []*models.Object {
	batch := make([]*models.Object, params.batchSize)
	for i := range batch {
		batch[i] = &models.Object{
			ID:    strfmt.UUID(uuid.New().String()),
			Class: params.className,
			Properties: map[string]interface{}{
				"name": params.tenantName,
			},
			Tenant: params.tenantName,
		}
	}
	return batch
}

func testAggregateTenantSuccess(t *testing.T, className, tenantName string, expectedCount int, nearObjectId string) {
	nearObject := ""
	if nearObjectId != "" {
		nearObject = fmt.Sprintf(`nearObject: {id: "%s", certainty: 0.4},`, nearObjectId)
	}

	query := fmt.Sprintf(`{Aggregate{%s(%s,tenant:%q){meta{count}}}}`, className, nearObject, tenantName)
	resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	result := resp.Get("Aggregate", className).AsSlice()
	require.Len(t, result, 1)
	count := result[0].(map[string]any)["meta"].(map[string]any)["count"].(json.Number)
	assert.Equal(t, json.Number(fmt.Sprint(expectedCount)), count)
}
