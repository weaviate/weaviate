//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZGraphQLAggregateThroughAlias(t *testing.T) {
	adminKey := sharedRootKey
	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUpShared(t)
	defer down()

	target := "HRSecret"

	helper.CreateClassAuth(t, &models.Class{
		Class: target,
		Properties: []*models.Property{
			{Name: "name", DataType: schema.DataTypeText.PropString()},
			{Name: "salary", DataType: schema.DataTypeInt.PropString()},
		},
	}, adminKey)
	helper.CreateObjectsBatchAuth(t, []*models.Object{
		{Class: target, Properties: map[string]interface{}{"name": "a", "salary": 50000}},
		{Class: target, Properties: map[string]interface{}{"name": "b", "salary": 72000}},
		{Class: target, Properties: map[string]interface{}{"name": "c", "salary": 91000}},
	}, adminKey)
	helper.CreateAliasAuth(t, &models.Alias{Alias: "PublicView", Class: target}, adminKey)

	targetMT := "HRSecretMT"
	helper.CreateClassAuth(t, &models.Class{
		Class:              targetMT,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: "salary", DataType: schema.DataTypeInt.PropString()}},
	}, adminKey)
	helper.CreateTenantsAuth(t, targetMT, []*models.Tenant{{Name: "t1"}}, adminKey)
	helper.CreateObjectsBatchAuth(t, []*models.Object{
		{Class: targetMT, Tenant: "t1", Properties: map[string]interface{}{"salary": 50000}},
	}, adminKey)
	helper.CreateAliasAuth(t, &models.Alias{Alias: "PublicViewMT", Class: targetMT}, adminKey)

	queries := []struct {
		name  string
		root  string
		alias string
		query string
		// want is meta.count when metaCount is set, else the number of rows
		want      int
		metaCount bool
	}{
		{"aggregate meta", "Aggregate", "PublicView", `{ Aggregate { PublicView { meta { count } } } }`, 3, true},
		{"aggregate groupBy", "Aggregate", "PublicView", `{ Aggregate { PublicView(groupBy: ["salary"]) { groupedBy { value } meta { count } } } }`, 3, false},
		{"aggregate where", "Aggregate", "PublicView", `{ Aggregate { PublicView(where: {path: ["salary"], operator: GreaterThan, valueInt: 60000}) { meta { count } } } }`, 2, true},
		{"aggregate tenant", "Aggregate", "PublicViewMT", `{ Aggregate { PublicViewMT(tenant: "t1") { meta { count } } } }`, 1, true},
		{"get", "Get", "PublicView", `{ Get { PublicView { salary } } }`, 3, false},
		{"get where", "Get", "PublicView", `{ Get { PublicView(where: {path: ["salary"], operator: GreaterThan, valueInt: 60000}) { salary } } }`, 2, false},
	}

	// GraphQL requires read_collections on every collection, so only read_data differs between the roles.
	readCollections := helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission()

	t.Run("read_data on the alias name only", func(t *testing.T) {
		helper.CreateRoleAndAssign(t, adminKey, customUser, "alias-name-reader", readCollections,
			helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection("Public*").Permission())

		for _, q := range queries {
			t.Run("forbidden: "+q.name, func(t *testing.T) {
				resp, err := queryGQL(t, q.query, customKey)
				require.NoError(t, err)
				require.Len(t, resp.Payload.Errors, 1, "expected a forbidden error, got data %v", resp.Payload.Data)
				require.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
			})
		}
	})

	t.Run("read_data on the target only", func(t *testing.T) {
		helper.CreateRoleAndAssign(t, adminKey, customUser, "target-reader", readCollections,
			helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(target+"*").Permission())

		for _, q := range queries {
			t.Run("allowed: "+q.name, func(t *testing.T) {
				resp := assertGQL(t, q.query, customKey)
				rows := resp.Data[q.root].(map[string]interface{})[q.alias].([]interface{})
				if !q.metaCount {
					require.Len(t, rows, q.want)
					return
				}
				require.Len(t, rows, 1)
				count := rows[0].(map[string]interface{})["meta"].(map[string]interface{})["count"]
				require.Equal(t, json.Number(strconv.Itoa(q.want)), count)
			})
		}
	})
}
