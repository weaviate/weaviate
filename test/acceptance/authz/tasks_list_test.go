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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/distributed_tasks"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzListTasksFiltersByCollection(t *testing.T) {
	adminUser, adminKey := "admin-user", "admin-key"
	customUser, customKey := "custom-user", "custom-key"
	adminAuth := helper.CreateAuth(adminKey)
	readable, hidden := "AuthzTasksReadable", "AuthzTasksHidden"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	compose, err := docker.New().WithWeaviate().
		WithWeaviateEnv("RUNTIME_REINDEX_ENABLED", "true").
		WithRBAC().WithApiKey().WithRbacRoots(adminUser).
		WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		Start(ctx)
	require.NoError(t, err)
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()
	defer compose.Terminate(ctx)

	for _, class := range []string{readable, hidden} {
		helper.CreateClassAuth(t, &models.Class{
			Class:              class,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			Properties: []*models.Property{
				{Name: "title", DataType: schema.DataTypeText.PropString()},
			},
		}, adminKey)
		helper.CreateTenantsAuth(t, class, []*models.Tenant{{Name: "SecretTenant", ActivityStatus: "HOT"}}, adminKey)

		params := clschema.NewSchemaObjectsIndexRebuildParams().
			WithClassName(class).WithPropertyName("title").WithIndexName("filterable").
			WithTenants([]string{"SecretTenant"})
		_, err = helper.Client(t).Schema.SchemaObjectsIndexRebuild(params, adminAuth)
		require.NoError(t, err)
	}

	tasksFor := func(t require.TestingT, key, class string) []models.DistributedTask {
		resp, err := helper.Client(nil).DistributedTasks.DistributedTasksGet(
			distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(key))
		require.NoError(t, err)
		var found []models.DistributedTask
		for _, task := range resp.Payload["reindex"] {
			if task.Payload.(map[string]interface{})["collection"] == class {
				found = append(found, task)
			}
		}
		return found
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Len(collect, tasksFor(collect, adminKey, readable), 1)
		assert.Len(collect, tasksFor(collect, adminKey, hidden), 1)
	}, 30*time.Second, 500*time.Millisecond)

	_, err = helper.Client(t).DistributedTasks.DistributedTasksGet(
		distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(customKey))
	var forbidden *distributed_tasks.DistributedTasksGetForbidden
	require.ErrorAs(t, err, &forbidden, "listing tasks needs read_cluster")

	roleName := "tasksReadableCollectionOnly"
	helper.CreateRole(t, adminKey, &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{
			{Action: &authorization.ReadCluster},
			helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(readable).Permission(),
		},
	})
	helper.AssignRoleToUser(t, adminKey, roleName, customUser)

	require.Empty(t, tasksFor(t, customKey, hidden), "a task on an unreadable collection must be left out")
	visible := tasksFor(t, customKey, readable)
	require.Len(t, visible, 1)
	require.NotContains(t, visible[0].Payload, "tenants", "tenant names need tenant read")
	require.NotContains(t, visible[0].Payload, "unitToShard", "shard names need tenant read")
	for _, unit := range visible[0].Units {
		require.Empty(t, unit.ID, "unit IDs embed the shard name")
	}

	helper.AddPermissions(t, adminKey, roleName,
		helper.NewTenantsPermission().WithAction(authorization.ReadTenants).WithCollection(readable).Permission())
	visible = tasksFor(t, customKey, readable)
	require.Len(t, visible, 1)
	require.Equal(t, []interface{}{"SecretTenant"}, visible[0].Payload.(map[string]interface{})["tenants"])
}
