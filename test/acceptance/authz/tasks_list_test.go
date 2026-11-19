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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/distributed_tasks"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzListTasksFiltersByCollection(t *testing.T) {
	customUser, customKey := "custom-user", "custom-key"
	readable, hidden := "AuthzTasksReadable", "AuthzTasksHidden"

	_, down := composeUpShared(t)
	defer down()

	startReindex := func(class string) {
		helper.CreateClassAuth(t, &models.Class{
			Class:              class,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			Properties: []*models.Property{
				{Name: "title", DataType: schema.DataTypeText.PropString()},
			},
		}, sharedRootKey)
		helper.CreateTenantsAuth(t, class, []*models.Tenant{{Name: "SecretTenant", ActivityStatus: "HOT"}}, sharedRootKey)

		params := clschema.NewSchemaObjectsIndexRebuildParams().
			WithClassName(class).WithPropertyName("title").WithIndexName("filterable").
			WithTenants([]string{"SecretTenant"})
		_, err := helper.Client(t).Schema.SchemaObjectsIndexRebuild(params, helper.CreateAuth(sharedRootKey))
		require.NoError(t, err)
	}
	reindexTasks := func(t require.TestingT, key string) []models.DistributedTask {
		resp, err := helper.Client(nil).DistributedTasks.DistributedTasksGet(
			distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(key))
		require.NoError(t, err)
		return resp.Payload["reindex"]
	}
	tasksFor := func(t require.TestingT, key, class string) []models.DistributedTask {
		var found []models.DistributedTask
		for _, task := range reindexTasks(t, key) {
			if task.Payload.(map[string]interface{})["collection"] == class {
				found = append(found, task)
			}
		}
		return found
	}
	// down() can't delete a collection whose reindex task is still active, and doesn't retry.
	defer assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		for _, task := range reindexTasks(collect, sharedRootKey) {
			assert.True(collect, distributedtask.TaskStatus(task.Status).IsTerminal(), "task %s is %s", task.ID, task.Status)
		}
	}, time.Minute, 500*time.Millisecond)

	startReindex(readable)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Len(collect, reindexTasks(collect, sharedRootKey), 1)
	}, 30*time.Second, 500*time.Millisecond, "the tenant-only check needs this to be the only reindex task")

	_, err := helper.Client(t).DistributedTasks.DistributedTasksGet(
		distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(customKey))
	var forbidden *distributed_tasks.DistributedTasksGetForbidden
	require.ErrorAs(t, err, &forbidden, "listing tasks needs read_cluster")

	tenantsOnly := "tasksReadableTenantsOnly"
	helper.CreateRole(t, sharedRootKey, &models.Role{
		Name: &tenantsOnly,
		Permissions: []*models.Permission{
			{Action: &authorization.ReadCluster},
			helper.NewTenantsPermission().WithAction(authorization.ReadTenants).WithCollection(readable).Permission(),
		},
	})
	helper.AssignRoleToUser(t, sharedRootKey, tenantsOnly, customUser)
	require.Empty(t, tasksFor(t, customKey, readable), "tenant read alone must not reveal the collection's task")
	helper.DeleteRole(t, sharedRootKey, tenantsOnly)

	startReindex(hidden)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Len(collect, tasksFor(collect, sharedRootKey, hidden), 1)
	}, 30*time.Second, 500*time.Millisecond)

	collectionOnly := "tasksReadableCollectionOnly"
	helper.CreateRole(t, sharedRootKey, &models.Role{
		Name: &collectionOnly,
		Permissions: []*models.Permission{
			{Action: &authorization.ReadCluster},
			helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(readable).Permission(),
		},
	})
	helper.AssignRoleToUser(t, sharedRootKey, collectionOnly, customUser)

	require.Empty(t, tasksFor(t, customKey, hidden), "a task on an unreadable collection must be left out")
	visible := tasksFor(t, customKey, readable)
	require.Len(t, visible, 1)
	require.NotContains(t, visible[0].Payload, "tenants", "tenant names need tenant read")
	require.NotContains(t, visible[0].Payload, "unitToShard", "shard names need tenant read")
	for _, unit := range visible[0].Units {
		require.Empty(t, unit.ID, "unit IDs embed the shard name")
	}

	helper.AddPermissions(t, sharedRootKey, collectionOnly,
		helper.NewTenantsPermission().WithAction(authorization.ReadTenants).WithCollection(readable).Permission())
	visible = tasksFor(t, customKey, readable)
	require.Len(t, visible, 1)
	require.Equal(t, []interface{}{"SecretTenant"}, visible[0].Payload.(map[string]interface{})["tenants"])
}
