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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/distributed_tasks"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestAuthzIndexesGet covers GET /v1/schema/{class}/indexes, which requires
// READ on the collection's metadata (read_collections on that collection).
func TestAuthzIndexesGet(t *testing.T) {
	customUser := "custom-user"
	customKey := "custom-key"
	roleName := "indexes-get-role"

	_, down := composeUpShared(t)
	defer down()

	const (
		className = "AuthzIndexStatus"
		otherName = "AuthzIndexStatusOther"
	)
	for _, name := range []string{className, otherName} {
		helper.CreateClassAuth(t, &models.Class{
			Class:      name,
			Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
		}, sharedRootKey)
	}
	defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(sharedRootKey))
	defer helper.DeleteClassWithAuthz(t, otherName, helper.CreateAuth(sharedRootKey))

	tests := []struct {
		name       string
		permission *models.Permission
		allowed    bool
	}{
		{
			name:       "denied without read_collections on the collection",
			permission: helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(otherName).Permission(),
			allowed:    false,
		},
		{
			name:       "denied with write-only access to the collection",
			permission: helper.NewCollectionsPermission().WithAction(authorization.UpdateCollections).WithCollection(className).Permission(),
			allowed:    false,
		},
		{
			name:       "allowed with read_collections on the collection",
			permission: helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection(className).Permission(),
			allowed:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			helper.CreateRole(t, sharedRootKey, &models.Role{
				Name:        String(roleName),
				Permissions: []*models.Permission{test.permission},
			})
			defer helper.DeleteRole(t, sharedRootKey, roleName)
			helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
			defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

			params := schema.NewSchemaObjectsIndexesGetParams().WithClassName(className)
			resp, err := helper.Client(t).Schema.SchemaObjectsIndexesGet(params, helper.CreateAuth(customKey))
			if !test.allowed {
				var parsed *schema.SchemaObjectsIndexesGetForbidden
				require.True(t, errors.As(err, &parsed), "expected forbidden, got %v", err)
				require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp.Payload)
			require.Equal(t, className, resp.Payload.Collection)
		})
	}
}

// terminalTaskIDs returns the IDs of the tasks that have stopped moving.
func terminalTaskIDs(tasks models.DistributedTasks) []string {
	var ids []string
	for _, namespace := range tasks {
		for _, task := range namespace {
			switch task.Status {
			case "FINISHED", "FAILED", "CANCELLED":
				ids = append(ids, task.ID)
			}
		}
	}
	return ids
}

// TestAuthzDistributedTasksGet covers GET /v1/tasks, which requires READ on the
// cluster resource (read_cluster).
func TestAuthzDistributedTasksGet(t *testing.T) {
	customUser := "custom-user"
	customKey := "custom-key"
	roleName := "tasks-get-role"

	_, down := composeUpShared(t)
	defer down()

	// A real task on the list. Without one both sides of the allow arm's
	// comparison are empty, and an implementation that answered every caller
	// with an empty stand-in would pass.
	const className = "AuthzTasksReindex"
	helper.CreateClassAuth(t, &models.Class{
		Class:      className,
		Properties: []*models.Property{{Name: "score", DataType: []string{"int"}}},
	}, sharedRootKey)
	defer helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(sharedRootKey))

	accepted, err := helper.Client(t).Schema.SchemaObjectsIndexesUpdate(
		schema.NewSchemaObjectsIndexesUpdateParams().
			WithClassName(className).WithPropertyName("score").
			WithBody(&models.IndexUpdateRequest{Rangeable: &models.IndexUpdateRangeable{Enabled: true}}),
		helper.CreateAuth(sharedRootKey))
	require.NoError(t, err)
	require.NotEmpty(t, accepted.Payload.TaskID)

	// Let it reach a terminal status before comparing payloads: a task still
	// running reports progress that moves between the two calls below.
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := helper.Client(t).DistributedTasks.DistributedTasksGet(
			distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(sharedRootKey))
		if !assert.NoError(ct, err) {
			return
		}
		assert.Contains(ct, terminalTaskIDs(resp.Payload), accepted.Payload.TaskID)
	}, 2*time.Minute, time.Second, "the reindex task must reach a terminal status")

	tests := []struct {
		name       string
		permission *models.Permission
		allowed    bool
	}{
		{
			name:       "denied without read_cluster",
			permission: helper.NewNodesPermission().WithAction(authorization.ReadNodes).WithVerbosity(verbosity.OutputMinimal).Permission(),
			allowed:    false,
		},
		{
			name:       "denied with collection reads only",
			permission: helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).WithCollection("*").Permission(),
			allowed:    false,
		},
		{
			name:       "allowed with read_cluster",
			permission: &models.Permission{Action: &authorization.ReadCluster},
			allowed:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			helper.CreateRole(t, sharedRootKey, &models.Role{
				Name:        String(roleName),
				Permissions: []*models.Permission{test.permission},
			})
			defer helper.DeleteRole(t, sharedRootKey, roleName)
			helper.AssignRoleToUser(t, sharedRootKey, roleName, customUser)
			defer helper.RevokeRoleFromUser(t, sharedRootKey, roleName, customUser)

			params := distributed_tasks.NewDistributedTasksGetParams()
			resp, err := helper.Client(t).DistributedTasks.DistributedTasksGet(params, helper.CreateAuth(customKey))
			if !test.allowed {
				var parsed *distributed_tasks.DistributedTasksGetForbidden
				require.True(t, errors.As(err, &parsed), "expected forbidden, got %v", err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp.Payload)

			// read_cluster grants the whole task list, so the answer has to be
			// the one root gets, not a filtered or empty stand-in.
			rootResp, rootErr := helper.Client(t).DistributedTasks.DistributedTasksGet(
				distributed_tasks.NewDistributedTasksGetParams(), helper.CreateAuth(sharedRootKey))
			require.NoError(t, rootErr)
			require.Equal(t, rootResp.Payload, resp.Payload)
			require.Contains(t, terminalTaskIDs(resp.Payload), accepted.Payload.TaskID,
				"the comparison only discriminates while a real task is on the list")
		})
	}
}
