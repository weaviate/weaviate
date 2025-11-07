//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzReplicationReplicate(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"

	testRoleName := "testRole"
	customUser := "custom-user"
	customKey := "custom-key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	compose, err := docker.New().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		With3NodeCluster().
		WithRBAC().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithRbacRoots(adminUser).
		WithUserApiKey(customUser, customKey).
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		helper.ResetClient()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
		cancel()
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	paragraphClass := articles.ParagraphsClass()

	helper.CreateClassAuth(t, paragraphClass, adminKey)
	defer helper.DeleteClassAuth(t, paragraphClass.Class, adminKey)

	req := getReplicateRequest(t, paragraphClass.Class, adminKey)

	helper.CreateRole(t, adminKey, &models.Role{
		Name:        &testRoleName,
		Permissions: []*models.Permission{},
	})
	defer helper.DeleteRole(t, adminKey, testRoleName)

	helper.AssignRoleToUser(t, adminKey, testRoleName, customUser)
	defer helper.RevokeRoleFromUser(t, adminKey, testRoleName, customUser)

	createReplication := &models.Permission{
		Action: &authorization.CreateReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.Collection,
			Shard:      req.Shard,
		},
	}
	readReplication := &models.Permission{
		Action: &authorization.ReadReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.Collection,
			Shard:      req.Shard,
		},
	}
	updateReplication := &models.Permission{
		Action: &authorization.UpdateReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.Collection,
			Shard:      req.Shard,
		},
	}
	deleteReplication := &models.Permission{
		Action: &authorization.DeleteReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.Collection,
			Shard:      req.Shard,
		},
	}

	var replicationId strfmt.UUID

	t.Run("Fail to replicate a shard without CREATE permissions", func(t *testing.T) {
		_, err := helper.Client(t).Replication.Replicate(replication.NewReplicateParams().WithBody(req), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		require.IsType(t, replication.NewReplicateForbidden(), err)
	})

	// Give permissions to replicate a shard
	helper.AddPermissions(t, adminKey, testRoleName, createReplication)

	t.Run("Replicate a shard with permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				Replicate(replication.NewReplicateParams().WithBody(req), helper.CreateAuth(customKey))
			require.Nil(ct, err)
			replicationId = *resp.Payload.ID
		}, 10*time.Second, 500*time.Millisecond, "op should be started but got error replicating: %s", err.Error())
	})

	t.Run("Fail to cancel a replication of a shard without UPDATE permissions", func(t *testing.T) {
		_, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(replicationId), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		require.IsType(t, replication.NewCancelReplicationForbidden(), err, "expected forbidden error, got: %s", err.Error())
	})

	// Give permissions to cancel a replication of a shard
	helper.AddPermissions(t, adminKey, testRoleName, updateReplication)

	t.Run("Cancel a replication of a shard with permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				CancelReplication(replication.NewCancelReplicationParams().WithID(replicationId), helper.CreateAuth(customKey))
			require.Nil(ct, err)
			require.IsType(ct, replication.NewCancelReplicationNoContent(), resp)
		}, 10*time.Second, 500*time.Millisecond, "op should be cancelled but got error: %s", err.Error())
	})

	t.Run("Fail to read a replication of a shard without READ permissions", func(t *testing.T) {
		_, err := helper.Client(t).Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithID(replicationId), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		require.IsType(t, replication.NewReplicationDetailsForbidden(), err)
	})

	// Give permissions to read a replication of a shard
	helper.AddPermissions(t, adminKey, testRoleName, readReplication)

	t.Run("Read a replication of a shard with permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				ReplicationDetails(replication.NewReplicationDetailsParams().WithID(replicationId), helper.CreateAuth(customKey))
			require.Nil(ct, err)
			require.Equal(ct, *resp.Payload.ID, replicationId)
		}, 10*time.Second, 500*time.Millisecond, "op should be read but got error: %s", err.Error())
	})

	t.Run("Fail to delete a replication of a shard without DELETE permissions", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(replication.NewDeleteReplicationParams().WithID(replicationId), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		require.IsType(t, replication.NewDeleteReplicationForbidden(), err)
	})

	// Give permissions to delete a replication of a shard
	helper.AddPermissions(t, adminKey, testRoleName, deleteReplication)

	t.Run("Delete a replication of a shard with permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				DeleteReplication(replication.NewDeleteReplicationParams().WithID(replicationId), helper.CreateAuth(customKey))
			require.Nil(ct, err)
			require.IsType(ct, replication.NewDeleteReplicationNoContent(), resp)
		}, 10*time.Second, 500*time.Millisecond, "op should be deleted but got error: %s", err.Error())
	})
}

func getReplicateRequest(t *testing.T, className, key string) *models.ReplicationReplicateReplicaRequest {
	verbose := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), helper.CreateAuth(key))
	require.Nil(t, err)
	return &models.ReplicationReplicateReplicaRequest{
		Collection: &className,
		SourceNode: &nodes.Payload.Nodes[0].Name,
		TargetNode: &nodes.Payload.Nodes[1].Name,
		Shard:      &nodes.Payload.Nodes[0].Shards[0].Name,
	}
}
