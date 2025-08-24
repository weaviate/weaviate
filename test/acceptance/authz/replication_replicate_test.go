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

package authz

import (
	"context"
	"errors"
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
			Collection: req.CollectionID,
			Shard:      req.ShardID,
		},
	}
	readReplication := &models.Permission{
		Action: &authorization.ReadReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.CollectionID,
			Shard:      req.ShardID,
		},
	}
	updateReplication := &models.Permission{
		Action: &authorization.UpdateReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.CollectionID,
			Shard:      req.ShardID,
		},
	}
	deleteReplication := &models.Permission{
		Action: &authorization.DeleteReplicate,
		Replicate: &models.PermissionReplicate{
			Collection: req.CollectionID,
			Shard:      req.ShardID,
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
			if err != nil && !errors.Is(err, replication.NewReplicateForbidden()) {
				t.Fatalf("failed to replicate shard: %v", err)
			}
			require.Nil(ct, err)
			replicationId = *resp.Payload.ID
		}, 2*time.Second, 100*time.Millisecond, "op should be started")
	})

	t.Run("Fail to cancel a replication of a shard without UPDATE permissions", func(t *testing.T) {
		_, err := helper.Client(t).Replication.CancelReplication(replication.NewCancelReplicationParams().WithID(replicationId), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		require.IsType(t, replication.NewCancelReplicationForbidden(), err)
	})

	// Give permissions to cancel a replication of a shard
	helper.AddPermissions(t, adminKey, testRoleName, updateReplication)

	t.Run("Cancel a replication of a shard with permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Replication.
				CancelReplication(replication.NewCancelReplicationParams().WithID(replicationId), helper.CreateAuth(customKey))
			if err != nil && !errors.Is(err, replication.NewCancelReplicationForbidden()) {
				t.Fatalf("failed to cancel replication: %v", err)
			}
			require.Nil(ct, err)
			require.IsType(ct, replication.NewCancelReplicationNoContent(), resp)
		}, 2*time.Second, 100*time.Millisecond, "op should be cancelled")
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
			if err != nil && !errors.Is(err, replication.NewReplicationDetailsForbidden()) {
				t.Fatalf("failed to read replication: %v", err)
			}
			require.Nil(ct, err)
			require.Equal(ct, *resp.Payload.ID, replicationId)
		}, 2*time.Second, 100*time.Millisecond, "op should be read")
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
			if err != nil && !errors.Is(err, replication.NewDeleteReplicationForbidden()) {
				t.Fatalf("failed to delete replication: %v", err)
			}
			require.Nil(ct, err)
			require.IsType(ct, replication.NewDeleteReplicationNoContent(), resp)
		}, 2*time.Second, 100*time.Millisecond, "op should be deleted")
	})
}

func getReplicateRequest(t *testing.T, className, key string) *models.ReplicationReplicateReplicaRequest {
	verbose := verbosity.OutputVerbose
	nodes, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), helper.CreateAuth(key))
	require.Nil(t, err)
	return &models.ReplicationReplicateReplicaRequest{
		CollectionID:        &className,
		SourceNodeName:      &nodes.Payload.Nodes[0].Name,
		DestinationNodeName: &nodes.Payload.Nodes[1].Name,
		ShardID:             &nodes.Payload.Nodes[0].Shards[0].Name,
	}
}
