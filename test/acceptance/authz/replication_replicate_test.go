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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzReplicationReplicate(t *testing.T) {
	adminKey := "admin-key"

	testRoleName := "testRole"
	customUser := "custom-user"
	customKey := "custom-key"

	_, down := composeUpSharedCluster(t)
	defer down()

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
		}, 10*time.Second, 500*time.Millisecond, "op should be started but got error replicating")
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
		}, 10*time.Second, 500*time.Millisecond, "op should be cancelled but got error")
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
		}, 10*time.Second, 500*time.Millisecond, "op should be read but got error")
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
		}, 10*time.Second, 500*time.Millisecond, "op should be deleted but got error")
	})

	var (
		errListForbidden          *replication.ListReplicationForbidden
		errForceDeleteForbidden   *replication.ForceDeleteReplicationsForbidden
		errShardingStateForbidden *replication.GetCollectionShardingStateForbidden
		errScalePlanForbidden     *replication.GetReplicationScalePlanForbidden
		errApplyScaleForbidden    *replication.ApplyReplicationScalePlanForbidden
	)

	wildcardRead := &models.Permission{
		Action:    &authorization.ReadReplicate,
		Replicate: &models.PermissionReplicate{},
	}
	wildcardUpdate := &models.Permission{
		Action:    &authorization.UpdateReplicate,
		Replicate: &models.PermissionReplicate{Collection: req.Collection},
	}
	wildcardDelete := &models.Permission{
		Action:    &authorization.DeleteReplicate,
		Replicate: &models.PermissionReplicate{},
	}
	helper.AddPermissions(t, adminKey, testRoleName, wildcardRead, wildcardUpdate, wildcardDelete)

	t.Run("list replications with READ permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ListReplication(replication.NewListReplicationParams(), helper.CreateAuth(customKey))
			require.NotErrorAs(ct, err, &errListForbidden)
		}, 10*time.Second, 500*time.Millisecond, "list should not be forbidden")
	})

	t.Run("force-delete replications with DELETE permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ForceDeleteReplications(replication.NewForceDeleteReplicationsParams(), helper.CreateAuth(customKey))
			require.NotErrorAs(ct, err, &errForceDeleteForbidden)
		}, 10*time.Second, 500*time.Millisecond, "force-delete should not be forbidden")
	})

	t.Run("get sharding state with READ permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.GetCollectionShardingState(
				replication.NewGetCollectionShardingStateParams().WithCollection(req.Collection), helper.CreateAuth(customKey))
			require.NotErrorAs(ct, err, &errShardingStateForbidden)
		}, 10*time.Second, 500*time.Millisecond, "sharding-state should not be forbidden")
	})

	t.Run("get scale plan with READ permissions", func(t *testing.T) {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.GetReplicationScalePlan(
				replication.NewGetReplicationScalePlanParams().WithCollection(*req.Collection).WithReplicationFactor(1), helper.CreateAuth(customKey))
			require.NotErrorAs(ct, err, &errScalePlanForbidden)
		}, 10*time.Second, 500*time.Millisecond, "scale plan should not be forbidden")
	})

	t.Run("apply scale plan with UPDATE permissions", func(t *testing.T) {
		planID := strfmt.UUID(uuid.NewString())
		body := &models.ReplicationScalePlan{
			PlanID:            planID,
			Collection:        *req.Collection,
			ShardScaleActions: map[string]models.ReplicationScalePlanShardScaleActionsAnon{},
		}
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, err := helper.Client(t).Replication.ApplyReplicationScalePlan(
				replication.NewApplyReplicationScalePlanParams().WithBody(body), helper.CreateAuth(customKey))
			require.NotErrorAs(ct, err, &errApplyScaleForbidden)
		}, 10*time.Second, 500*time.Millisecond, "apply scale plan should not be forbidden")
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
