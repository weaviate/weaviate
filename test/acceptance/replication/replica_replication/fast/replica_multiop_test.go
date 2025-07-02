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

package replication

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

type ReplicationMultiOpTestSuite struct {
	suite.Suite
}

func (suite *ReplicationMultiOpTestSuite) SetupSuite() {
	t := suite.T()
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
}

func TestReplicationMultiOpTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationMultiOpTestSuite))
}

func (suite *ReplicationMultiOpTestSuite) TestReplicaMovementHappyPath() {
	t := suite.T()
	mainCtx := context.Background()

	clusterSize := 5

	compose, err := docker.New().
		WithWeaviateCluster(clusterSize).
		WithText2VecContextionary().
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(mainCtx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(mainCtx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	testClass := models.Class{
		Class: "MultiTenantClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:            true,
			AutoTenantCreation: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor: 2,
		},
	}
	helper.CreateClass(t, &testClass)

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	tenantsCount := 100
	objectsPerTenant := 3

	for i := 0; i < tenantsCount; i++ {
		tenantName := fmt.Sprintf("tenant-%d", i)
		tenantObjects := make([]*models.Object, objectsPerTenant)

		for j := 0; j < objectsPerTenant; j++ {
			tenantObjects[j] = &models.Object{
				Class: testClass.Class,
				Properties: map[string]interface{}{
					"name": fmt.Sprintf("Object %d for %s", j, tenantName),
				},
				Tenant: tenantName,
			}
		}

		common.CreateObjects(t, compose.GetWeaviate().URI(), tenantObjects)
	}

	// for each tenant, find the two nodes assigned to the tenant
	tenantNodes := make(map[string][]string)

	nodesParams := nodes.NewNodesGetClassParams().WithClassName(testClass.Class)
	nodesResp, err := helper.Client(t).Nodes.NodesGetClass(nodesParams, nil)
	require.NoError(t, err)
	require.NotNil(t, nodesResp.Payload)

	nodes := nodesResp.Payload.Nodes

	for _, node := range nodes {
		for _, shard := range node.Shards {
			if shard.Class != testClass.Class {
				continue
			}

			tenantNodes[shard.Name] = append(tenantNodes[shard.Name], node.Name)
		}
	}

	for tenant, assignedNodes := range tenantNodes {
		require.Len(t, assignedNodes, 2, "tenant %s should have exactly two nodes assigned, got %d", tenant, len(assignedNodes))

		// find the two nodes in nodesResp.Payload.Nodes not in assignedNodes
		var nonAssignedNodes []string
		for _, node := range nodesResp.Payload.Nodes {
			if node.Name != assignedNodes[0] && node.Name != assignedNodes[1] {
				nonAssignedNodes = append(nonAssignedNodes, node.Name)
			}
		}
		require.Len(t, nonAssignedNodes, clusterSize-len(assignedNodes), "there should be %d non-assigned nodes, got %d", clusterSize-len(assignedNodes), len(nonAssignedNodes))

		// start two replica replication operations, one from each of the assigned nodes using the non-assigned nodes as targets
		// each operation should use a different target node and a different source node
		// using a function that does the actual replication operation
		opIds := make([]strfmt.UUID, 0, len(assignedNodes))

		for i, sourceNode := range assignedNodes {
			targetNode := nonAssignedNodes[i]

			// start the replica movement operation
			opID, err := startReplicaMovementOperation(t, compose.GetWeaviate().URI(), testClass.Class, tenant, sourceNode, targetNode)
			require.NoError(t, err, "failed to start replica movement operation from %s to %s for tenant %s", sourceNode, targetNode, tenant)

			opIds = append(opIds, opID)
		}

		// wait for the replica movement operations to complete
		for i, sourceNode := range assignedNodes {
			targetNode := nonAssignedNodes[i]

			// wait for the replica movement operation to complete
			waitForReplicaMovementOperationToComplete(t, compose.GetWeaviate().URI(), testClass.Class, sourceNode, targetNode, tenant, opIds[i])
		}
	}
}

func startReplicaMovementOperation(t *testing.T, uri, className, tenant, sourceNode, targetNode string) (strfmt.UUID, error) {
	resp, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(
			&models.ReplicationReplicateReplicaRequest{
				CollectionID:        &className,
				SourceNodeName:      &sourceNode,
				DestinationNodeName: &targetNode,
				ShardID:             &tenant,
			},
		),
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.ID)
	require.NotEmpty(t, *resp.Payload.ID)

	return *resp.Payload.ID, nil
}

func waitForReplicaMovementOperationToComplete(t *testing.T, uri, className, sourceNode, targetNode, tenant string, opID strfmt.UUID) {
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		details, err := helper.Client(t).Replication.ReplicationDetails(
			replication.NewReplicationDetailsParams().WithID(opID), nil,
		)
		require.Nil(t, err, "failed to get replication details %s", err)
		require.NotNil(t, details, "expected replication details to be not nil")
		require.NotNil(t, details.Payload, "expected replication details payload to be not nil")
		require.NotNil(t, details.Payload.Status, "expected replication status to be not nil")
		require.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
	}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", opID)
}
