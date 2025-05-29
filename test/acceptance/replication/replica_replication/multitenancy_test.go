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

// import (
// 	"context"
// 	"fmt"
// 	"net/http"
// 	"testing"
// 	"time"

// 	"github.com/go-openapi/strfmt"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// 	"github.com/stretchr/testify/suite"
// 	"github.com/weaviate/weaviate/client/nodes"
// 	"github.com/weaviate/weaviate/client/replication"
// 	"github.com/weaviate/weaviate/cluster/proto/api"
// 	"github.com/weaviate/weaviate/cluster/router/types"
// 	"github.com/weaviate/weaviate/entities/models"
// 	"github.com/weaviate/weaviate/entities/verbosity"
// 	"github.com/weaviate/weaviate/test/acceptance/replication/common"
// 	"github.com/weaviate/weaviate/test/docker"
// 	"github.com/weaviate/weaviate/test/helper"
// 	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
// )

// type ReplicaMultitenancyTestSuite struct {
// 	suite.Suite
// }

// func (suite *ReplicaMultitenancyTestSuite) SetupTest() {
// 	suite.T().Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")
// }

// func TestReplicaMultitenancyTestSuite(t *testing.T) {
// 	suite.Run(t, new(ReplicaMultitenancyTestSuite))
// }

// func (suite *ReplicaMultitenancyTestSuite) TestReplicaMovementActivatedTenant() {
// 	t := suite.T()
// 	mainCtx := context.Background()

// 	var (
// 		clusterSize = 3
// 		tenantName  = "tenant-0"
// 	)

// 	compose, err := docker.New().
// 		WithWeaviateCluster(clusterSize).
// 		WithText2VecContextionary().
// 		Start(mainCtx)
// 	require.NoError(t, err)

// 	defer func() {
// 		if err := compose.Terminate(mainCtx); err != nil {
// 			t.Fatalf("failed to terminate test containers: %s", err.Error())
// 		}
// 	}()

// 	helper.SetupClient(compose.GetWeaviate().URI())
// 	paragraphClass := articles.ParagraphsClass()

// 	t.Run("create schema", func(t *testing.T) {
// 		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
// 			Factor:       int64(clusterSize - 1),
// 			AsyncEnabled: false,
// 		}
// 		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
// 			AutoTenantCreation:   false,
// 			AutoTenantActivation: false,
// 			Enabled:              true,
// 		}
// 		paragraphClass.Vectorizer = "text2vec-contextionary"
// 		helper.CreateClass(t, paragraphClass)
// 	})

// 	t.Run("create tenant", func(t *testing.T) {
// 		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "HOT"}}
// 		helper.CreateTenants(t, paragraphClass.Class, tenants)
// 	})

// 	t.Run("insert paragraphs", func(t *testing.T) {
// 		batch := make([]*models.Object, len(paragraphIDs))
// 		for i, id := range paragraphIDs {
// 			batch[i] = articles.NewParagraph().
// 				WithID(id).
// 				WithContents(fmt.Sprintf("paragraph#%d", i)).
// 				WithTenant(tenantName).
// 				Object()
// 		}
// 		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
// 	})

// 	var sourceNode, targetNode string
// 	var targetNodeID int

// 	t.Run("find source and target nodes", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		for i, node := range body.Payload.Nodes {
// 			if len(node.Shards) == 0 {
// 				targetNode = node.Name
// 				targetNodeID = i + 1
// 			} else {
// 				sourceNode = node.Name
// 			}
// 		}

// 		require.NotEmpty(t, sourceNode)
// 		require.NotEmpty(t, targetNode)
// 	})

// 	transferType := api.COPY.String()

// 	var uuid strfmt.UUID

// 	t.Run(fmt.Sprintf("start %q of %s from %s to %s for paragraph", transferType, tenantName, sourceNode, targetNode), func(t *testing.T) {
// 		resp, err := helper.Client(t).Replication.Replicate(
// 			replication.NewReplicateParams().WithBody(
// 				&models.ReplicationReplicateReplicaRequest{
// 					CollectionID:        &paragraphClass.Class,
// 					SourceNodeName:      &sourceNode,
// 					DestinationNodeName: &targetNode,
// 					ShardID:             &tenantName,
// 					TransferType:        &transferType,
// 				},
// 			),
// 			nil,
// 		)
// 		require.NoError(t, err)
// 		require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
// 		uuid = *resp.Payload.ID
// 	})

// 	t.Run("waiting for replication to finish", func(t *testing.T) {
// 		require.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			details, err := helper.Client(t).Replication.ReplicationDetails(
// 				replication.NewReplicationDetailsParams().WithID(uuid), nil,
// 			)
// 			require.Nil(ct, err, "failed to get replication details %s", err)
// 			require.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
// 		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", uuid)
// 	})

// 	t.Run("validate target node belongs to the tenant replication group", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		var targetNodeFound bool

// 		for _, node := range body.Payload.Nodes {
// 			if targetNode == node.Name {
// 				targetNodeFound = true
// 				break
// 			}
// 		}

// 		require.True(t, targetNodeFound)
// 	})

// 	t.Run("assert data is available for paragraph on target node with consistency level one", func(t *testing.T) {
// 		require.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(targetNodeID).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
// 			require.Nil(ct, err)
// 			require.Len(ct, resp, len(paragraphIDs))
// 		}, 10*time.Second, 1*time.Second, "target node doesn't have paragraph data")
// 	})
// }

// func (suite *ReplicaMultitenancyTestSuite) TestReplicaMovementDeactivatedTenantWithAutoTenantActivation() {
// 	t := suite.T()
// 	mainCtx := context.Background()

// 	var (
// 		clusterSize = 3
// 		tenantName  = "tenant-0"
// 	)

// 	compose, err := docker.New().
// 		WithWeaviateCluster(clusterSize).
// 		WithText2VecContextionary().
// 		Start(mainCtx)
// 	require.NoError(t, err)

// 	defer func() {
// 		if err := compose.Terminate(mainCtx); err != nil {
// 			t.Fatalf("failed to terminate test containers: %s", err.Error())
// 		}
// 	}()

// 	helper.SetupClient(compose.GetWeaviate().URI())
// 	paragraphClass := articles.ParagraphsClass()

// 	t.Run("create schema", func(t *testing.T) {
// 		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
// 			Factor:       int64(clusterSize - 1),
// 			AsyncEnabled: false,
// 		}
// 		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
// 			AutoTenantCreation:   false,
// 			AutoTenantActivation: true,
// 			Enabled:              true,
// 		}
// 		paragraphClass.Vectorizer = "text2vec-contextionary"
// 		helper.CreateClass(t, paragraphClass)
// 	})

// 	t.Run("create tenant", func(t *testing.T) {
// 		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "HOT"}}
// 		helper.CreateTenants(t, paragraphClass.Class, tenants)
// 	})

// 	t.Run("insert paragraphs", func(t *testing.T) {
// 		batch := make([]*models.Object, len(paragraphIDs))
// 		for i, id := range paragraphIDs {
// 			batch[i] = articles.NewParagraph().
// 				WithID(id).
// 				WithContents(fmt.Sprintf("paragraph#%d", i)).
// 				WithTenant(tenantName).
// 				Object()
// 		}
// 		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
// 	})

// 	var sourceNode, targetNode string
// 	var targetNodeID int

// 	t.Run("find source and target nodes", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		for i, node := range body.Payload.Nodes {
// 			if len(node.Shards) == 0 {
// 				targetNode = node.Name
// 				targetNodeID = i + 1
// 			} else {
// 				sourceNode = node.Name
// 			}
// 		}

// 		require.NotEmpty(t, sourceNode)
// 		require.NotEmpty(t, targetNode)
// 	})

// 	t.Run("deactivate tenant", func(t *testing.T) {
// 		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
// 		helper.UpdateTenants(t, paragraphClass.Class, tenants)
// 	})

// 	transferType := api.COPY.String()

// 	var uuid strfmt.UUID

// 	t.Run(fmt.Sprintf("start %q of %s from %s to %s for paragraph", transferType, tenantName, sourceNode, targetNode), func(t *testing.T) {
// 		resp, err := helper.Client(t).Replication.Replicate(
// 			replication.NewReplicateParams().WithBody(
// 				&models.ReplicationReplicateReplicaRequest{
// 					CollectionID:        &paragraphClass.Class,
// 					SourceNodeName:      &sourceNode,
// 					DestinationNodeName: &targetNode,
// 					ShardID:             &tenantName,
// 					TransferType:        &transferType,
// 				},
// 			),
// 			nil,
// 		)
// 		require.NoError(t, err)
// 		require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
// 		uuid = *resp.Payload.ID
// 	})

// 	t.Run("waiting for replication to finish", func(t *testing.T) {
// 		require.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			details, err := helper.Client(t).Replication.ReplicationDetails(
// 				replication.NewReplicationDetailsParams().WithID(uuid), nil,
// 			)
// 			require.Nil(ct, err, "failed to get replication details %s", err)

// 			if !assert.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY") {
// 				t.Run("deactivate tenant", func(t *testing.T) {
// 					tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
// 					helper.UpdateTenants(t, paragraphClass.Class, tenants)
// 				})
// 			}
// 		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", uuid)
// 	})

// 	t.Run("validate target node belongs to the tenant replication group", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		var targetNodeFound bool

// 		for _, node := range body.Payload.Nodes {
// 			if targetNode == node.Name {
// 				targetNodeFound = true
// 				break
// 			}
// 		}

// 		require.True(t, targetNodeFound)
// 	})

// 	t.Run("assert data is available for paragraph on target node with consistency level one", func(t *testing.T) {
// 		require.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(targetNodeID).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
// 			require.Nil(ct, err)
// 			require.Len(ct, resp, len(paragraphIDs))
// 		}, 10*time.Second, 1*time.Second, "target node doesn't have paragraph data")
// 	})
// }

// func (suite *ReplicaMultitenancyTestSuite) TestReplicaMovementDeactivatedTenantWithoutAutoTenantActivation() {
// 	t := suite.T()
// 	mainCtx := context.Background()

// 	var (
// 		clusterSize = 3
// 		tenantName  = "tenant-0"
// 	)

// 	compose, err := docker.New().
// 		WithWeaviateCluster(clusterSize).
// 		WithText2VecContextionary().
// 		Start(mainCtx)
// 	require.NoError(t, err)

// 	defer func() {
// 		if err := compose.Terminate(mainCtx); err != nil {
// 			t.Fatalf("failed to terminate test containers: %s", err.Error())
// 		}
// 	}()

// 	helper.SetupClient(compose.GetWeaviate().URI())
// 	paragraphClass := articles.ParagraphsClass()

// 	t.Run("create schema", func(t *testing.T) {
// 		paragraphClass.ReplicationConfig = &models.ReplicationConfig{
// 			Factor:       int64(clusterSize - 1),
// 			AsyncEnabled: false,
// 		}
// 		paragraphClass.MultiTenancyConfig = &models.MultiTenancyConfig{
// 			AutoTenantCreation:   false,
// 			AutoTenantActivation: false,
// 			Enabled:              true,
// 		}
// 		paragraphClass.Vectorizer = "text2vec-contextionary"
// 		helper.CreateClass(t, paragraphClass)
// 	})

// 	t.Run("create tenant", func(t *testing.T) {
// 		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "HOT"}}
// 		helper.CreateTenants(t, paragraphClass.Class, tenants)
// 	})

// 	t.Run("insert paragraphs", func(t *testing.T) {
// 		batch := make([]*models.Object, len(paragraphIDs))
// 		for i, id := range paragraphIDs {
// 			batch[i] = articles.NewParagraph().
// 				WithID(id).
// 				WithContents(fmt.Sprintf("paragraph#%d", i)).
// 				WithTenant(tenantName).
// 				Object()
// 		}
// 		common.CreateObjectsCL(t, compose.GetWeaviate().URI(), batch, types.ConsistencyLevelAll)
// 	})

// 	var sourceNode, targetNode string
// 	var targetNodeID int

// 	t.Run("find source and target nodes", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		for i, node := range body.Payload.Nodes {
// 			if len(node.Shards) == 0 {
// 				targetNode = node.Name
// 				targetNodeID = i + 1
// 			} else {
// 				sourceNode = node.Name
// 			}
// 		}

// 		require.NotEmpty(t, sourceNode)
// 		require.NotEmpty(t, targetNode)
// 	})

// 	t.Run("deactivate tenant", func(t *testing.T) {
// 		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "COLD"}}
// 		helper.UpdateTenants(t, paragraphClass.Class, tenants)
// 	})

// 	transferType := api.COPY.String()

// 	var uuid strfmt.UUID

// 	t.Run(fmt.Sprintf("start %q of %s from %s to %s for paragraph", transferType, tenantName, sourceNode, targetNode), func(t *testing.T) {
// 		resp, err := helper.Client(t).Replication.Replicate(
// 			replication.NewReplicateParams().WithBody(
// 				&models.ReplicationReplicateReplicaRequest{
// 					CollectionID:        &paragraphClass.Class,
// 					SourceNodeName:      &sourceNode,
// 					DestinationNodeName: &targetNode,
// 					ShardID:             &tenantName,
// 					TransferType:        &transferType,
// 				},
// 			),
// 			nil,
// 		)
// 		require.NoError(t, err)
// 		require.Equal(t, http.StatusOK, resp.Code(), "replication replicate operation didn't return 200 OK")
// 		uuid = *resp.Payload.ID
// 	})

// 	t.Run("waiting for replication to finish", func(t *testing.T) {
// 		require.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			details, err := helper.Client(t).Replication.ReplicationDetails(
// 				replication.NewReplicationDetailsParams().WithID(uuid), nil,
// 			)
// 			require.Nil(ct, err, "failed to get replication details %s", err)
// 			require.Equal(ct, "READY", details.Payload.Status.State, "expected replication status to be READY")
// 		}, 240*time.Second, 1*time.Second, "replication operation %s not finished in time", uuid)
// 	})

// 	t.Run("validate target node belongs to the tenant replication group", func(t *testing.T) {
// 		verbose := verbosity.OutputVerbose
// 		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(paragraphClass.Class)
// 		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
// 		require.NoError(t, clientErr)
// 		require.NotNil(t, body.Payload)

// 		var targetNodeFound bool

// 		for _, node := range body.Payload.Nodes {
// 			if targetNode == node.Name {
// 				targetNodeFound = true
// 				break
// 			}
// 		}

// 		require.True(t, targetNodeFound)
// 	})

// 	t.Run("activate tenant", func(t *testing.T) {
// 		tenants := []*models.Tenant{{Name: tenantName, ActivityStatus: "ACTIVE"}}
// 		helper.UpdateTenants(t, paragraphClass.Class, tenants)
// 	})

// 	t.Run("assert data is available for paragraph on target node with consistency level one", func(t *testing.T) {
// 		require.EventuallyWithT(t, func(ct *assert.CollectT) {
// 			resp := common.GQLTenantGet(t, compose.GetWeaviateNode(targetNodeID).URI(), paragraphClass.Class, types.ConsistencyLevelOne, tenantName)
// 			require.Nil(ct, err)
// 			require.Len(ct, resp, len(paragraphIDs))
// 		}, 10*time.Second, 1*time.Second, "target node doesn't have paragraph data")
// 	})
// }
