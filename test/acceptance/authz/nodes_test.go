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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzNodes(t *testing.T) {
	t.Parallel()
	adminUser := "existing-user"
	adminKey := "existing-key"

	customUser := "custom-user"
	customKey := "custom-key"
	customRole := "custom"

	_, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	clsA := articles.ArticlesClass()

	t.Run("setup", func(t *testing.T) {
		helper.CreateClassAuth(t, articles.ParagraphsClass(), adminKey)
		helper.CreateClassAuth(t, clsA, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("article1").Object()}, adminKey)
	})

	t.Run("fail to get nodes without minimal read_nodes", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*nodes.NodesGetForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to get cluster stats without read_cluster", func(t *testing.T) {
		_, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*cluster.ClusterGetStatisticsForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("make custom role with read_nodes and minimal nodes resource", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{Name: &customRole, Permissions: []*models.Permission{
			helper.NewNodesPermission().WithAction(authorization.ReadNodes).WithVerbosity(verbosity.OutputMinimal).Permission(),
		}})
	})

	t.Run("assign custom role to custom user", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, customRole, customUser)
	})

	t.Run("get minimal nodes with read_nodes", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Nodes, 1)
	})

	t.Run("add read_cluster to custom role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, customRole, &models.Permission{Action: &authorization.ReadCluster})
	})

	t.Run("get cluster stats with read_cluster", func(t *testing.T) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Statistics, 1)
	})

	t.Run("fail to get verbose nodes without verbose read_nodes on all collections", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(clsA.Class).WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*nodes.NodesGetClassForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add verbose read_nodes on class to custom role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, customRole, helper.NewNodesPermission().WithAction(authorization.ReadNodes).WithVerbosity(verbosity.OutputVerbose).WithCollection(clsA.Class).Permission())
	})

	t.Run("get verbose nodes by class with verbose read_nodes on class", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(clsA.Class).WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Nodes, 1)
	})

	t.Run("fail to get verbose nodes on all classes without read_nodes on *", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*nodes.NodesGetForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add read_data on * to custom role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, customRole, helper.NewNodesPermission().WithAction(authorization.ReadNodes).WithVerbosity(verbosity.OutputVerbose).WithCollection("*").Permission())
	})

	t.Run("get verbose nodes on all classes with read_data on *", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Nodes, 1)
	})
}
