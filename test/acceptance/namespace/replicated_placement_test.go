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

package namespace

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// shardOwners maps each shard verboseNodes reports to the nodes that list it.
func shardOwners(t *testing.T, c *assert.CollectT, qualifiedClass, key string) (map[string][]string, bool) {
	t.Helper()
	nodeList, ok := verboseNodes(t, c, qualifiedClass, key)
	if !ok {
		return nil, false
	}
	owners := map[string][]string{}
	for _, n := range nodeList {
		for _, s := range n.Shards {
			owners[s.Name] = append(owners[s.Name], n.Name)
		}
	}
	return owners, true
}

// TestNamespaces_ReplicatedPlacement checks placement, reads and home_nodes
// updates for classes above factor 1 in a namespace whose home_nodes lists
// all three nodes.
func TestNamespaces_ReplicatedPlacement(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	homeNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

	helper.CreateNamespaceWithHomeNodes(t, ns, homeNodes, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	helper.CreateClassAuth(t, &models.Class{
		Class:             "Movies",
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
	}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Movies", adminKey) })

	t.Run("an RF=3 class has one shard on every home node", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			owners, ok := shardOwners(t, c, ns+":Movies", adminKey)
			if !ok || !assert.Len(c, owners, 1, "expected one shard, got %v", owners) {
				return
			}
			for _, nodes := range owners {
				assert.ElementsMatch(c, homeNodes, nodes)
			}
		}, 30*time.Second, 500*time.Millisecond, "the RF=3 shard never sat on all three home nodes")
	})

	t.Run("an object written at ALL reads back at ONE from each node", func(t *testing.T) {
		id := strfmt.UUID(uuid.NewString())
		require.NoError(t, helper.CreateObjectVia(t, helper.Client(t),
			&models.Object{ID: id, Class: "Movies"}, types.ConsistencyLevelAll, userKey))
		for _, node := range homeNodes {
			c := helper.NewClient(t, uriForNode(t, node))
			helper.WaitForClassVia(t, c, "Movies", userKey)
			_, err := helper.GetObjectVia(t, c, "Movies", id, types.ConsistencyLevelOne, userKey)
			require.NoError(t, err, "object missing on %s", node)
		}
	})

	t.Run("an RF=2 tenant added via the third node lands on the first two", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{
			Class:              "Books",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			ReplicationConfig:  &models.ReplicationConfig{Factor: 2},
		}, userKey)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Books", adminKey) })

		entry := helper.NewClient(t, uriForNode(t, homeNodes[2]))
		helper.WaitForClassVia(t, entry, "Books", userKey)
		_, err := entry.Schema.TenantsCreate(
			schema.NewTenantsCreateParams().WithClassName("Books").WithBody([]*models.Tenant{{Name: "tenantA"}}),
			helper.CreateAuth(userKey),
		)
		require.NoError(t, err)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			owners, ok := shardOwners(t, c, ns+":Books", adminKey)
			if ok {
				assert.ElementsMatch(c, homeNodes[:2], owners["tenantA"])
			}
		}, 30*time.Second, 500*time.Millisecond, "tenantA never sat on the first two home nodes")
	})

	t.Run("home_nodes cannot shrink below the RF=3 class", func(t *testing.T) {
		_, err := helper.Client(t).Namespaces.UpdateNamespace(
			namespaces.NewUpdateNamespaceParams().
				WithNamespaceID(ns).
				WithBody(&models.NamespaceUpdateRequest{HomeNodes: homeNodes[:2]}),
			helper.CreateAuth(adminKey),
		)
		var unproc *namespaces.UpdateNamespaceUnprocessableEntity
		require.ErrorAs(t, err, &unproc)
		assert.Equal(t, homeNodes, helper.GetNamespace(t, ns, adminKey).HomeNodes)
	})
}
