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
			_, err := helper.GetObjectVia(t, c, "Movies", "", id, types.ConsistencyLevelOne, userKey)
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

// classShard is one shard of a class, with the tenant it holds where the
// class has tenants.
type classShard struct{ class, tenant, shard string }

// newRF2Shards creates in ns an RF=2 class and an RF=2 class with one HOT
// tenant, each with a title property, and returns their two shards. Its cleanup
// asserts nothing, so a test may delete the classes with their namespace.
func newRF2Shards(t *testing.T, ns, userKey string) []classShard {
	t.Helper()

	const singleClass, multiClass, tenant = "Docs", "Books", "warm"
	for _, class := range []*models.Class{
		{Class: singleClass, Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}}},
		mtClass(multiClass),
	} {
		class.ReplicationConfig = &models.ReplicationConfig{Factor: 2}
		helper.CreateClassAuth(t, class, userKey)
		t.Cleanup(func() { helper.DeleteClassWithoutAssert(t, ns+":"+class.Class, adminKey) })
	}
	helper.CreateTenantsAuth(t, multiClass, []*models.Tenant{{Name: tenant}}, userKey)
	return []classShard{
		{class: singleClass, shard: requireShardCountEventually(t, ns+":"+singleClass, 1)[0]},
		{class: multiClass, tenant: tenant, shard: tenant},
	}
}

// writeAtAllEventually writes an object with id to tg at ALL, retrying while a
// replica cannot acknowledge. Pass an empty id while a replica is closed, because a
// supplied id's existence check fails there with a 500 and only a write reopens it.
func writeAtAllEventually(t *testing.T, tg classShard, id strfmt.UUID, key string) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, helper.CreateObjectVia(t, helper.Client(t),
			&models.Object{ID: id, Class: tg.class, Tenant: tg.tenant}, types.ConsistencyLevelAll, key))
	}, movementTimeout, 500*time.Millisecond, "a write at ALL to %s %q never succeeded", tg.class, tg.shard)
}

// requireReplicaServes waits until node lists the shard of tg and answers a read
// of id at ONE. A read at ONE fails over to another replica only on an error, so
// a node that lists the shard but lacks id answers 404.
func requireReplicaServes(t *testing.T, ns, node string, tg classShard, id strfmt.UUID, key string) {
	t.Helper()

	client := helper.NewClient(t, uriForNode(t, node))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		shard, _, err := shardOnNode(t, ns+":"+tg.class, tg.shard, node)
		if !assert.NoError(c, err) || !assert.NotNil(c, shard, "%q lists no shard %q", node, tg.shard) {
			return
		}
		_, err = helper.GetObjectVia(t, client, tg.class, tg.tenant, id, types.ConsistencyLevelOne, key)
		assert.NoError(c, err)
	}, movementTimeout, 500*time.Millisecond, "%q never served %s %q", node, tg.class, tg.shard)
}

// TestNamespaces_StoppedHomeNodeKeepsPlacement creates an RF=2 class and tenant
// while the first of three home nodes is stopped. That node stays in the RAFT
// configuration, so placement keeps it over the third, and it serves both
// replicas once restarted. Not parallel, because it stops a node.
func TestNamespaces_StoppedHomeNodeKeepsPlacement(t *testing.T) {
	homeNodes := []string{restartNodeName, docker.Weaviate0, docker.Weaviate1}
	ns := uniqueNS()
	helper.CreateNamespaceWithHomeNodes(t, ns, homeNodes, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	var targets []classShard
	withNodeStopped(t, restartNodeName, nil, func(t *testing.T) {
		targets = newRF2Shards(t, ns, userKey)

		t.Run("the stopped node keeps its place among the replicas", func(t *testing.T) {
			for _, tg := range targets {
				replicas, err := shardReplicas(t, ns+":"+tg.class, tg.shard)
				require.NoError(t, err)
				assert.ElementsMatch(t, homeNodes[:2], replicas, "%s %q", tg.class, tg.shard)
			}
		})
	})

	t.Run("the restarted node serves both replicas", func(t *testing.T) {
		for _, tg := range targets {
			id := strfmt.UUID(uuid.NewString())
			writeAtAllEventually(t, tg, id, userKey)
			requireReplicaServes(t, ns, restartNodeName, tg, id, userKey)
		}
	})
}
