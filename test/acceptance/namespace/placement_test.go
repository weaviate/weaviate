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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// homeNodeShards splits the shards reported by GET /v1/nodes/<class> into
// those on homeNode and a count of shards seen on any other node.
func homeNodeShards(t *testing.T, qualifiedClass, homeNode, key string) (homeShards []*models.NodeShardStatus, otherNodeShardCount int) {
	t.Helper()
	verbose := "verbose"
	resp, err := helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(qualifiedClass).WithOutput(&verbose),
		helper.CreateAuth(key),
	)
	require.NoError(t, err)
	require.NotNil(t, resp.Payload)
	for _, n := range resp.Payload.Nodes {
		if n.Name == homeNode {
			homeShards = n.Shards
			continue
		}
		otherNodeShardCount += len(n.Shards)
	}
	return homeShards, otherNodeShardCount
}

// TestNamespaces_CollectionPinsToHomeNode places a namespaced collection
// and asserts its shard lands only on the namespace's home_node.
func TestNamespaces_CollectionPinsToHomeNode(t *testing.T) {
	const (
		ns       = "pincol"
		homeNode = "weaviate-1"
	)

	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Movies", adminKey) })

	home, other := homeNodeShards(t, ns+":Movies", homeNode, adminKey)
	require.Len(t, home, 1, "expected exactly one shard on home_node %q", homeNode)
	assert.Equal(t, ns+":Movies", home[0].Class)
	assert.Zero(t, other, "no shards expected outside home_node")
}

// TestNamespaces_TenantPinsToHomeNode covers tenant placement on a
// namespaced MT class: every tenant's shard lands on the namespace's
// home_node, and a tenant reactivated after freeze still pins there.
func TestNamespaces_TenantPinsToHomeNode(t *testing.T) {
	const (
		ns       = "pintnt"
		homeNode = "weaviate-2"
	)

	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	helper.CreateClassAuth(t, &models.Class{
		Class:              "Books",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Books", adminKey) })

	tenants := []*models.Tenant{
		{Name: "tenantA", ActivityStatus: models.TenantActivityStatusHOT},
		{Name: "tenantB", ActivityStatus: models.TenantActivityStatusHOT},
	}
	helper.CreateTenantsAuth(t, "Books", tenants, userKey)

	t.Run("create pins all tenants to home_node", func(t *testing.T) {
		home, other := homeNodeShards(t, ns+":Books", homeNode, adminKey)
		require.Len(t, home, len(tenants), "expected one shard per tenant on home_node %q", homeNode)
		for _, s := range home {
			assert.Equal(t, ns+":Books", s.Class)
		}
		assert.Zero(t, other, "no tenant shards expected outside home_node")
	})

	t.Run("unfreeze restores tenant to home_node", func(t *testing.T) {
		helper.UpdateTenantsWithAuthz(t, "Books", []*models.Tenant{
			{Name: "tenantA", ActivityStatus: models.TenantActivityStatusFROZEN},
		}, helper.CreateAuth(userKey))
		waitForTenantStatus(t, ns+":Books", "tenantA", models.TenantActivityStatusFROZEN, adminKey)

		helper.UpdateTenantsWithAuthz(t, "Books", []*models.Tenant{
			{Name: "tenantA", ActivityStatus: models.TenantActivityStatusHOT},
		}, helper.CreateAuth(userKey))
		waitForTenantStatus(t, ns+":Books", "tenantA", models.TenantActivityStatusHOT, adminKey)

		// The schema status flips to HOT as soon as the apply commits, but the
		// shard download from S3 and the index re-registration are async. Poll
		// the /nodes endpoint until the unfrozen shard is back on home_node.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			home, other := homeNodeShards(t, ns+":Books", homeNode, adminKey)
			assert.Len(c, home, len(tenants))
			assert.Zero(c, other, "reactivated tenant shard must remain on home_node")
		}, 60*time.Second, 500*time.Millisecond, "unfrozen tenant never returned to home_node %q", homeNode)
	})
}

// waitForTenantStatus polls GetTenants until tenant reaches want, or fails
// the test on timeout.
func waitForTenantStatus(t *testing.T, qualifiedClass, tenant, want, key string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.GetTenantsWithAuthz(t, qualifiedClass, helper.CreateAuth(key))
		if !assert.NoError(c, err) {
			return
		}
		for _, ten := range resp.Payload {
			if ten.Name == tenant {
				assert.Equal(c, want, ten.ActivityStatus,
					"tenant %q activity status not yet %q", tenant, want)
				return
			}
		}
		assert.Failf(c, "tenant missing", "tenant %q not found in Get response", tenant)
	}, 90*time.Second, 500*time.Millisecond, "tenant %q never reached %q", tenant, want)
}
