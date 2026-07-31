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
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// shardsForClass returns the shards a class has materialized across the
// cluster. Node status walks each index's shard map rather than the schema, so
// a tenant whose shard was refused is absent here while still listed as a
// tenant — which is what makes a refused materialization observable at all.
func shardsForClass(t *testing.T, qualifiedClass string) ([]string, error) {
	t.Helper()

	verbose := "verbose"
	params := nodes.NewNodesGetClassParams().WithClassName(qualifiedClass).WithOutput(&verbose)
	resp, err := helper.Client(t).Nodes.NodesGetClass(params, helper.CreateAuth(adminKey))
	if err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	names := []string{}
	for _, node := range resp.Payload.Nodes {
		for _, shard := range node.Shards {
			if _, dup := seen[shard.Name]; dup {
				continue
			}
			seen[shard.Name] = struct{}{}
			names = append(names, shard.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// requireShardsEventually waits for the cluster-wide shard set to settle on
// want. A tenant create returns once the leader applied it, so the node that
// hosts the shard may still be materializing it.
func requireShardsEventually(t *testing.T, qualifiedClass string, want ...string) {
	t.Helper()

	sort.Strings(want)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got, err := shardsForClass(t, qualifiedClass)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, want, got)
	}, 30*time.Second, 200*time.Millisecond, "shard set never settled on %v", want)
}

// requireShardAbsent holds for a window asserting the shard never appears. A
// one-shot check would pass simply by running before the materialization it is
// meant to rule out.
func requireShardAbsent(t *testing.T, qualifiedClass, shardName string) {
	t.Helper()

	require.Never(t, func() bool {
		got, err := shardsForClass(t, qualifiedClass)
		if err != nil {
			return false
		}
		return slices.Contains(got, shardName)
	}, 5*time.Second, 250*time.Millisecond, "shard %q must not be materialized", shardName)
}

// requireTenantEventually waits until the tenant is visible with the given
// status, which is what says the command reached the schema — so a missing
// shard afterwards is a refused materialization rather than a lost command.
func requireTenantEventually(t *testing.T, qualifiedClass, tenant, status string) {
	t.Helper()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got, err := getOneTenantAuth(t, qualifiedClass, tenant, adminKey)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, status, got.ActivityStatus)
	}, 30*time.Second, 200*time.Millisecond, "tenant %q never reported %s", tenant, status)
}

// A suspended namespace must materialize no further shards. Tenant commands are
// what reaches that decision on a running node: unlike a class create, they
// carry no namespace gate at RAFT apply, so they run all the way down into the
// shard registration the namespace guards.
func TestNamespaces_SuspendRefusesTenantShardMaterialization(t *testing.T) {
	t.Parallel()
	ns1, _, user1Key, _ := twoNamespaces(t)

	const class = "SuspendShards"
	setupMTClassInNs1(t, ns1, class, user1Key)
	qualified := ns1 + ":" + class

	// Baseline while active: the HOT tenant materializes and the COLD one does
	// not. Everything below is read against this.
	require.NoError(t, addTenantsAuth(t, qualified, []*models.Tenant{
		{Name: "warm", ActivityStatus: models.TenantActivityStatusHOT},
		{Name: "chilled", ActivityStatus: models.TenantActivityStatusCOLD},
	}, adminKey))
	requireShardsEventually(t, qualified, "warm")

	helper.SuspendNamespace(t, ns1, adminKey)
	resumed := false
	t.Cleanup(func() {
		// Active to active is not a valid transition, so this only fires when the
		// test stopped before reaching its own resume.
		if !resumed {
			helper.ResumeNamespace(t, ns1, adminKey)
		}
	})

	// The namespace's own key is rejected while it is suspended, so these run as
	// the global operator against the qualified class name.
	t.Run("a new HOT tenant materializes no shard", func(t *testing.T) {
		_ = addTenantsAuth(t, qualified, []*models.Tenant{
			{Name: "added", ActivityStatus: models.TenantActivityStatusHOT},
		}, adminKey)

		requireTenantEventually(t, qualified, "added", models.TenantActivityStatusHOT)
		requireShardAbsent(t, qualified, "added")
	})

	t.Run("activating a COLD tenant materializes no shard", func(t *testing.T) {
		_ = updateTenantsAuth(t, qualified, []*models.Tenant{
			{Name: "chilled", ActivityStatus: models.TenantActivityStatusHOT},
		}, adminKey)

		requireTenantEventually(t, qualified, "chilled", models.TenantActivityStatusHOT)
		requireShardAbsent(t, qualified, "chilled")
	})

	// The shard that was already open when the suspend landed stays open: the
	// guards refuse new materialization but unload nothing.
	t.Run("a shard open before the suspend stays open", func(t *testing.T) {
		got, err := shardsForClass(t, qualified)
		require.NoError(t, err)
		assert.Equal(t, []string{"warm"}, got)
	})

	t.Run("resuming lets a tenant materialize again", func(t *testing.T) {
		helper.ResumeNamespace(t, ns1, adminKey)
		resumed = true

		require.NoError(t, addTenantsAuth(t, qualified, []*models.Tenant{
			{Name: "afterresume", ActivityStatus: models.TenantActivityStatusHOT},
		}, adminKey))
		requireShardsEventually(t, qualified, "afterresume", "warm")
	})
}
