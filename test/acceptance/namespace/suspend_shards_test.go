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
	"context"
	"slices"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
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

	// Sorted on a copy: want aliases the caller's slice when spread with "...".
	want = append([]string(nil), want...)
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
//
// The two counters are what stop it passing on an absence nobody established:
// require.Never returns true on its own deadline even if no check finished, and
// an unreachable node is reported as a node with no shards and no error.
func requireShardAbsent(t *testing.T, qualifiedClass, shardName string) {
	t.Helper()

	var listed, failed atomic.Int64
	require.Never(t, func() bool {
		got, err := shardsForClass(t, qualifiedClass)
		if err != nil {
			failed.Add(1)
			return false
		}
		listed.Add(1)
		return slices.Contains(got, shardName)
	}, 5*time.Second, 250*time.Millisecond, "shard %q must not be materialized", shardName)

	require.Zero(t, failed.Load(), "listing shards for %q failed", qualifiedClass)
	require.Positive(t, listed.Load(), "no shard listing finished for %q", qualifiedClass)
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
	// Resuming an already-active namespace is accepted, so this needs no guard
	// against the resume the test does itself.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })

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

		require.NoError(t, addTenantsAuth(t, qualified, []*models.Tenant{
			{Name: "afterresume", ActivityStatus: models.TenantActivityStatusHOT},
		}, adminKey))
		requireShardsEventually(t, qualified, "afterresume", "warm")
	})
}

// The node the restart below targets. The shared client talks to weaviate-0, so
// restarting the last node leaves that client valid for the rest of the
// package. StopNode and StartNode count from 0, matching the container name.
const (
	restartNodeName  = docker.Weaviate2
	restartNodeIndex = 2
)

// requireShardCountEventually waits until the class reports exactly want shards
// and returns them. A single-tenant class's shard name is generated, so callers
// capture it here rather than hardcoding one.
func requireShardCountEventually(t *testing.T, qualifiedClass string, want int) []string {
	t.Helper()

	var got []string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		names, err := shardsForClass(t, qualifiedClass)
		if !assert.NoError(c, err) {
			return
		}
		if !assert.Len(c, names, want) {
			return
		}
		got = names
	}, 30*time.Second, 200*time.Millisecond, "%q never reported %d shards", qualifiedClass, want)
	return got
}

// A namespace suspended before a node goes down must come back with none of its
// shards loaded, while an active namespace on the same node reloads normally.
// Both namespaces pin to the same home node, so one restart puts them through
// the same boot and their state is the only difference between them.
//
// Not parallel, and must stay that way: Go runs the non-parallel tests first,
// one after another, and only releases the t.Parallel() bodies once they are
// all done. That ordering is what makes taking a node away here safe — this
// test has the shared cluster to itself.
func TestNamespaces_SuspendedNamespaceLoadsNoShardsAfterRestart(t *testing.T) {
	ctx := context.Background()

	keepNS, dropNS := uniqueNS(), uniqueNS()
	for _, ns := range []string{keepNS, dropNS} {
		helper.CreateNamespaceWithHomeNode(t, ns, restartNodeName, adminKey)
		t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	}

	keepKey := createNamespacedUser(t, "u1", keepNS, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, keepNS+":u1", adminKey) })
	dropKey := createNamespacedUser(t, "u1", dropNS, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, dropNS+":u1", adminKey) })

	// Two class shapes, because boot decides their shards differently: a
	// single-tenant class by namespace state alone, a multi-tenant one by that
	// plus each tenant's activity status.
	const (
		keepClass   = "Kept"
		dropSTClass = "Dropped"
		dropMTClass = "DroppedTenants"
		tenant      = "warm"
		title       = "survives the suspend"
	)
	keepQualified := keepNS + ":" + keepClass
	dropSTQualified := dropNS + ":" + dropSTClass
	dropMTQualified := dropNS + ":" + dropMTClass

	setupClassInNs1(t, keepNS, keepClass, keepKey)
	setupClassInNs1(t, dropNS, dropSTClass, dropKey)
	setupMTClassInNs1(t, dropNS, dropMTClass, dropKey)
	// The namespaced key pairs with the short class name; the qualified name is
	// what the global operator uses once the suspend rejects that key.
	require.NoError(t, addTenantsAuth(t, dropMTClass, []*models.Tenant{
		{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT},
	}, dropKey))

	objectID := strfmt.UUID("6a2c4f18-9d3b-4a71-8e05-2f7c1b9d4e63")
	_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: objectID, Class: dropSTClass, Properties: map[string]any{"title": title},
	}, dropKey)
	require.NoError(t, err)

	keepShards := requireShardCountEventually(t, keepQualified, 1)
	dropSTShards := requireShardCountEventually(t, dropSTQualified, 1)
	requireShardsEventually(t, dropMTQualified, tenant)

	helper.SuspendNamespace(t, dropNS, adminKey)
	// Resuming an already-active namespace is accepted, so this needs no guard
	// against the resume the test does itself.
	t.Cleanup(func() { helper.ResumeNamespace(t, dropNS, adminKey) })

	t.Run("suspending leaves an already-open shard open", func(t *testing.T) {
		got, err := shardsForClass(t, dropSTQualified)
		require.NoError(t, err)
		assert.Equal(t, dropSTShards, got)
	})

	// A failed stop or start would leave the node down for every later test in
	// the package, so put it back whatever happens here.
	t.Cleanup(func() {
		require.NoError(t, sharedCompose.EnsureRunning(ctx, restartNodeIndex))
	})

	// The kill runs first, while the suspended namespace's shards are still open
	// and its data still unflushed: that is the boot with something to recover.
	// The graceful pass afterwards starts from shards already closed, so it only
	// re-checks that the decision holds.
	zero := time.Duration(0)
	for _, tc := range []struct {
		name    string
		timeout *time.Duration
	}{
		{"after a crash the suspended namespace loads no shards", &zero},
		{"after a graceful restart the suspended namespace loads no shards", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, sharedCompose.StopNode(ctx, restartNodeIndex, tc.timeout))
			require.NoError(t, sharedCompose.StartNode(ctx, restartNodeIndex))

			// A node that is down reports no shards and no error, so this is also
			// what rules out "absent because nobody answered" below.
			requireShardsEventually(t, keepQualified, keepShards...)

			requireShardAbsent(t, dropSTQualified, dropSTShards[0])
			requireShardAbsent(t, dropMTQualified, tenant)
		})
	}

	t.Run("the suspended namespace keeps its schema", func(t *testing.T) {
		for _, class := range []string{dropSTQualified, dropMTQualified} {
			_, err := helper.GetClassAuthWithReturn(t, class, adminKey)
			require.NoError(t, err, "class %q must still be in the schema", class)
		}
		requireTenantEventually(t, dropMTQualified, tenant, models.TenantActivityStatusHOT)
	})

	// KNOWN GAP, pinned deliberately: returning to active reopens nothing, so the
	// shards this node skipped at boot stay closed and reads of them keep
	// failing. Only a write reopens one, because a read is not allowed to
	// materialize a shard (Index.GetShard passes ensureInit=false). Invert this
	// subtest once resuming reopens a namespace's shards on its own.
	t.Run("resuming alone reopens no shards", func(t *testing.T) {
		helper.ResumeNamespace(t, dropNS, adminKey)

		requireShardAbsent(t, dropSTQualified, dropSTShards[0])
		requireShardAbsent(t, dropMTQualified, tenant)
	})

	t.Run("a write reopens the shard with its data intact", func(t *testing.T) {
		// Retried: the resume is only confirmed on the node the client talks to,
		// so the node that owns the shard may still be refusing writes. The id is
		// left to the server because supplying one runs a duplicate check whose
		// remote arm reads a closed shard's error as "the id exists".
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
				Class:      dropSTClass,
				Properties: map[string]any{"title": "written after the resume"},
			}, dropKey)
			assert.NoError(c, err)
		}, 30*time.Second, 250*time.Millisecond, "a write must be accepted once the namespace is active")
		requireShardsEventually(t, dropSTQualified, dropSTShards...)

		obj, err := helper.GetObjectAuth(t, dropSTQualified, objectID, adminKey)
		require.NoError(t, err)
		require.NotNil(t, obj)
		props, ok := obj.Properties.(map[string]any)
		require.True(t, ok, "unexpected property shape %T", obj.Properties)
		assert.Equal(t, title, props["title"], "the object written before the suspend must survive")
	})
}
