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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func strPtr(s string) *string { return &s }

// nodesGetVerbose issues a verbose all-collections NodesGet. This path never
// 403s: it returns 200 with shards filtered to the caller's authorized collections.
func nodesGetVerbose(t *testing.T, key string) *models.NodesStatusResponse {
	t.Helper()
	resp, err := helper.Client(t).Nodes.NodesGet(
		nodes.NewNodesGetParams().WithOutput(strPtr(verbosity.OutputVerbose)),
		helper.CreateAuth(key),
	)
	require.NoError(t, err)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

// nodesGetClassVerbose issues a verbose by-class query; the server resolves the
// unqualified class to the caller's namespace.
func nodesGetClassVerbose(t *testing.T, key, class string) *models.NodesStatusResponse {
	t.Helper()
	resp, err := helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithClassName(class).WithOutput(strPtr(verbosity.OutputVerbose)),
		helper.CreateAuth(key),
	)
	require.NoError(t, err)
	require.NotNil(t, resp.Payload)
	return resp.Payload
}

// requireMinimalForbidden asserts the node-wide minimal view (default output)
// is denied — that path runs the upfront authorize, unlike verbose.
func requireMinimalForbidden(t *testing.T, key string) {
	t.Helper()
	_, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(key))
	require.Error(t, err)
	var forbidden *nodes.NodesGetForbidden
	require.True(t, errors.As(err, &forbidden), "expected NodesGetForbidden, got %T: %v", err, err)
}

// shardClassPrefixes returns the set of namespace prefixes ("<ns>:") seen across
// every node's shards, plus the total shard count.
func shardClassPrefixes(nodeStatuses []*models.NodeStatus) (prefixes map[string]struct{}, total int) {
	prefixes = map[string]struct{}{}
	for _, n := range nodeStatuses {
		for _, sh := range n.Shards {
			if ns, _, ok := strings.Cut(sh.Class, ":"); ok {
				prefixes[ns+":"] = struct{}{}
			}
			total++
		}
	}
	return prefixes, total
}

// assertScopedTo asserts every returned shard belongs to wantNS, at least one
// shard is present, and each node's aggregate matches the returned (scoped)
// shards over the wire — ShardCount == number of shards and ObjectCount == the
// sum of their counts. The aggregate equality is what pins the leak fix: a
// node-wide Stats spanning other namespaces would break it.
func assertScopedTo(t *testing.T, nodeStatuses []*models.NodeStatus, wantNS string) {
	t.Helper()
	prefixes, total := shardClassPrefixes(nodeStatuses)
	assert.Positive(t, total, "scoped caller must see at least one of its own shards")
	for p := range prefixes {
		assert.Equal(t, wantNS, p, "verbose nodes leaked a shard outside namespace %q", wantNS)
	}
	for _, n := range nodeStatuses {
		if n.Stats == nil {
			continue
		}
		var objects int64
		for _, sh := range n.Shards {
			objects += sh.ObjectCount
		}
		assert.Equal(t, int64(len(n.Shards)), n.Stats.ShardCount,
			"node %s aggregate ShardCount must match the returned (scoped) shards", n.Name)
		assert.Equal(t, objects, n.Stats.ObjectCount,
			"node %s aggregate ObjectCount must equal the sum of the returned (scoped) shards", n.Name)
	}
}

// assertNoNodeWideLeak: a caller that sees no shards must see no node-wide
// aggregate either — BatchStats dropped and Stats zeroed on every node. Pins the
// empty-node leak (a node with no shards for the caller still withholds its
// node-wide ingest signal).
func assertNoNodeWideLeak(t *testing.T, nodeStatuses []*models.NodeStatus) {
	t.Helper()
	for _, n := range nodeStatuses {
		assert.Nil(t, n.BatchStats, "node %s leaked node-wide BatchStats to a caller with no shards", n.Name)
		if n.Stats != nil {
			assert.Equal(t, int64(0), n.Stats.ShardCount, "node %s ShardCount must be 0 for a caller with no shards", n.Name)
			assert.Equal(t, int64(0), n.Stats.ObjectCount, "node %s ObjectCount must be 0 for a caller with no shards", n.Name)
		}
	}
}

// TestNamespaces_NodesEndpoint pins the namespace-aware contract of /v1/nodes:
// a namespaced admin (and any role with verbose read_nodes) sees verbose
// node/shard info for its own collections only, regular namespace viewers see
// none, the node-wide minimal view stays operator-only, and the global root
// sees all.
func TestNamespaces_NodesEndpoint(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	const class = "NodesProbe"
	setupClassInBothNamespaces(t, class, user1Key, user2Key)

	// Seed one object per namespace so the owning shard reports a non-zero count.
	id := strfmt.UUID("11111111-1111-1111-1111-111111111111")
	_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: id, Class: class, Properties: map[string]any{"title": "c1"},
	}, user1Key)
	require.NoError(t, err)
	_, err = helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: id, Class: class, Properties: map[string]any{"title": "c2"},
	}, user2Key)
	require.NoError(t, err)

	t.Run("namespaced admin sees verbose nodes for its own collections only", func(t *testing.T) {
		// EventuallyWithT absorbs RAFT-apply lag on the multi-node cluster.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, total := shardClassPrefixes(nodesGetVerbose(t, user1Key).Nodes)
			assert.Positive(c, total, "ns admin should eventually see its own shards")
		}, 20*time.Second, 200*time.Millisecond, "ns admin verbose nodes never populated")
		assertScopedTo(t, nodesGetVerbose(t, user1Key).Nodes, "customer1:")
	})

	t.Run("namespaced admin denied node-wide minimal view", func(t *testing.T) {
		requireMinimalForbidden(t, user1Key)
	})

	t.Run("namespaced admin by-class verbose is scoped and leaks no node-wide batch stats", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, total := shardClassPrefixes(nodesGetClassVerbose(t, user1Key, class).Nodes)
			assert.Positive(c, total, "ns admin should eventually see its own class shards")
		}, 20*time.Second, 200*time.Millisecond, "ns admin by-class verbose nodes never populated")

		payload := nodesGetClassVerbose(t, user1Key, class)
		assertScopedTo(t, payload.Nodes, "customer1:")
		// BatchStats is node-wide; a class-scoped caller must not receive it.
		for _, n := range payload.Nodes {
			assert.Nil(t, n.BatchStats,
				"node %s leaked node-wide BatchStats to a class-scoped caller via by-class", n.Name)
		}
	})

	t.Run("regular namespace viewer sees no shards and is denied minimal", func(t *testing.T) {
		viewerKey := createNamespacedViewerUser(t, "nodesview", "customer1", adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, "customer1:nodesview", adminKey) })

		viewerNodes := nodesGetVerbose(t, viewerKey).Nodes
		_, total := shardClassPrefixes(viewerNodes)
		assert.Zero(t, total, "viewer without a nodes grant must see no shards")
		assertNoNodeWideLeak(t, viewerNodes)
		requireMinimalForbidden(t, viewerKey)
	})

	t.Run("custom verbose-nodes role grants scoped access to a non-admin namespace user", func(t *testing.T) {
		key := helper.CreateUserWithNamespace(t, "vn", "customer1", adminKey)
		t.Cleanup(func() { helper.DeleteUser(t, "customer1:vn", adminKey) })

		// No role yet: verbose returns 200 with no shards, minimal is forbidden.
		bareNodes := nodesGetVerbose(t, key).Nodes
		_, total := shardClassPrefixes(bareNodes)
		assert.Zero(t, total, "bare namespace user must see no shards before the role is granted")
		assertNoNodeWideLeak(t, bareNodes)
		requireMinimalForbidden(t, key)

		// A custom role with verbose read_nodes over all collections; the matcher
		// scopes it to the caller's namespace. There is no built-in nodes role for
		// non-admin namespace users — this is how an operator would grant one.
		helper.CreateRoleAndAssign(t, adminKey, "customer1:vn", "ns-nodes-viewer",
			helper.NewNodesPermission().
				WithAction(authorization.ReadNodes).
				WithVerbosity(verbosity.OutputVerbose).
				WithCollection("*").
				Permission())
		helper.WaitForOwnRole(t, key, "ns-nodes-viewer")

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, total := shardClassPrefixes(nodesGetVerbose(t, key).Nodes)
			assert.Positive(c, total, "the verbose-nodes role should eventually expose the namespace's shards")
		}, 20*time.Second, 200*time.Millisecond, "verbose-nodes role never populated")
		assertScopedTo(t, nodesGetVerbose(t, key).Nodes, "customer1:")

		// verbose-only role: the node-wide minimal view stays denied.
		requireMinimalForbidden(t, key)
	})

	t.Run("global root sees shards from every namespace", func(t *testing.T) {
		prefixes, total := shardClassPrefixes(nodesGetVerbose(t, adminKey).Nodes)
		require.Positive(t, total)
		_, hasC1 := prefixes["customer1:"]
		_, hasC2 := prefixes["customer2:"]
		assert.True(t, hasC1 && hasC2, "root must see shards from both customer1 and customer2; saw %v", prefixes)
	})
}
