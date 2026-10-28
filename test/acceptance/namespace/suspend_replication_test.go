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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// shardOnNode returns what node reports for one shard of a class, or nil when it
// holds no such shard. Node status walks each index's shard map rather than the
// schema, so a node a movement never materialized the shard on reports nothing
// here while still being named as that movement's target.
func shardOnNode(t *testing.T, qualifiedClass, shardName, node string) (*models.NodeShardStatus, error) {
	t.Helper()

	verbose := "verbose"
	params := nodes.NewNodesGetClassParams().WithClassName(qualifiedClass).WithOutput(&verbose)
	resp, err := helper.Client(t).Nodes.NodesGetClass(params, helper.CreateAuth(adminKey))
	if err != nil {
		return nil, err
	}

	for _, n := range resp.Payload.Nodes {
		if n.Name != node {
			continue
		}
		for _, shard := range n.Shards {
			if shard.Name == shardName {
				return shard, nil
			}
		}
	}
	return nil, nil
}

// requireShardReleasedOnNode waits until node has listed no such shard for three
// seconds in a row. A shard the node opens and its sweep closes during the wait
// restarts the window rather than failing it. live is read from node in the same
// poll, because a node that timed out lists no shards either.
func requireShardReleasedOnNode(t *testing.T, qualifiedClass, shardName, node string, live presentShard) {
	t.Helper()

	const window = 3 * time.Second
	var absentSince time.Time
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		anchor, err := shardOnNode(t, live.qualifiedClass, live.name, node)
		if !assert.NoError(c, err) || !assert.NotNil(c, anchor, "%q stopped listing %q", node, live.name) {
			absentSince = time.Time{}
			return
		}
		shard, err := shardOnNode(t, qualifiedClass, shardName, node)
		if !assert.NoError(c, err) || !assert.Nil(c, shard, "%q still lists shard %q", node, shardName) {
			absentSince = time.Time{}
			return
		}
		if absentSince.IsZero() {
			absentSince = time.Now()
		}
		assert.GreaterOrEqual(c, time.Since(absentSince), window)
	}, 30*time.Second, 250*time.Millisecond, "%q never went %v without listing shard %q", node, window, shardName)
}

// registerMovement returns the op id of a movement of one shard onto targetNode,
// or the error its registration answered with. It registers with adminKey,
// because the replicate endpoint forbids every namespaced key.
func registerMovement(t *testing.T, qualifiedClass, shardName, sourceNode, targetNode, transferType string) (strfmt.UUID, error) {
	t.Helper()

	resp, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &qualifiedClass,
			Shard:      &shardName,
			SourceNode: &sourceNode,
			TargetNode: &targetNode,
			Type:       &transferType,
		}),
		helper.CreateAuth(adminKey),
	)
	if err != nil {
		return "", err
	}
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.ID)
	return *resp.Payload.ID, nil
}

// startCopyMovement registers a COPY of one shard onto targetNode and returns the
// op id.
func startCopyMovement(t *testing.T, qualifiedClass, shardName, sourceNode, targetNode string) strfmt.UUID {
	t.Helper()

	opID, err := registerMovement(t, qualifiedClass, shardName, sourceNode, targetNode, api.COPY.String())
	require.NoError(t, err, "failed to register COPY of %q %s->%s", shardName, sourceNode, targetNode)
	return opID
}

// movementState returns an op's current state and the messages it has recorded.
// The messages are where a refused movement says why it stopped, which is what
// tells a refusal apart from a movement that is only slow.
func movementState(t *testing.T, opID strfmt.UUID) (string, []string, error) {
	t.Helper()

	details, err := helper.Client(t).Replication.ReplicationDetails(
		replication.NewReplicationDetailsParams().WithID(opID), helper.CreateAuth(adminKey),
	)
	if err != nil {
		return "", nil, err
	}
	if details.Payload == nil || details.Payload.Status == nil {
		return "", nil, nil
	}

	messages := make([]string, 0, len(details.Payload.Status.Errors))
	for _, e := range details.Payload.Status.Errors {
		messages = append(messages, e.Message)
	}
	return details.Payload.Status.State, messages, nil
}

// A scale plan whose addition names no source node adds an empty replica, which
// registers no replication op — there is nothing in flight for a suspend to
// interrupt. The command is not gated on namespace state, so the apply commits
// its schema half and then opens the shard on the target, even though the
// namespace holds no shards open.
//
// That is what happens, not what should. A suspended namespace ought to refuse
// this command before the schema records anything, and the scale-plan and
// replica-list assertions invert when it does. Until then the target opens a
// shard on a node that is not the namespace's home node, and the target's own
// sweep closes it within one sweep interval. The open is not asserted, because
// it can start and end between two polls.
func TestNamespaces_SuspendAcceptsEmptyReplicaAdd(t *testing.T) {
	t.Parallel()

	const (
		// The namespace pins its shards to its home node, so the addition lands
		// on a node that holds nothing of this class.
		homeNode    = docker.Weaviate1
		targetNode  = docker.Weaviate0
		class       = "EmptyReplicaShard"
		anchorClass = "Anchor"
		planID      = strfmt.UUID("6f1c9d84-2a70-4c1e-8b53-9e0d17a4c6b2")
	)

	ns := uniqueNS()
	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	setupClassInNs1(t, ns, class, userKey)
	qualified := ns + ":" + class

	// Read before the suspend: a suspended namespace reports no shards.
	shardName := requireShardCountEventually(t, qualified, 1)[0]

	// The anchor's namespace is active and homed on the target, so its shard stays
	// open there.
	anchorNS := uniqueNS()
	helper.CreateNamespaceWithHomeNode(t, anchorNS, targetNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, anchorNS, adminKey) })
	anchorKey := createNamespacedUser(t, "u1", anchorNS, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, anchorNS+":u1", adminKey) })
	setupClassInNs1(t, anchorNS, anchorClass, anchorKey)
	anchorQualified := anchorNS + ":" + anchorClass
	anchor := presentShard{anchorQualified, requireShardCountEventually(t, anchorQualified, 1)[0]}

	helper.SuspendNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

	resp, err := helper.Client(t).Replication.ApplyReplicationScalePlan(
		replication.NewApplyReplicationScalePlanParams().WithBody(&models.ReplicationScalePlan{
			PlanID:     planID,
			Collection: qualified,
			ShardScaleActions: map[string]models.ReplicationScalePlanShardScaleActionsAnon{
				shardName: {
					AddNodes:    map[string]string{targetNode: ""},
					RemoveNodes: []string{},
				},
			},
		}),
		helper.CreateAuth(adminKey),
	)
	require.NoError(t, err, "a suspended namespace must not fail the scale plan")
	require.NotNil(t, resp.Payload)
	require.Empty(t, resp.Payload.OperationIds,
		"an addition with no source node registers no replication op")

	t.Run("the schema names the target as a replica", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, err := helper.Client(t).Replication.GetCollectionShardingState(
				replication.NewGetCollectionShardingStateParams().WithCollection(&qualified).WithShard(&shardName),
				helper.CreateAuth(adminKey),
			)
			if !assert.NoError(c, err) || !assert.Len(c, state.Payload.ShardingState.Shards, 1) {
				return
			}
			assert.Contains(c, state.Payload.ShardingState.Shards[0].Replicas, targetNode)
		}, 30*time.Second, 250*time.Millisecond, "the scale plan never named %q a replica of %q", targetNode, shardName)
	})

	t.Run("the target's sweep releases the shard", func(t *testing.T) {
		requireShardReleasedOnNode(t, qualified, shardName, targetNode, anchor)
	})
}

// A replica movement registered while its collection's namespace is suspended is
// refused with 422. The same registration succeeds once the namespace is back.
func TestNamespaces_SuspendRefusesReplicaMovement(t *testing.T) {
	t.Parallel()

	const (
		// The namespace pins its shards to its home node, which makes that node
		// the movement's only possible source.
		sourceNode = docker.Weaviate1
		targetNode = docker.Weaviate0
		class      = "MovedShard"
		title      = "written before the movement"
	)

	ns := uniqueNS()
	helper.CreateNamespaceWithHomeNode(t, ns, sourceNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })

	setupClassInNs1(t, ns, class, userKey)
	qualified := ns + ":" + class

	// One object, so the movement has something to copy and the replica it lands
	// can be told apart from an empty shard.
	objectID := strfmt.UUID("1d4e7a2c-6b8f-4c39-9e15-3a0d8f6b2c47")
	_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: objectID, Class: class, Properties: map[string]any{"title": title},
	}, userKey)
	require.NoError(t, err)

	shardName := requireShardCountEventually(t, qualified, 1)[0]
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		shard, err := shardOnNode(t, qualified, shardName, sourceNode)
		if !assert.NoError(c, err) {
			return
		}
		assert.NotNil(c, shard, "shard %q never landed on %q", shardName, sourceNode)
	}, 30*time.Second, 250*time.Millisecond, "the movement has no source replica to copy from")

	helper.SuspendNamespace(t, ns, adminKey)
	// Resuming an already-active namespace is accepted, so this needs no guard
	// against the resume the test does itself.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

	t.Run("the registration is refused with 422", func(t *testing.T) {
		_, err := registerMovement(t, qualified, shardName, sourceNode, targetNode, api.COPY.String())
		var unprocessable *replication.ReplicateUnprocessableEntity
		require.ErrorAs(t, err, &unprocessable)
	})

	t.Run("resuming admits the movement and lets it finish", func(t *testing.T) {
		helper.ResumeNamespace(t, ns, adminKey)
		opID := startCopyMovement(t, qualified, shardName, sourceNode, targetNode)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, messages, err := movementState(t, opID)
			if !assert.NoError(c, err) {
				return
			}
			assert.Equal(c, "READY", state, "movement still %q; errors %v", state, messages)
		}, 180*time.Second, 2*time.Second, "the movement never finished after the resume")
	})

	t.Run("the new replica carries the shard's data", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			shard, err := shardOnNode(t, qualified, shardName, targetNode)
			if !assert.NoError(c, err) {
				return
			}
			if !assert.NotNil(c, shard, "shard %q never appeared on %q", shardName, targetNode) {
				return
			}
			assert.Equal(c, int64(1), shard.ObjectCount)
		}, 60*time.Second, 500*time.Millisecond, "the copied replica never reported the object")

		obj, err := helper.GetObjectAuth(t, qualified, objectID, adminKey)
		require.NoError(t, err)
		require.NotNil(t, obj)
		props, ok := obj.Properties.(map[string]any)
		require.True(t, ok, "unexpected property shape %T", obj.Properties)
		assert.Equal(t, title, props["title"])
	})
}
