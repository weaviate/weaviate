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
	"sync/atomic"
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

// requireShardAbsentOnNode holds for a window asserting node never reports the
// shard. Same two counters as requireShardAbsent, for the same reason:
// require.Never returns true on its own deadline even if no check finished, and
// a node that did not answer is reported as a node with no shards and no error.
func requireShardAbsentOnNode(t *testing.T, qualifiedClass, shardName, node string) {
	t.Helper()

	var listed, failed atomic.Int64
	require.Never(t, func() bool {
		shard, err := shardOnNode(t, qualifiedClass, shardName, node)
		if err != nil {
			failed.Add(1)
			return false
		}
		listed.Add(1)
		return shard != nil
	}, 5*time.Second, 250*time.Millisecond, "shard %q must not be materialized on %q", shardName, node)

	require.Zero(t, failed.Load(), "listing shards for %q failed", qualifiedClass)
	require.Positive(t, listed.Load(), "no shard listing finished for %q", qualifiedClass)
}

// requireNoMovementErrors holds for a window asserting the op records no error.
// The FSM cancels a movement at 50 and drops the files its target already
// copied, so a suspension charged here would destroy a movement that only had to
// wait for the namespace. Same two counters as requireShardAbsentOnNode, for the
// same reason.
func requireNoMovementErrors(t *testing.T, opID strfmt.UUID) {
	t.Helper()

	var read, failed atomic.Int64
	require.Never(t, func() bool {
		_, messages, err := movementState(t, opID)
		if err != nil {
			failed.Add(1)
			return false
		}
		read.Add(1)
		return len(messages) > 0
	}, 20*time.Second, 1*time.Second, "the suspension was charged to the movement's error budget")

	require.Zero(t, failed.Load(), "reading the movement's state failed")
	require.Positive(t, read.Load(), "no movement state was read")
}

// startCopyMovement registers a COPY of one shard onto targetNode and returns the
// op id. Registered by the global operator against the qualified class name: a
// suspended namespace rejects its own key, and the handler resolves no short
// names of its own.
func startCopyMovement(t *testing.T, qualifiedClass, shardName, sourceNode, targetNode string) strfmt.UUID {
	t.Helper()

	copyType := api.COPY.String()
	resp, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &qualifiedClass,
			Shard:      &shardName,
			SourceNode: &sourceNode,
			TargetNode: &targetNode,
			Type:       &copyType,
		}),
		helper.CreateAuth(adminKey),
	)
	require.NoError(t, err, "failed to register COPY of %q %s->%s", shardName, sourceNode, targetNode)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.ID)
	return *resp.Payload.ID
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
// That is what happens, not what should: a suspended namespace ought to refuse
// this command before the schema records anything. Both halves below invert when
// it does — the scale plan is refused, and the target materializes nothing.
// Until then a suspend leaves an open shard on a node that is not the
// namespace's home node, and nothing closes it before the next restart.
func TestNamespaces_SuspendMaterializesEmptyReplicaAdd(t *testing.T) {
	t.Parallel()

	const (
		// The namespace pins its shards to its home node, so the addition lands
		// on a node that holds nothing of this class.
		homeNode   = docker.Weaviate1
		targetNode = docker.Weaviate0
		class      = "EmptyReplicaShard"
		planID     = strfmt.UUID("6f1c9d84-2a70-4c1e-8b53-9e0d17a4c6b2")
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

	t.Run("the target materializes the shard", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			shard, err := shardOnNode(t, qualified, shardName, targetNode)
			if !assert.NoError(c, err) {
				return
			}
			assert.NotNil(c, shard, "shard %q never appeared on %q", shardName, targetNode)
		}, 30*time.Second, 250*time.Millisecond, "the replica add opened no shard on the target")
	})
}

// A replica movement started while its collection's namespace is suspended is
// refused, and finishes once the namespace is back.
//
// The refusal comes from the source node: a movement opens change capture through
// the request path, which a suspend refuses, so it stops there — before the target
// has materialized anything. It re-reads the source on every dispatch, so a
// movement that was already copying files is refused the same way once its next
// dispatch lands. Neither spends the op's error budget, which is what lets a
// suspend of any length pause a movement rather than cancel it.
//
// What this does NOT cover: the target-side exemption itself, which a movement
// reaches once it starts finalizing. Getting there with the namespace suspended
// needs a suspend landing inside that window, which no hook here makes
// deterministic.
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

	opID := startCopyMovement(t, qualified, shardName, sourceNode, targetNode)

	t.Run("the movement stops on the source's namespace check", func(t *testing.T) {
		// Asserting where it stops, not just that it stalls: a movement that
		// never ran at all would also never reach READY, and would still read
		// REGISTERED here.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, messages, err := movementState(t, opID)
			if !assert.NoError(c, err) {
				return
			}
			assert.Equal(c, "HYDRATING", state, "the movement stopped elsewhere; errors %v", messages)
		}, 60*time.Second, 1*time.Second, "the movement never reached the source's refusal")
	})

	t.Run("the refusal is not charged to the movement", func(t *testing.T) {
		requireNoMovementErrors(t, opID)
	})

	t.Run("the target materializes no shard", func(t *testing.T) {
		requireShardAbsentOnNode(t, qualified, shardName, targetNode)

		// The op keeps retrying, so it is neither done nor given up on.
		state, _, err := movementState(t, opID)
		require.NoError(t, err)
		require.NotContains(t, []string{"READY", "CANCELLED"}, state)
	})

	t.Run("resuming lets the movement finish", func(t *testing.T) {
		helper.ResumeNamespace(t, ns, adminKey)

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
