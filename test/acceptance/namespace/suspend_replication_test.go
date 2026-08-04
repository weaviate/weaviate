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
	"strings"
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

// A replica movement is refused while its collection's namespace is suspended,
// and finishes once the namespace is back.
//
// The refusal comes from the source node. Every entry point a movement uses to
// read the source shard takes the request-path namespace check, so the movement
// stops there — before the target has materialized anything.
//
// What this does NOT cover: the target-side exemption itself. A movement only
// loads its target shard in FINALIZING, which this movement never reaches while
// suspended, and by the time the resume lets it through the namespace is active
// again — where the exempt and non-exempt loads behave identically. Reaching
// FINALIZING with the namespace suspended needs a suspend landing inside that
// window, which no hook here makes deterministic.
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
		// Asserting the message, not just a stall: a movement that never ran at
		// all would also never reach READY.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, messages, err := movementState(t, opID)
			if !assert.NoError(c, err) {
				return
			}
			refused := false
			for _, m := range messages {
				refused = refused || strings.Contains(m, "namespace is suspended")
			}
			assert.True(c, refused, "no error named the suspension; state %q, errors %v", state, messages)
		}, 60*time.Second, 1*time.Second, "the movement never recorded the source's refusal")
	})

	t.Run("the target materializes no shard", func(t *testing.T) {
		requireShardAbsentOnNode(t, qualified, shardName, targetNode)

		// The op keeps retrying, so it is neither done nor given up on. CANCELLED
		// would be the error budget running out, which the window above stays
		// well inside — read here so the resume below fails on its own terms.
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
