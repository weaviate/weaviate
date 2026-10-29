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
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// movementTimeout bounds each wait on a movement's state.
const movementTimeout = 2 * time.Minute

// movementPath orders the states a COPY or MOVE passes through. A COPY goes from
// INTEGRATING straight to READY.
var movementPath = []api.ShardReplicationState{
	api.REGISTERED, api.HYDRATING, api.FINALIZING, api.INTEGRATING, api.DEHYDRATING, api.READY,
}

// atOrPast reports whether got is want or a state after it on movementPath.
func atOrPast(got, want api.ShardReplicationState) bool {
	at := slices.Index(movementPath, got)
	return at >= 0 && at >= slices.Index(movementPath, want)
}

// newNamespaceOnNode creates a namespace homed on node and a namespaced user in
// it, and returns the namespace and the user's key.
func newNamespaceOnNode(t *testing.T, node string) (ns, userKey string) {
	t.Helper()

	ns = uniqueNS()
	helper.CreateNamespaceWithHomeNode(t, ns, node, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	userKey = createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })
	return ns, userKey
}

// newSingleShardClass creates a class in ns and returns its qualified name and
// its one shard.
func newSingleShardClass(t *testing.T, ns, class, userKey string) (qualifiedClass, shardName string) {
	t.Helper()

	setupClassInNs1(t, ns, class, userKey)
	qualifiedClass = ns + ":" + class
	return qualifiedClass, requireShardCountEventually(t, qualifiedClass, 1)[0]
}

// newPresentShard returns the shard of a class in a new active namespace homed on
// node, which node keeps listing while a test suspends another namespace.
func newPresentShard(t *testing.T, node string) presentShard {
	t.Helper()

	ns, userKey := newNamespaceOnNode(t, node)
	qualifiedClass, shardName := newSingleShardClass(t, ns, "Anchor", userKey)
	return presentShard{qualifiedClass, shardName}
}

// shardReplicas returns the nodes the sharding state names as replicas of one
// shard.
func shardReplicas(t *testing.T, qualifiedClass, shardName string) ([]string, error) {
	t.Helper()

	resp, err := helper.Client(t).Replication.GetCollectionShardingState(
		replication.NewGetCollectionShardingStateParams().WithCollection(&qualifiedClass).WithShard(&shardName),
		helper.CreateAuth(adminKey),
	)
	if err != nil {
		return nil, err
	}
	shards := resp.Payload.ShardingState.Shards
	if len(shards) != 1 {
		return nil, fmt.Errorf("the sharding state lists %d shards for %q, want 1", len(shards), shardName)
	}
	return shards[0].Replicas, nil
}

// seedObjects writes n objects into class through userKey, so a movement of its
// shard has data to copy, and returns their ids.
func seedObjects(t *testing.T, class, userKey string, n int) []strfmt.UUID {
	t.Helper()

	const batchSize = 1000
	ids := make([]strfmt.UUID, 0, n)
	for start := 0; start < n; start += batchSize {
		batch := make([]*models.Object, 0, min(batchSize, n-start))
		for i := start; i < start+cap(batch); i++ {
			id := strfmt.UUID(uuid.NewString())
			ids = append(ids, id)
			batch = append(batch, &models.Object{
				ID: id, Class: class, Properties: map[string]any{"title": fmt.Sprintf("seeded %d", i)},
			})
		}
		helper.CreateObjectsBatchAuth(t, batch, userKey)
	}
	return ids
}

// currentMovementState returns the op's state. It fails the test on an op that is
// gone or CANCELLED, because a suspend must only pause a movement. Any other error
// comes back for the caller to retry, since a leader election can cause one.
func currentMovementState(t *testing.T, opID strfmt.UUID) (api.ShardReplicationState, error) {
	t.Helper()

	state, messages, err := movementState(t, opID)
	var gone *replication.ReplicationDetailsNotFound
	require.False(t, errors.As(err, &gone), "op %s is gone", opID)
	if err != nil {
		return "", err
	}
	require.NotEqual(t, api.CANCELLED.String(), state, "op %s was cancelled; errors %v", opID, messages)
	return api.ShardReplicationState(state), nil
}

// waitMovementState polls the op until it reads want or a later state on
// movementPath, and returns the state it read. The op can move on before a
// suspend issued next applies.
func waitMovementState(t *testing.T, opID strfmt.UUID, want api.ShardReplicationState) api.ShardReplicationState {
	t.Helper()

	deadline := time.Now().Add(movementTimeout)
	for {
		got, err := currentMovementState(t, opID)
		if err == nil && atOrPast(got, want) {
			return got
		}
		require.True(t, time.Now().Before(deadline),
			"op %s never reached %s; last read %q, error %v", opID, want, got, err)
		time.Sleep(50 * time.Millisecond)
	}
}

// shardListedOnLiveNode reports whether node lists the shard. answered is false
// when node does not answer either read or does not list live, because a node
// that did not answer lists no shards either.
func shardListedOnLiveNode(t *testing.T, qualifiedClass, shardName, node string, live presentShard) (listed, answered bool, err error) {
	t.Helper()

	anchor, answered, err := shardOnNode(t, live.qualifiedClass, live.name, node)
	if err != nil || !answered || anchor == nil {
		return false, false, err
	}
	shard, answered, err := shardOnNode(t, qualifiedClass, shardName, node)
	if err != nil || !answered {
		return false, false, err
	}
	return shard != nil, true, nil
}

// requireShardListedThrough holds for d asserting node keeps listing the shard. Its
// listed and failed counters keep a window in which no listing finished from
// passing, and it counts no tick on which node does not list live.
func requireShardListedThrough(t *testing.T, qualifiedClass, shardName, node string, d time.Duration, live presentShard) {
	t.Helper()

	var listed, failed atomic.Int64
	require.Never(t, func() bool {
		present, answered, err := shardListedOnLiveNode(t, qualifiedClass, shardName, node, live)
		switch {
		case err != nil:
			failed.Add(1)
			return false
		case !answered:
			return false
		}
		listed.Add(1)
		return !present
	}, d, 250*time.Millisecond, "%q stopped listing shard %q", node, shardName)

	require.Zero(t, failed.Load(), "listing shards on %q failed", node)
	require.Positive(t, listed.Load(), "no shard listing finished on %q", node)
}

// requireShardListedUntil reads the shard on each node of live, then the op, until
// the op reaches want or d passes, and returns the state it last read. It fails
// when a node stops listing the shard while the op is short of want, or never
// lists it at all unless the first read already reached want.
func requireShardListedUntil(t *testing.T, qualifiedClass, shardName string, opID strfmt.UUID,
	want api.ShardReplicationState, d time.Duration, live map[string]presentShard,
) api.ShardReplicationState {
	t.Helper()

	var (
		got              api.ShardReplicationState
		finished, failed int
		listedTicks      = map[string]int{}
		wantAtFirstTick  bool
	)
	deadline := time.Now().Add(d)
	for tick := 0; ; tick++ {
		var dropped []string
		for node, anchor := range live {
			listed, answered, err := shardListedOnLiveNode(t, qualifiedClass, shardName, node, anchor)
			switch {
			case err != nil:
				failed++
			case !answered:
			case listed:
				finished++
				listedTicks[node]++
			default:
				finished++
				if listedTicks[node] > 0 {
					dropped = append(dropped, node)
				}
			}
		}

		// The op is read after the listings because it never moves back, so a
		// shard missing before a read short of want was missing while it was needed.
		if state, err := currentMovementState(t, opID); err == nil {
			got = state
			if atOrPast(got, want) {
				wantAtFirstTick = tick == 0
				break
			}
			require.Empty(t, dropped, "these nodes stopped listing %q with the op in %s", shardName, got)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	require.Zero(t, failed, "listing shard %q failed", shardName)
	require.Positive(t, finished, "no listing of shard %q finished", shardName)
	if !wantAtFirstTick {
		for node := range live {
			require.Positive(t, listedTicks[node], "%q never listed shard %q", node, shardName)
		}
	}
	return got
}

// requireNoMovementErrorsInHistory fails on an error recorded in any state the op
// has been in. requireNoMovementErrors reads only the current state's errors, and
// each state starts with none.
func requireNoMovementErrorsInHistory(t *testing.T, opID strfmt.UUID) {
	t.Helper()

	includeHistory := true
	details, err := helper.Client(t).Replication.ReplicationDetails(
		replication.NewReplicationDetailsParams().WithID(opID).WithIncludeHistory(&includeHistory),
		helper.CreateAuth(adminKey),
	)
	require.NoError(t, err)
	require.NotNil(t, details.Payload.Status)

	for _, status := range append(details.Payload.StatusHistory, details.Payload.Status) {
		require.Empty(t, status.Errors, "the op recorded errors in %s", status.State)
	}
}

// A COPY in FINALIZING under a suspend keeps its source and target loaded until
// READY.
func TestNamespaces_SuspendKeepsFinalizingMovement(t *testing.T) {
	t.Parallel()

	const (
		sourceNode = docker.Weaviate1
		targetNode = docker.Weaviate0
		class      = "FinalizingCopy"
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	qualified, shardName := newSingleShardClass(t, ns, class, userKey)
	seedObjects(t, class, userKey, 1000)
	live := map[string]presentShard{
		sourceNode: newPresentShard(t, sourceNode),
		targetNode: newPresentShard(t, targetNode),
	}

	opID := startCopyMovement(t, qualified, shardName, sourceNode, targetNode)
	t.Logf("suspending with the op in %s", waitMovementState(t, opID, api.FINALIZING))
	helper.SuspendNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

	t.Run("the source and target stay listed until READY", func(t *testing.T) {
		got := requireShardListedUntil(t, qualified, shardName, opID, api.READY, movementTimeout, live)
		require.Equal(t, api.READY, got)
	})
}

// A COPY in HYDRATING under a suspend keeps its source loaded. The source refuses a
// new change capture or snapshot while suspended, so depending on how far the copy
// got, the op reaches READY or waits in HYDRATING without an error.
func TestNamespaces_SuspendKeepsHydratingMovement(t *testing.T) {
	t.Parallel()

	const (
		sourceNode = docker.Weaviate1
		targetNode = docker.Weaviate0
		class      = "HydratingCopy"
		// objectCount is sized so the file copy usually outlasts the suspend and a
		// sweep pass.
		objectCount = 50_000
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	qualified, shardName := newSingleShardClass(t, ns, class, userKey)
	seedObjects(t, class, userKey, objectCount)
	live := map[string]presentShard{sourceNode: newPresentShard(t, sourceNode)}

	opID := startCopyMovement(t, qualified, shardName, sourceNode, targetNode)
	t.Logf("suspending with the op in %s", waitMovementState(t, opID, api.HYDRATING))
	helper.SuspendNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

	got := requireShardListedUntil(t, qualified, shardName, opID, api.READY, movementTimeout, live)
	if got == api.HYDRATING {
		t.Run("the op waits in HYDRATING without an error until the resume", func(t *testing.T) {
			requireNoMovementErrors(t, opID)
			helper.ResumeNamespace(t, ns, adminKey)
			waitMovementState(t, opID, api.READY)
		})
		return
	}
	if got != api.READY {
		got = requireShardListedUntil(t, qualified, shardName, opID, api.READY, movementTimeout, live)
	}
	require.Equal(t, api.READY, got, "the op stopped short of READY under the suspend")

	t.Run("no state of the op recorded an error", func(t *testing.T) {
		requireNoMovementErrorsInHistory(t, opID)
	})
}

// A MOVE held in INTEGRATING under a suspend keeps its source and target loaded,
// then reaches READY on the target alone. Every object written before the move
// reads back after the resume. Not parallel, because it stops a third node to
// hold the op in INTEGRATING.
//
// No write runs during the move, because with NAMESPACES_ENABLED the target fails
// to replay a captured write, whose object is stored without its class name.
func TestNamespaces_SuspendKeepsIntegratingMove(t *testing.T) {
	const (
		sourceNode = docker.Weaviate1
		targetNode = docker.Weaviate0
		class      = "IntegratingMove"
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	qualified, shardName := newSingleShardClass(t, ns, class, userKey)
	written := seedObjects(t, class, userKey, 1000)
	live := map[string]presentShard{
		sourceNode: newPresentShard(t, sourceNode),
		targetNode: newPresentShard(t, targetNode),
	}

	// The target's consumer leaves INTEGRATING only once every node has reported
	// it, so the op waits there while restartNodeName is down.
	var opID strfmt.UUID
	withNodeStopped(t, restartNodeName, nil, func(t *testing.T) {
		var err error
		opID, err = registerMovement(t, qualified, shardName, sourceNode, targetNode, api.MOVE.String())
		require.NoError(t, err)
		require.Equal(t, api.INTEGRATING, waitMovementState(t, opID, api.INTEGRATING))
		helper.SuspendNamespace(t, ns, adminKey)
		t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

		t.Run("the source and target stay listed while the op waits in INTEGRATING", func(t *testing.T) {
			for node, anchor := range live {
				requireShardListedThrough(t, qualified, shardName, node, sweepWindow, anchor)
			}
			require.Equal(t, api.INTEGRATING, waitMovementState(t, opID, api.INTEGRATING),
				"the op left INTEGRATING while the listings ran")
		})
	})

	t.Run("the op reaches READY with only the target holding the shard", func(t *testing.T) {
		waitMovementState(t, opID, api.READY)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			replicas, err := shardReplicas(t, qualified, shardName)
			if assert.NoError(c, err) {
				assert.Equal(c, []string{targetNode}, replicas)
			}
		}, 30*time.Second, 250*time.Millisecond, "the sharding state never listed only %q", targetNode)
	})

	t.Run("every object written before the move reads back after the resume", func(t *testing.T) {
		helper.ResumeNamespace(t, ns, adminKey)
		for _, id := range written {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				obj, err := helper.GetObjectAuth(t, qualified, id, adminKey)
				if assert.NoError(c, err) {
					assert.NotNil(c, obj)
				}
			}, 30*time.Second, 250*time.Millisecond, "object %s never read back", id)
		}
	})
}

// A movement waiting in REGISTERED under the suspend keeps its source loaded until
// it ends, whether force-deleted or cancelled. Not parallel, because it stops the
// target to hold both ops in REGISTERED.
func TestNamespaces_SuspendReleasesEndedMovement(t *testing.T) {
	const (
		sourceNode     = docker.Weaviate1
		targetNode     = restartNodeName
		forcedClass    = "ForceDeletedCopy"
		cancelledClass = "CancelledCopy"
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	forcedQualified, forcedShard := newSingleShardClass(t, ns, forcedClass, userKey)
	cancelledQualified, cancelledShard := newSingleShardClass(t, ns, cancelledClass, userKey)
	live := newPresentShard(t, sourceNode)

	var cancelledOp strfmt.UUID
	withNodeStopped(t, targetNode, nil, func(t *testing.T) {
		// Registration checks only the shard's replica list, so it accepts a
		// stopped target.
		forcedOp := startCopyMovement(t, forcedQualified, forcedShard, sourceNode, targetNode)
		cancelledOp = startCopyMovement(t, cancelledQualified, cancelledShard, sourceNode, targetNode)
		helper.SuspendNamespace(t, ns, adminKey)
		t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

		t.Run("both sources stay listed", func(t *testing.T) {
			requireShardListedThrough(t, forcedQualified, forcedShard, sourceNode, sweepWindow, live)
			requireShardListedThrough(t, cancelledQualified, cancelledShard, sourceNode, sweepWindow, live)
		})

		t.Run("a force-deleted op releases its source and no other", func(t *testing.T) {
			_, err := helper.Client(t).Replication.ForceDeleteReplications(
				replication.NewForceDeleteReplicationsParams().WithBody(
					&models.ReplicationReplicateForceDeleteRequest{ID: forcedOp}),
				helper.CreateAuth(adminKey),
			)
			require.NoError(t, err)

			requireShardReleasedOnNode(t, forcedQualified, forcedShard, sourceNode, live)
			requireShardListedThrough(t, cancelledQualified, cancelledShard, sourceNode, sweepWindow, live)
		})

		_, err := helper.Client(t).Replication.CancelReplication(
			replication.NewCancelReplicationParams().WithID(cancelledOp), helper.CreateAuth(adminKey))
		require.NoError(t, err)
	})

	// The cancel completes only once the target's consumer is back to run it.
	t.Run("a cancelled op releases its source once its target is back", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, messages, err := movementState(t, cancelledOp)
			if assert.NoError(c, err) {
				assert.Equal(c, api.CANCELLED.String(), state, "op still %q; errors %v", state, messages)
			}
		}, movementTimeout, 500*time.Millisecond, "the cancel never completed")

		requireShardReleasedOnNode(t, cancelledQualified, cancelledShard, sourceNode, live)
	})
}

// A COPY target that restarts in FINALIZING under the suspend registers none of the
// namespace's shards at boot. Its consumer's retry reloads the shard, which it keeps
// until READY. Not parallel, because it stops the target.
func TestNamespaces_SuspendKeepsMovementTargetAcrossRestart(t *testing.T) {
	const (
		sourceNode = docker.Weaviate1
		targetNode = restartNodeName
		class      = "RestartedTarget"
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	qualified, shardName := newSingleShardClass(t, ns, class, userKey)
	seedObjects(t, class, userKey, 1000)
	live := map[string]presentShard{
		sourceNode: newPresentShard(t, sourceNode),
		targetNode: newPresentShard(t, targetNode),
	}

	opID := startCopyMovement(t, qualified, shardName, sourceNode, targetNode)
	waitMovementState(t, opID, api.FINALIZING)

	// The op cannot advance while the target's consumer is down, so the state
	// read with the target stopped is the one its restart meets. The test kills
	// the target, because the op leaves FINALIZING long before a graceful stop ends.
	kill := time.Duration(0)
	var stoppedIn api.ShardReplicationState
	withNodeStopped(t, targetNode, &kill, func(t *testing.T) {
		stoppedIn = waitMovementState(t, opID, api.FINALIZING)
		helper.SuspendNamespace(t, ns, adminKey)
		t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })
	})

	if stoppedIn != api.FINALIZING {
		waitMovementState(t, opID, api.READY)
		t.Skipf("the target went down with the op in %s, so the restart in FINALIZING was not reached", stoppedIn)
	}

	t.Run("the target's consumer reloads the shard its boot left out", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			shard, _, err := shardOnNode(t, qualified, shardName, targetNode)
			if assert.NoError(c, err) {
				assert.NotNil(c, shard)
			}
		}, movementTimeout, 250*time.Millisecond, "%q never listed shard %q again", targetNode, shardName)
	})

	t.Run("the reloaded target stays listed until READY", func(t *testing.T) {
		got := requireShardListedUntil(t, qualified, shardName, opID, api.READY, movementTimeout, live)
		require.Equal(t, api.READY, got)
	})
}

// removeShardDirAndKill deletes one shard's directory on node and kills node, so
// its next boot finds the directory missing. It kills, because a graceful stop
// flushes into the directory and recreates it.
func removeShardDirAndKill(t *testing.T, node, class, shard string) {
	t.Helper()
	ctx := context.Background()
	// StopNode counts from 0, matching the container name, where GetWeaviateNode
	// counts from 1.
	index := nodeIndexFromName(t, node)
	suffix := index - 1
	t.Cleanup(func() { require.NoError(t, sharedCompose.EnsureRunning(ctx, suffix)) })

	dir := "/data/" + strings.ToLower(class) + "/" + shard
	code, _, err := sharedCompose.GetWeaviateNode(index).Container().
		Exec(ctx, []string{"sh", "-c", `test -d "$1" || exit 3; rm -rf "$1"`, "sh", dir})
	require.NoError(t, err, "exec rm %s on %q", dir, node)
	require.Zero(t, code, "rm %s on %q exited %d, where 3 means the directory is missing", dir, node, code)

	zero := time.Duration(0)
	require.NoError(t, sharedCompose.StopNode(ctx, suffix, &zero))
}

// waitSelfRecoveryOp polls the ops targeting node until one is a SELF_RECOVERY of
// the shard, and returns its id.
func waitSelfRecoveryOp(t *testing.T, qualifiedClass, shardName, node string) strfmt.UUID {
	t.Helper()

	var opID strfmt.UUID
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams().WithTargetNode(&node), helper.CreateAuth(adminKey))
		if !assert.NoError(c, err) {
			return
		}
		for _, op := range resp.Payload {
			if op.ID != nil && op.Type != nil && *op.Type == api.SELF_RECOVERY.String() &&
				op.Collection != nil && *op.Collection == qualifiedClass && op.Shard != nil && *op.Shard == shardName {
				opID = *op.ID
				return
			}
		}
		assert.Fail(c, "no SELF_RECOVERY op of the shard yet")
	}, movementTimeout, 250*time.Millisecond, "%q never registered a SELF_RECOVERY of shard %q", node, shardName)
	return opID
}

// A SELF_RECOVERY in FINALIZING under a suspend keeps its promoted shard and its
// source loaded until READY. Not parallel, because it kills and restarts a node.
// That node keeps its RAFT state, so this covers an intact node's recovery.
func TestNamespaces_SuspendKeepsSelfRecovery(t *testing.T) {
	const (
		sourceNode = docker.Weaviate1
		targetNode = restartNodeName
		class      = "SelfRecovered"
	)
	ns, userKey := newNamespaceOnNode(t, sourceNode)
	qualified, shardName := newSingleShardClass(t, ns, class, userKey)
	// A probe that finds no objects on any peer sends the recovery to the empty
	// fallback, which registers no op.
	seedObjects(t, class, userKey, 1000)
	live := map[string]presentShard{
		sourceNode: newPresentShard(t, sourceNode),
		targetNode: newPresentShard(t, targetNode),
	}

	// The COPY gives targetNode the replica whose directory its boot then misses.
	waitMovementState(t, startCopyMovement(t, qualified, shardName, sourceNode, targetNode), api.READY)
	removeShardDirAndKill(t, targetNode, qualified, shardName)
	require.NoError(t, sharedCompose.StartNode(context.Background(), restartNodeIndex))

	opID := waitSelfRecoveryOp(t, qualified, shardName, targetNode)
	if got := waitMovementState(t, opID, api.FINALIZING); got != api.FINALIZING {
		waitMovementState(t, opID, api.READY)
		t.Skipf("the op was in %s before the suspend, so FINALIZING under the suspend was not reached", got)
	}
	helper.SuspendNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })
	if got, err := currentMovementState(t, opID); err == nil && got == api.READY {
		t.Skip("the op read READY as soon as the suspend returned, so the listings below would assert nothing")
	}

	t.Run("the promoted shard and the source stay listed until READY", func(t *testing.T) {
		got := requireShardListedUntil(t, qualified, shardName, opID, api.READY, movementTimeout, live)
		require.Equal(t, api.READY, got)
	})
}
