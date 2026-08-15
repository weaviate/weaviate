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

package reindex_backup_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// guardTopology is a capture living entirely on one node, and a reindex target
// owned by another.
type guardTopology struct {
	backupClass  string
	reindexClass string
	probe        clusterNode
	placements   []string
}

// A node holding no stake at all in a running capture must still refuse a
// submission, which it can only know by asking its peers — a node-local check
// would admit here.
func TestMultiNodeReindexRefusedWhileRemoteNodeBacksUp(t *testing.T) {
	ctx := context.Background()

	const (
		propName = "body"
		backupID = "reindex-guard-remote-backup"
		bucket   = "reindex-guard-bucket"
		backend  = "s3"
	)

	compose := startGuardCluster(ctx, t, bucket)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	nodes := clusterNodes(compose, guardClusterSize)
	t.Cleanup(func() { dumpClusterLogs(ctx, t, compose, nodes) })

	helper.SetupClient(nodes[0].uri)
	t.Cleanup(helper.ResetClient)

	leader := raftLeaderName(t, nodes[0].uri, 60*time.Second)
	awaitClusterMembers(t, nodes[0].uri, nodeNames(nodes), 60*time.Second)
	topo := resolveGuardTopology(t, nodes, leader, propName)
	coordinator := nodeByName(t, nodes, leader)
	t.Logf("topology: leader %q owns capture class %q, probe %q owns reindex class %q; sampled: %v",
		leader, topo.backupClass, topo.probe.name, topo.reindexClass, topo.placements)

	// Confirmed against the nodes API, not just the placement bookkeeping.
	require.NotEqual(t, leader, topo.probe.name,
		"the probe node must not be the node running the capture, or the 409 proves nothing cluster-wide")
	backupOwners, ok := shardOwners(nodes[0].uri, topo.backupClass)
	require.True(t, ok, "could not read shard ownership of capture class %q", topo.backupClass)
	require.NotContains(t, backupOwners, topo.probe.name,
		"the probe node holds a shard of the captured class %q, so a node-local check would also refuse",
		topo.backupClass)

	awaitClassVisible(t, coordinator.uri, topo.backupClass, coordinator.name)
	awaitClassVisible(t, topo.probe.uri, topo.reindexClass, topo.probe.name)

	importBodies(t, topo.backupClass, guardDataset)
	importBodies(t, topo.reindexClass, probeDataset)

	require.Equal(t, leader, raftLeaderName(t, nodes[0].uri, 60*time.Second),
		"the RAFT leader changed during setup; the resolved topology no longer holds")
	require.NoError(t, startS3Backup(coordinator.uri, topo.backupClass, backupID, bucket))

	statusOf := nodeBackupStatus(coordinator.uri, backend, backupID)
	run := probeReindexDuringBackup(t, topo.probe.uri, topo.reindexClass, propName, "whitespace",
		statusOf, 10*time.Minute)
	blocked := assertReindexBlocked(t, run, backupID)
	t.Logf("probe node %q refused while only leader %q held backup %s: %s",
		topo.probe.name, leader, backupID, blocked.body)

	// The caller holds a grant on one collection only.
	message := guardMessage(blocked.body)
	for _, node := range nodes {
		assert.NotContainsf(t, message, node.name, "the 409 body leaked node %q", node.name)
	}
	assert.NotContainsf(t, message, topo.backupClass,
		"the 409 body leaked the captured collection %q, which the caller was never granted",
		topo.backupClass)
	for _, placement := range topo.placements {
		className, _, _ := strings.Cut(placement, "=")
		if className == topo.reindexClass {
			continue
		}
		assert.NotContainsf(t, message, className,
			"the 409 body leaked collection %q, which the caller has no grant on", className)
	}

	// The negative arm: the block has to lift on a node that never took part.
	awaitBackupSuccess(t, statusOf, backupID, 10*time.Minute)
	taskID := awaitReindexAccepted(t, topo.probe.uri, topo.reindexClass, propName, "whitespace", 60*time.Second)
	t.Logf("reindex accepted on probe node %q after backup %s finished: task %s",
		topo.probe.name, backupID, taskID)
}

// Closes the gap the single-node suite cannot: there the node that refuses a
// capture and the node that publishes the refusal are one process, so nothing
// proves the coordinator rebuilds a participant's answer rather than forwarding
// it, node and shard names included.
func TestMultiNodeBackupRefusalFromARemoteParticipantNamesNoNode(t *testing.T) {
	ctx := context.Background()

	const (
		propName = "body"
		backupID = "reindex-guard-remote-refusal"
		bucket   = "reindex-guard-refusal-bucket"
	)

	compose := startGuardCluster(ctx, t, bucket)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	nodes := clusterNodes(compose, guardClusterSize)
	t.Cleanup(func() { dumpClusterLogs(ctx, t, compose, nodes) })

	helper.SetupClient(nodes[0].uri)
	t.Cleanup(helper.ResetClient)

	leader := raftLeaderName(t, nodes[0].uri, 60*time.Second)
	awaitClusterMembers(t, nodes[0].uri, nodeNames(nodes), 60*time.Second)
	topo := resolveGuardTopology(t, nodes, leader, propName)
	coordinator := nodeByName(t, nodes, leader)

	// The class the leader does NOT own is the one that migrates, so the
	// refusal has to travel back from a participant.
	migratingClass := topo.reindexClass
	require.NotEqual(t, leader, topo.probe.name)
	owners, ok := shardOwners(nodes[0].uri, migratingClass)
	require.True(t, ok)
	require.NotContains(t, owners, leader,
		"the coordinator must hold no shard of %q, or it could refuse without asking anyone",
		migratingClass)
	shardOwner := topo.probe

	awaitClassVisible(t, shardOwner.uri, migratingClass, shardOwner.name)
	importBodies(t, migratingClass, guardDataset)

	taskID := awaitReindexAccepted(t, shardOwner.uri, migratingClass, propName, "lowercase", 60*time.Second)
	reindexhelpers.AwaitReindexLive(t, shardOwner.uri, taskID, reindexhelpers.WithTimeout(60*time.Second))

	statusBefore := reindexTaskStatus(t, shardOwner.uri, taskID)
	httpStatus, body, ok := tryS3Backup(coordinator.uri, migratingClass, backupID, bucket)
	require.True(t, ok, "the capture request must reach the coordinator")
	statusAfter := reindexTaskStatus(t, shardOwner.uri, taskID)

	// Judge the window before the verdict: a migration that drained mid-call
	// would make either outcome meaningless.
	require.Truef(t, liveReindexStatus(statusBefore) && liveReindexStatus(statusAfter),
		"the migration must still be live on both sides of the capture request (before=%q after=%q); "+
			"grow guardDataset until it outlives the call", statusBefore, statusAfter)

	// Refused, without pinning which code: the backup branch answers 500 where
	// its restore twin answers 422, a defect this branch neither owns nor fixes:
	// https://github.com/weaviate/0-weaviate-issues/issues/582
	require.GreaterOrEqualf(t, httpStatus, http.StatusBadRequest,
		"a capture a participant refuses must not be admitted: %s", body)

	message := guardMessage(body)
	require.Contains(t, message, "backup blocked: runtime-reindex in flight",
		"the body must name the refused operation and the blocking condition; got: %s", message)
	require.Contains(t, message, migratingClass,
		"the body must name the collection the caller asked about; got: %s", message)

	// Read off the live cluster, not written as literals: a redaction assertion
	// against a string production never emits passes whatever production does.
	shardName := reindexhelpers.GetFirstShardName(t, shardOwner.uri, migratingClass)
	require.NotEmptyf(t, shardName, "could not read a real shard name for %q to redact against",
		migratingClass)
	require.NotContainsf(t, message, shardName,
		"the refusal names shard %q, which the caller has no other way to learn; got: %s",
		shardName, message)
	for _, node := range nodes {
		require.NotContainsf(t, message, node.name,
			"the coordinator forwarded a participant's wording naming node %q instead of rebuilding "+
				"the refusal from the caller's own request; got: %s", node.name, message)
	}

	// The negative arm: once the migration drains, the same capture is admitted
	// through the same coordinator and the same participant.
	reindexhelpers.AwaitReindexViaIndexes(t, shardOwner.uri, migratingClass, propName,
		"searchable", reindexhelpers.WithTimeout(300*time.Second))
	// A fresh id: the refused attempt above may have left state under the old one.
	const drainedBackupID = "reindex-guard-remote-refusal-drained"
	require.Eventuallyf(t, func() bool {
		status, _, ok := tryS3Backup(coordinator.uri, migratingClass, drainedBackupID, bucket)
		return ok && status == http.StatusOK
	}, 60*time.Second, 500*time.Millisecond,
		"the same capture must be admitted once the migration has drained")
	awaitBackupSuccess(t, nodeBackupStatus(coordinator.uri, "s3", drainedBackupID), drainedBackupID, 10*time.Minute)
}

// resolveGuardTopology creates single-shard, RF=1 classes until one lands on
// the RAFT leader and another lands elsewhere. Placement cannot be requested.
func resolveGuardTopology(t *testing.T, nodes []clusterNode, leader, propName string) guardTopology {
	t.Helper()

	var topo guardTopology
	var spares []string
	for attempt := 0; attempt < maxPlacementAttempts; attempt++ {
		// Zero-padded so no sampled name is a prefix of another: the redaction
		// assertions are substring checks.
		className := fmt.Sprintf("ReindexGuard_Placement_%02d", attempt)
		createSingleShardClass(t, className, propName)
		owner := awaitSingleShardOwner(t, nodes[0].uri, className, 60*time.Second)
		topo.placements = append(topo.placements, fmt.Sprintf("%s=%s", className, owner))

		switch {
		case owner == leader && topo.backupClass == "":
			topo.backupClass = className
		case owner != leader && topo.reindexClass == "":
			topo.reindexClass = className
			topo.probe = nodeByName(t, nodes, owner)
		default:
			spares = append(spares, className)
		}
		if topo.backupClass != "" && topo.reindexClass != "" {
			break
		}
	}

	// Drop the losing samples so only the two topology classes remain.
	for _, className := range spares {
		helper.DeleteClass(t, className)
	}

	if topo.backupClass == "" || topo.reindexClass == "" {
		t.Fatalf("could not resolve the placement this test needs within %d attempts: it needs one "+
			"class owned by RAFT leader %q and one owned by another node of %v; ownership seen: %v",
			maxPlacementAttempts, leader, nodeNames(nodes), topo.placements)
	}
	return topo
}
