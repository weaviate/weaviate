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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// maxPlacementAttempts bounds the search for the two placements the test
// needs; shards land off a shuffled list, so 16 draws make bad luck rare.
const maxPlacementAttempts = 16

// probeDataset sizes the reindex-target class; it only needs to exist, since
// the backup window comes from the other (backup) class.
const probeDataset = 500

// guardTopology is the placement the remote-backup test is built on: a backup
// that lives entirely on one node, and a reindex target owned by another.
type guardTopology struct {
	backupClass  string
	reindexClass string
	probe        clusterNode
	placements   []string
}

// TestMultiNodeReindexRefusedWhileRemoteNodeBacksUp asserts a node with no
// stake in a running backup still refuses reindex — the property the
// cluster-wide fan-out exists for. Two classes split ownership so the probe
// node provably holds none of the backup: a node-local check would answer 202.
func TestMultiNodeReindexRefusedWhileRemoteNodeBacksUp(t *testing.T) {
	ctx := context.Background()

	const (
		propName  = "body"
		backupID  = "reindex-guard-remote-backup"
		bucket    = "reindex-guard-bucket"
		backend   = "s3"
		nodeCount = 3
		// A 2+ node cluster refuses the filesystem backend, so the backup goes through MinIO.
		region = "us-east-1"
	)

	compose, err := docker.New().
		With3NodeCluster().
		WithBackendS3(bucket, region).
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1").
		WithWeaviateEnv("DISABLE_LAZY_LOAD_SHARDS", "true").
		WithWeaviateEnv("MEMBERLIST_FAST_FAILURE_DETECTION", "false").
		WithWeaviateEnv("USE_INVERTED_SEARCHABLE", "false").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	nodes := clusterNodes(compose, nodeCount)
	defer func() {
		for i := 1; i <= nodeCount; i++ {
			dumpWeaviateLogs(ctx, t, compose.GetWeaviateNode(i).Container(), nodes[i-1].name)
		}
	}()

	helper.SetupClient(nodes[0].uri)
	defer helper.ResetClient()

	leader := raftLeaderName(t, nodes[0].uri, 60*time.Second)
	topo := resolveGuardTopology(t, nodes, leader, propName)
	coordinator := nodeByName(t, nodes, leader)
	t.Logf("topology: leader %q owns backup class %q, probe %q owns reindex class %q; placements sampled: %v",
		leader, topo.backupClass, topo.probe.name, topo.reindexClass, topo.placements)

	// Without this the test can still pass on a node-local check: if the probe
	// node also held the backup, its own slot would produce the 409 and the
	// cluster-wide fan-out would never be exercised. resolveGuardTopology is
	// supposed to rule that out, so state it against the API rather than trust it.
	require.NotEqual(t, leader, topo.probe.name,
		"the probe node must not be the node that runs the backup, or the 409 proves nothing cluster-wide")
	backupOwners, ok := shardOwners(nodes[0].uri, topo.backupClass)
	require.True(t, ok, "could not read shard ownership of backup class %q", topo.backupClass)
	require.NotContains(t, backupOwners, topo.probe.name,
		"the probe node holds a shard of the backup class %q, so a node-local check would also answer 409",
		topo.backupClass)

	awaitClassVisible(t, coordinator.uri, topo.backupClass, coordinator.name)
	awaitClassVisible(t, topo.probe.uri, topo.reindexClass, topo.probe.name)

	// All objects land on the leader, so the corpus size that buys a
	// multi-second window in the single-node tests works here too.
	importBodies(t, topo.backupClass, guardDataset)
	importBodies(t, topo.reindexClass, probeDataset)

	// A leadership change since resolving the topology could enroll a node
	// the test didn't account for.
	require.Equal(t, leader, raftLeaderName(t, nodes[0].uri, 60*time.Second),
		"the RAFT leader changed during setup; the resolved topology no longer holds")
	require.NoError(t, startS3Backup(coordinator.uri, topo.backupClass, backupID, bucket))

	statusOf := nodeBackupStatus(coordinator.uri, backend, backupID)
	run := probeReindexDuringBackup(t, topo.probe.uri, topo.reindexClass, propName, "whitespace",
		statusOf, 10*time.Minute)
	blocked := assertReindexBlocked(t, run, backupID)

	// The backup includes only the leader's class, so the probe node holds no
	// slot for it. The 409 is therefore only reachable by asking the leader.
	t.Logf("probe node %q refused while only leader %q held backup %s: %s",
		topo.probe.name, leader, backupID, blocked.body)

	// assertReindexBlocked already covers the backup id; node names need
	// read_nodes, which this route does not.
	message := guardMessage(blocked.body)
	for _, node := range nodes {
		assert.NotContainsf(t, message, node.name,
			"the 409 body leaked node %q", node.name)
	}

	// The block has to lift on a node that never took part in the backup.
	awaitBackupSuccess(t, statusOf, backupID, 10*time.Minute)
	taskID := awaitReindexAccepted(t, topo.probe.uri, topo.reindexClass, propName, "whitespace", 60*time.Second)
	t.Logf("reindex accepted on probe node %q after backup %s finished: task %s",
		topo.probe.name, backupID, taskID)
}

// resolveGuardTopology creates single-shard, RF=1 classes until one lands on
// the RAFT leader and another elsewhere; placement can't be requested directly.
func resolveGuardTopology(t *testing.T, nodes []clusterNode, leader, propName string) guardTopology {
	t.Helper()

	var topo guardTopology
	var spares []string
	for attempt := 0; attempt < maxPlacementAttempts; attempt++ {
		className := fmt.Sprintf("ReindexGuard_RemoteBackup_%d", attempt)
		helper.CreateClass(t, &models.Class{
			Class: className,
			Properties: []*models.Property{
				{Name: propName, DataType: []string{"text"}, Tokenization: "word"},
			},
			Vectorizer: "none",
			// One shard at RF=1 puts the whole class on a single node.
			ShardingConfig:    map[string]interface{}{"desiredCount": 1},
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		})
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
		t.Fatalf("could not resolve the placement the test needs within %d attempts: it needs one "+
			"class owned by RAFT leader %q and one owned by another node of %v; ownership seen: %v",
			maxPlacementAttempts, leader, nodeNames(nodes), topo.placements)
	}
	return topo
}

// awaitClassVisible blocks until the node's own schema view serves the class
// (what the submission handler checks).
func awaitClassVisible(t *testing.T, restURI, className, nodeName string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		_, ok := reindexhelpers.FetchClass(restURI, className, true)
		return ok
	}, 60*time.Second, 100*time.Millisecond,
		"class %s must be locally visible on %s before the test drives it", className, nodeName)
}
