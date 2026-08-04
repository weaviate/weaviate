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

// maxPlacementAttempts bounds the search for the two placements the test needs.
// Shards are placed off a shuffled candidate list, so each attempt is an
// independent draw and 16 of them make an unlucky run vanishingly rare.
const maxPlacementAttempts = 16

// probeDataset sizes the class the reindex is submitted for. It only has to
// exist on the probe node; the backup window is bought by the other class.
const probeDataset = 500

// guardTopology is the placement the remote-backup test is built on: a backup
// that lives entirely on one node, and a reindex target owned by another.
type guardTopology struct {
	backupClass  string
	reindexClass string
	probe        clusterNode
	placements   []string
}

// TestMultiNodeReindexRefusedWhileRemoteNodeBacksUp asserts that a node with no
// stake in a running backup still refuses a runtime-reindex, and names the
// remote node that holds the backup.
//
// This is the property the cluster-wide fan-out exists for. A node-local check
// could only ever name the node that answered the request, so the test drives
// the request at a node that provably holds no part of the backup and requires
// the 409 to name somebody else.
//
// It takes two classes to get there. The submission is validated against local
// shards before the guard runs, so the probe node has to own the class it is
// asked to reindex; a backup meanwhile enrolls every shard owner of the backed
// up class, the RAFT leader, and the node that received the create call.
// Collapsing all three of those roles onto the leader and parking a second
// class elsewhere is what leaves one node holding everything and another node
// holding nothing.
func TestMultiNodeReindexRefusedWhileRemoteNodeBacksUp(t *testing.T) {
	ctx := context.Background()

	const (
		propName  = "body"
		backupID  = "reindex-guard-remote-backup"
		bucket    = "reindex-guard-bucket"
		backend   = "s3"
		nodeCount = 3
		// A cluster of 2+ nodes refuses the filesystem backend outright, so the
		// backup has to go through MinIO.
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

	awaitClassVisible(t, coordinator.uri, topo.backupClass, coordinator.name)
	awaitClassVisible(t, topo.probe.uri, topo.reindexClass, topo.probe.name)

	// Every object of the backed-up class lands on the leader, so the corpus the
	// single-node siblings use buys the same multi-second window here, and the
	// upload to MinIO only widens it.
	importBodies(t, topo.backupClass, guardDataset)
	importBodies(t, topo.reindexClass, probeDataset)

	// A leadership change since the topology was resolved would enroll a node
	// the test did not account for, including possibly the probe.
	require.Equal(t, leader, raftLeaderName(t, nodes[0].uri, 60*time.Second),
		"the RAFT leader changed during setup; the resolved topology no longer holds")
	require.NoError(t, startS3Backup(coordinator.uri, topo.backupClass, backupID, bucket))

	statusOf := nodeBackupStatus(coordinator.uri, backend, backupID)
	run := probeReindexDuringBackup(t, topo.probe.uri, topo.reindexClass, propName, "whitespace",
		statusOf, 10*time.Minute)
	blocked := assertReindexBlocked(t, run, backupID)

	named := blockingNodeName(t, blocked.body)
	t.Logf("guard named node %q while the probe ran on node %q", named, topo.probe.name)
	require.NotEqualf(t, topo.probe.name, named,
		"the 409 named the probe node itself, which a node-local check would also produce; "+
			"the guard has to report the remote node holding the backup. body: %s", blocked.body)
	assert.Equalf(t, leader, named,
		"every slot of backup %q sits on leader %q, so no other node can legitimately be named; body: %s",
		backupID, leader, blocked.body)

	// The block has to lift on a node that never took part in the backup.
	awaitBackupSuccess(t, statusOf, backupID, 10*time.Minute)
	taskID := awaitReindexAccepted(t, topo.probe.uri, topo.reindexClass, propName, "whitespace", 60*time.Second)
	t.Logf("reindex accepted on probe node %q after backup %s finished: task %s",
		topo.probe.name, backupID, taskID)
}

// resolveGuardTopology creates single-shard, replication-factor-1 classes until
// one is owned by the RAFT leader and another by a different node. Placement
// cannot be requested, so create-and-inspect is the only way to obtain it.
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
			// One shard at replication factor 1 puts the entire class on a single
			// node, which is what both halves of the topology are built out of.
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

	// The samples that lost are dropped so the cluster holds only the two
	// classes the topology talks about.
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

// awaitClassVisible blocks until the node serves the class out of its own
// schema view, which is what the submission handler validates against.
func awaitClassVisible(t *testing.T, restURI, className, nodeName string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		_, ok := reindexhelpers.FetchClass(restURI, className, true)
		return ok
	}, 60*time.Second, 100*time.Millisecond,
		"class %s must be locally visible on %s before the test drives it", className, nodeName)
}
