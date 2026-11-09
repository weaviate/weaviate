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

package reindex_multinode

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	replicationclient "github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

const postOpsObjects = 25

type postMigrationOp int

const (
	opRestore postMigrationOp = iota
	opRestoreNodeMapping
	opMove
	opCopy
)

type postOpsMigration struct {
	prop            *models.Property
	indexType, body string
	applied         func(*models.Property) bool
	singleTokenHits int // BM25 hits for "alpha" alone; zero once tokenization is field
}

// TestMultiNode_PostMigrationOps moves a shard's data (backup/restore or
// replica movement) right after a reindex reaches FINISHED. Without a restart
// in between, the operation lands while finalizing the migration is still
// deferred to the shard's next load, a window earlier tests never exercised.
func TestMultiNode_PostMigrationOps(t *testing.T) {
	ctx := context.Background()
	// Filesystem backups are per-node and refused on a multi-node cluster.
	const backupBucket = "post-migration-ops"
	compose, cleanup := start3NodeReindexClusterWithS3Backup(ctx, t, backupBucket,
		"USE_INVERTED_SEARCHABLE", "false",
		"REPLICA_MOVEMENT_ENABLED", "true",
	)
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	trueVal, falseVal := true, false
	changeTokenization := postOpsMigration{
		prop:      &models.Property{Name: "text", DataType: []string{"text"}, Tokenization: "word"},
		indexType: "searchable", body: `{"tokenization":"field"}`,
		applied: func(p *models.Property) bool { return p.Tokenization == "field" },
	}
	enableFilterable := postOpsMigration{
		prop: &models.Property{
			Name: "text", DataType: []string{"text"}, Tokenization: "word",
			IndexFilterable: &falseVal, IndexSearchable: &trueVal,
		},
		indexType: "filterable", body: `{}`,
		applied:         func(p *models.Property) bool { return p.IndexFilterable != nil && *p.IndexFilterable },
		singleTokenHits: postOpsObjects,
	}

	// Each operation sees both migrations and each migration both restart
	// modes, without running the full cross product. A tokenization change
	// moved without a restart is TestMultiNode_ReplicaMoveBeforeFinalizeKeepsMigratedIndex.
	cases := []struct {
		name          string
		op            postMigrationOp
		restartSource bool
		migration     postOpsMigration
		tenant        string
	}{
		{"Restore/NoRestart/ChangeTokenization", opRestore, false, changeTokenization, ""},
		{"Restore/SourceRestart/EnableFilterable", opRestore, true, enableFilterable, ""},
		{"RestoreNodeMapping/NoRestart/EnableFilterable", opRestoreNodeMapping, false, enableFilterable, ""},
		{"RestoreNodeMapping/SourceRestart/ChangeTokenization", opRestoreNodeMapping, true, changeTokenization, ""},
		{"Move/NoRestart/EnableFilterable", opMove, false, enableFilterable, ""},
		{"Move/SourceRestart/ChangeTokenization", opMove, true, changeTokenization, ""},
		{"Copy/NoRestart/EnableFilterable", opCopy, false, enableFilterable, ""},
		{"Copy/SourceRestart/ChangeTokenization", opCopy, true, changeTokenization, ""},
		{"Move/NoRestart/ChangeTokenization/MultiTenant", opMove, false, changeTokenization, "tenant1"},
	}

	// Ports change on every restart, so URIs are always looked up fresh.
	anyURI := func() string { return compose.GetWeaviateNode(1).URI() }
	nodeIndex := func(t *testing.T, name string) int {
		n, err := strconv.Atoi(strings.TrimPrefix(name, "weaviate-"))
		require.NoError(t, err, name)
		return n
	}

	restartNode := func(t *testing.T, name string) {
		n := nodeIndex(t, name)
		require.NoError(t, compose.StopNode(ctx, n, nil))
		require.NoError(t, compose.StartNode(ctx, n))
		// Replication ops and restores refuse a node the others still see as down.
		require.Eventually(t, func() bool {
			for i := 1; i <= 3; i++ {
				var nodes models.NodesStatusResponse
				if !httpGetJSON(fmt.Sprintf("http://%s/v1/nodes", compose.GetWeaviateNode(i).URI()), &nodes) ||
					len(nodes.Nodes) != 3 {
					return false
				}
				for _, node := range nodes.Nodes {
					if node.Status == nil || *node.Status != models.NodeStatusStatusHEALTHY {
						return false
					}
				}
			}
			return true
		}, 60*time.Second, time.Second, "cluster must see %s healthy after restart", name)
	}

	replicas := func(t require.TestingT, className string) (string, []string) {
		var state models.ReplicationShardingStateResponse
		require.True(t, httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s",
			anyURI(), className), &state) && state.ShardingState != nil)
		require.Len(t, state.ShardingState.Shards, 1)
		return state.ShardingState.Shards[0].Shard, state.ShardingState.Shards[0].Replicas
	}

	replicate := func(t *testing.T, className, shard, source, target, opType string, want []string) {
		helper.SetupClient(anyURI())
		res, err := helper.Client(t).Replication.Replicate(replicationclient.NewReplicateParams().
			WithBody(&models.ReplicationReplicateReplicaRequest{
				Collection: &className, Shard: &shard, SourceNode: &source,
				TargetNode: &target, Type: &opType,
			}), nil)
		require.NoError(t, err)
		id := *res.GetPayload().ID
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			helper.SetupClient(anyURI())
			status, err := helper.Client(t).Replication.ReplicationDetails(
				replicationclient.NewReplicationDetailsParams().WithID(id), nil)
			require.NoError(ct, err)
			require.Equal(ct, "READY", status.Payload.Status.State)
		}, 180*time.Second, 250*time.Millisecond, "%s %s→%s", opType, source, target)
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			_, got := replicas(ct, className)
			assert.ElementsMatch(ct, want, got)
		}, 60*time.Second, 250*time.Millisecond)
	}

	backupAndRestore := func(t *testing.T, className, backupID string, nodeMapping map[string]string) {
		helper.SetupClient(anyURI())
		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, "s3", backupID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, backupID, "s3", nil, helper.WithDeadline(2*time.Minute))
		deleteCollection(t, anyURI(), className)
		_, err = helper.RestoreBackup(t, helper.DefaultRestoreConfig(), className, "s3", backupID, nodeMapping, false)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyRestored(t, backupID, "s3", nil, helper.WithDeadline(2*time.Minute))
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			className := fmt.Sprintf("PostMigrationOps%d", i)
			// Node 1 may restart mid-case, so its URI is resolved at cleanup time.
			defer func() { deleteCollection(t, anyURI(), className) }()

			texts := make([]string, postOpsObjects)
			for j := range texts {
				texts[j] = "alpha bravo"
			}
			if tc.tenant == "" {
				createCollection(t, compose, anyURI(), className, 1, 1, []*models.Property{tc.migration.prop})
				importObjects(t, anyURI(), className, texts)
			} else {
				createMTCollection(t, anyURI(), className, 1, []*models.Property{tc.migration.prop})
				createTenants(t, anyURI(), className, []string{tc.tenant})
				importObjectsTenant(t, anyURI(), className, tc.tenant, texts)
			}

			// "alpha" alone tells the new tokenization apart from the old;
			// the other queries must return every object whatever the index.
			tenantArg := ""
			if tc.tenant != "" {
				tenantArg = fmt.Sprintf("tenant:%q,", tc.tenant)
			}
			checks := []struct {
				args string
				want int
			}{
				{`bm25:{query:"alpha bravo",properties:["text"]},limit:1000`, postOpsObjects},
				{`bm25:{query:"alpha",properties:["text"]},limit:1000`, tc.migration.singleTokenHits},
				{`where:{path:["text"],operator:Equal,valueText:"alpha bravo"},limit:1000`, postOpsObjects},
				{`limit:1000`, postOpsObjects},
			}
			verify := func(stage string) {
				for n := 1; n <= 3; n++ {
					require.EventuallyWithTf(t, func(ct *assert.CollectT) {
						uri := compose.GetWeaviateNode(n).URI()
						for _, c := range checks {
							ids, err := executeGraphQL(uri, className,
								fmt.Sprintf(`{Get{%s(%s%s){_additional{id}}}}`, className, tenantArg, c.args))
							require.NoError(ct, err, c.args)
							assert.Len(ct, ids, c.want, c.args)
						}
					}, 30*time.Second, 250*time.Millisecond, "%s: queries via node %d", stage, n)
				}
			}

			taskID := reindexhelpers.SubmitIndexUpsert(t, anyURI(), className, "text",
				tc.migration.indexType, tc.migration.body)
			reindexhelpers.AwaitReindexFinished(t, anyURI(), taskID, reindexhelpers.WithTimeout(180*time.Second))
			require.Eventually(t, func() bool {
				for _, p := range getClassFromNode(t, anyURI(), className).Properties {
					if p.Name == "text" {
						return tc.migration.applied(p)
					}
				}
				return false
			}, 60*time.Second, 250*time.Millisecond, "schema must reflect the migration")
			verify("after FINISHED")

			shard, owners := replicas(t, className)
			require.Len(t, owners, 1, "fixture expects replication factor 1")
			source := owners[0]
			target := fmt.Sprintf("weaviate-%d", (nodeIndex(t, source)+1)%3)

			if tc.restartSource {
				restartNode(t, source)
				verify("after source restart")
			}

			destination := target
			backupID := strings.ToLower(className)
			switch tc.op {
			case opRestore:
				backupAndRestore(t, className, backupID, nil)
				destination = source
			case opRestoreNodeMapping:
				// A backup can list nodes that hold no shard of the class, and
				// mapping onto one of those collides, so every node is rotated.
				rotation := map[string]string{}
				for k := 0; k < 3; k++ {
					rotation[fmt.Sprintf("weaviate-%d", k)] = fmt.Sprintf("weaviate-%d", (k+1)%3)
				}
				backupAndRestore(t, className, backupID, rotation)
			case opMove:
				replicate(t, className, shard, source, target, "MOVE", []string{target})
			case opCopy:
				replicate(t, className, shard, source, target, "COPY", []string{source, target})
			}
			_, owners = replicas(t, className)
			require.Contains(t, owners, destination)
			verify("after operation")

			restartNode(t, destination)
			verify("after destination restart")
		})
	}
}
