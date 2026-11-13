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

package idleshardcollector

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

const clusterSize = 3

var all = "ALL"

// cluster is a 3-node licensed cluster unloading idle shards, with a seeded multi-tenant class.
type cluster struct {
	compose *docker.DockerCompose
	cl      *apiclient.Weaviate
	class   string
	tenants []string
	ledger  *ledger
}

func startCluster(ctx context.Context, t *testing.T, class string, rf int64, tenantCount int, extra func(*docker.Compose) *docker.Compose) *cluster {
	t.Helper()
	b := newCompose(t, true, clusterSize, fastKnobs(idleKnobs))
	if extra != nil {
		b = extra(b)
	}
	compose := start(ctx, t, b)
	c := &cluster{
		compose: compose,
		cl:      newClient(compose.GetWeaviateNode(1).URI()),
		class:   class,
		tenants: tenantNames("t", tenantCount),
		ledger:  newLedger(),
	}
	waitUnloader(ctx, t, compose, clusterSize)
	mt := vectorClass(class, true, rf)
	mt.ReplicationConfig.DeletionStrategy = models.ReplicationConfigDeletionStrategyDeleteOnConflict
	createClass(ctx, t, c.cl, mt)
	createTenants(ctx, t, c.cl, class, c.tenants)
	seed(ctx, t, c.cl, c.ledger, class, c.tenants, 3, &all)
	c.waitIdle(ctx, t, "every node unloads its idle shards")
	return c
}

func (c *cluster) waitIdle(ctx context.Context, t *testing.T, msg string) {
	t.Helper()
	eventuallyLoaded(ctx, t, c.compose, clusterSize, 2*time.Minute, noneLoaded, msg)
}

func (c *cluster) nodeNames(ctx context.Context, t *testing.T) []string {
	t.Helper()
	resp, err := c.cl.Nodes.NodesGet(nodes.NewNodesGetParams().WithContext(ctx), nil)
	require.NoError(t, err)
	names := make([]string, 0, len(resp.Payload.Nodes))
	for _, n := range resp.Payload.Nodes {
		names = append(names, n.Name)
	}
	slices.Sort(names)
	require.Len(t, names, clusterSize)
	return names
}

func TestMultiNodeBackupAndRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	const bucket = "lazy-unload-backups"
	c := startCluster(ctx, t, "Backup", 3, 20, func(b *docker.Compose) *docker.Compose {
		return b.WithBackendS3(bucket, "us-east-1")
	})

	const backend, id = "s3", "lazy-unload-backup"
	_, err := c.cl.Backups.BackupsCreate(backups.NewBackupsCreateParams().WithContext(ctx).WithBackend(backend).
		WithBody(&models.BackupCreateRequest{ID: id, Include: []string{c.class}}), nil)
	require.NoError(t, err)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := c.cl.Backups.BackupsCreateStatus(backups.NewBackupsCreateStatusParams().WithContext(ctx).
			WithBackend(backend).WithID(id), nil)
		require.NoError(ct, err)
		require.NotNil(ct, resp.Payload.Status)
		require.NotEqual(ct, models.BackupCreateStatusResponseStatusFAILED, *resp.Payload.Status, resp.Payload.Error)
		require.Equal(ct, models.BackupCreateStatusResponseStatusSUCCESS, *resp.Payload.Status)
	}, 5*time.Minute, time.Second, "backup of a mostly unloaded class succeeds")

	_, err = c.cl.Schema.SchemaObjectsDelete(schema.NewSchemaObjectsDeleteParams().WithContext(ctx).WithClassName(c.class), nil)
	require.NoError(t, err)
	_, err = c.cl.Backups.BackupsRestore(backups.NewBackupsRestoreParams().WithContext(ctx).WithBackend(backend).WithID(id).
		WithBody(&models.BackupRestoreRequest{Include: []string{c.class}}), nil)
	require.NoError(t, err)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := c.cl.Backups.BackupsRestoreStatus(backups.NewBackupsRestoreStatusParams().WithContext(ctx).
			WithBackend(backend).WithID(id), nil)
		require.NoError(ct, err)
		require.NotNil(ct, resp.Payload.Status)
		require.NotEqual(ct, models.BackupRestoreStatusResponseStatusFAILED, *resp.Payload.Status, resp.Payload.Error)
		require.Equal(ct, models.BackupRestoreStatusResponseStatusSUCCESS, *resp.Payload.Status)
	}, 5*time.Minute, time.Second, "restore succeeds")

	statuses, err := tenantStatuses(ctx, c.cl, c.class)
	require.NoError(t, err)
	require.Len(t, statuses, len(c.tenants))
	for name, status := range statuses {
		require.Equal(t, models.TenantActivityStatusHOT, status, "tenant %s", name)
	}
	verifyAll(ctx, t, c.cl, c.ledger, c.class, c.tenants)
	c.waitIdle(ctx, t, "the restored class unloads once idle")
}

func TestMultiNodeAsyncReplicationConvergesForActiveTenants(t *testing.T) {
	missedDeletesConverge(t, true, 3, 5*time.Minute)
}

// missedDeletesConverge deletes one object in each of the first tenants while node 3 is
// down, then waits for async replication to remove them from node 3; keepPeersLoaded
// reads every replica each round.
func missedDeletesConverge(t *testing.T, keepPeersLoaded bool, tenants int, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c := startCluster(ctx, t, "AsyncRep", 3, 10, nil)
	lagging := c.compose.GetWeaviateNode(3).Name()
	names := c.nodeNames(ctx, t)

	require.NoError(t, c.compose.StopAt(ctx, 2, nil))
	one := "ONE"
	deleted := map[string]strfmt.UUID{}
	for _, tenant := range c.tenants[:tenants] {
		id := c.ledger.ids(c.class, tenant)[0]
		_, err := c.cl.Objects.ObjectsClassDelete(objects.NewObjectsClassDeleteParams().WithContext(ctx).
			WithClassName(c.class).WithID(id).WithTenant(&tenant).WithConsistencyLevel(&one), nil)
		require.NoError(t, err)
		c.ledger.remove(c.class, tenant, id)
		deleted[tenant] = id
	}
	eventuallyLoaded(ctx, t, c.compose, clusterSize-1, 2*time.Minute, noneLoaded,
		"the surviving nodes unload their idle shards")
	require.NoError(t, c.compose.StartAt(ctx, 2))
	lc := newClient(c.compose.GetWeaviateNode(3).URI())

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for tenant, id := range deleted {
			if keepPeersLoaded {
				for _, n := range names {
					require.NoError(ct, readObject(ctx, c.cl, c.class, tenant, c.ledger.ids(c.class, tenant)[0], &n))
				}
			}
			gone, err := objectGone(ctx, lc, c.class, tenant, id, lagging)
			require.NoError(ct, err)
			require.True(ct, gone, "%s still holds the delete it missed in %s", lagging, tenant)
		}
	}, timeout, 2*time.Second, "async replication repairs the replica that missed the deletes")
	verifyAll(ctx, t, c.cl, c.ledger, c.class, c.tenants)
	c.waitIdle(ctx, t, "every node unloads its idle shards again")
}

func TestMultiNodeTenantStatusFlip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c := startCluster(ctx, t, "Flip", 3, 12, nil)

	firstNode := c.compose.GetWeaviateNode(1).Name()
	loaded, unloaded := c.tenants[0], c.tenants[1]
	require.NoError(t, readObject(ctx, c.cl, c.class, loaded, c.ledger.ids(c.class, loaded)[0], &firstNode))
	byNode, err := nodeLoadedShards(ctx, c.cl, c.class)
	require.NoError(t, err)
	first := byNode[firstNode]
	require.True(t, first[loaded], "the tenant just read is loaded")
	require.False(t, first[unloaded], "an idle tenant is unloaded")

	for _, tenant := range []string{loaded, unloaded} {
		require.NoError(t, setTenantStatus(ctx, c.cl, c.class, tenant, models.TenantActivityStatusCOLD))
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		statuses, err := tenantStatuses(ctx, c.cl, c.class)
		require.NoError(ct, err)
		require.Equal(ct, models.TenantActivityStatusCOLD, statuses[loaded])
		require.Equal(ct, models.TenantActivityStatusCOLD, statuses[unloaded])
	}, time.Minute, 500*time.Millisecond)
	for _, tenant := range []string{loaded, unloaded} {
		id := c.ledger.ids(c.class, tenant)[0]
		err := readObject(ctx, c.cl, c.class, tenant, id, nil)
		var cold *objects.ObjectsClassGetUnprocessableEntity
		require.True(t, errors.As(err, &cold), "a COLD tenant refuses reads, got %v", err)
	}

	for _, tenant := range []string{loaded, unloaded} {
		require.NoError(t, setTenantStatus(ctx, c.cl, c.class, tenant, models.TenantActivityStatusHOT))
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, tenant := range []string{loaded, unloaded} {
			id := c.ledger.ids(c.class, tenant)[0]
			require.NoError(ct, readObject(ctx, c.cl, c.class, tenant, id, nil))
		}
	}, time.Minute, 500*time.Millisecond, "the tenants serve again once HOT")
	statuses, err := tenantStatuses(ctx, c.cl, c.class)
	require.NoError(t, err)
	for name, status := range statuses {
		require.Equal(t, models.TenantActivityStatusHOT, status, "tenant %s", name)
	}
	verifyAll(ctx, t, c.cl, c.ledger, c.class, c.tenants)
	c.waitIdle(ctx, t, "every node unloads its idle shards again")
}

func TestMultiNodeReplicaMovement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c := startCluster(ctx, t, "Movement", 2, 12, func(b *docker.Compose) *docker.Compose {
		return b.WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true")
	})
	names := c.nodeNames(ctx, t)

	resp, err := c.cl.Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
		WithContext(ctx).WithCollection(&c.class), nil)
	require.NoError(t, err)
	type move struct{ shard, source, target string }
	var moves []move
	for _, s := range resp.Payload.ShardingState.Shards {
		if len(moves) == 3 {
			break
		}
		require.Len(t, s.Replicas, 2)
		for _, n := range names {
			if !slices.Contains(s.Replicas, n) {
				moves = append(moves, move{shard: s.Shard, source: s.Replicas[0], target: n})
			}
		}
	}
	require.Len(t, moves, 3)

	moveType := api.MOVE.String()
	opIDs := make([]strfmt.UUID, len(moves))
	for i, m := range moves {
		resp, err := c.cl.Replication.Replicate(replication.NewReplicateParams().WithContext(ctx).WithBody(
			&models.ReplicationReplicateReplicaRequest{
				Collection: &c.class,
				Shard:      &m.shard,
				SourceNode: &m.source,
				TargetNode: &m.target,
				Type:       &moveType,
			}), nil)
		require.NoError(t, err, "start MOVE of %s %s->%s", m.shard, m.source, m.target)
		opIDs[i] = *resp.Payload.ID
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i, id := range opIDs {
			details, err := c.cl.Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithContext(ctx).WithID(id), nil)
			require.NoError(ct, err)
			require.NotNil(ct, details.Payload.Status)
			require.NotEqual(ct, "CANCELLED", details.Payload.Status.State, "MOVE of %s", moves[i].shard)
			require.Equal(ct, "READY", details.Payload.Status.State, "MOVE of %s", moves[i].shard)
		}
	}, 5*time.Minute, 2*time.Second, "every MOVE completes while idle shards unload")

	resp, err = c.cl.Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
		WithContext(ctx).WithCollection(&c.class), nil)
	require.NoError(t, err)
	replicas := map[string][]string{}
	for _, s := range resp.Payload.ShardingState.Shards {
		replicas[s.Shard] = s.Replicas
	}
	for _, m := range moves {
		require.Contains(t, replicas[m.shard], m.target)
		require.NotContains(t, replicas[m.shard], m.source)
		target := m.target
		for _, id := range c.ledger.ids(c.class, m.shard) {
			require.NoError(t, readObject(ctx, c.cl, c.class, m.shard, id, &target), "moved replica on %s", target)
		}
	}
	verifyAll(ctx, t, c.cl, c.ledger, c.class, c.tenants)
	c.waitIdle(ctx, t, "every node unloads its idle shards again")
}
