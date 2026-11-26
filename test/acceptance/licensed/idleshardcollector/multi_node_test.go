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
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
)

const clusterSize = 3

var all = "ALL"

// cluster is a 3-node licensed cluster with a seeded multi-tenant class whose shards have all gone idle.
type cluster struct {
	compose *docker.DockerCompose
	cl      *apiclient.Weaviate
	names   []string
	class   string
	tenants []string
	ledger  *ledger
}

func startCluster(ctx context.Context, t *testing.T, b *docker.Compose, class string, rf int64, tenantCount, perShard int) *cluster {
	t.Helper()
	compose := start(ctx, t, b)
	c := &cluster{
		compose: compose,
		cl:      newClient(compose.GetWeaviateNode(1).URI()),
		class:   class,
		tenants: tenantNames(tenantCount),
		ledger:  newLedger(),
	}
	for i := 1; i <= clusterSize; i++ {
		c.names = append(c.names, compose.GetWeaviateNode(i).Name())
	}
	mt := vectorClass(class, true, rf)
	mt.ReplicationConfig.DeletionStrategy = models.ReplicationConfigDeletionStrategyDeleteOnConflict
	createClass(ctx, t, c.cl, mt)
	createTenants(ctx, t, c.cl, class, c.tenants)
	seed(ctx, t, c.cl, c.ledger, class, c.tenants, perShard, &all)
	eventuallyLoaded(ctx, t, compose, clusterSize, 0, "every node unloads its idle shards")
	return c
}

func TestMultiNodeBackupAndRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	b := newCompose(t, true, clusterSize, idleTTL).WithBackendS3("lazy-unload-backups", "us-east-1")
	c := startCluster(ctx, t, b, "Backup", 3, 20, 3)

	const backend, id = "s3", "lazy-unload-backup"
	_, err := c.cl.Backups.BackupsCreate(backups.NewBackupsCreateParams().WithContext(ctx).WithBackend(backend).
		WithBody(&models.BackupCreateRequest{ID: id, Include: []string{c.class}}), nil)
	require.NoError(t, err)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := c.cl.Backups.BackupsCreateStatus(backups.NewBackupsCreateStatusParams().WithContext(ctx).
			WithBackend(backend).WithID(id), nil)
		require.NoError(ct, err)
		require.NotNil(ct, resp.Payload.Status)
		require.Equal(ct, models.BackupCreateStatusResponseStatusSUCCESS, *resp.Payload.Status, resp.Payload.Error)
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
		require.Equal(ct, models.BackupRestoreStatusResponseStatusSUCCESS, *resp.Payload.Status, resp.Payload.Error)
	}, 5*time.Minute, time.Second, "restore succeeds")

	requireAllHot(ctx, t, c.cl, c.class, len(c.tenants))
	verify(ctx, t, c.cl, c.ledger, c.class, c.tenants, true)
	eventuallyLoaded(ctx, t, c.compose, clusterSize, 0, "the restored class unloads once idle")
}

func TestMultiNodeAsyncReplicationConvergesForActiveTenants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c := startCluster(ctx, t, newCompose(t, true, clusterSize, idleTTL), "AsyncRep", 3, 10, 3)
	lagging := c.names[2]

	require.NoError(t, c.compose.StopAt(ctx, 2, nil))
	one := "ONE"
	deleted := map[string]strfmt.UUID{}
	for _, tenant := range c.tenants[:3] {
		id := c.ledger.ids(c.class, tenant)[0]
		_, err := c.cl.Objects.ObjectsClassDelete(objects.NewObjectsClassDeleteParams().WithContext(ctx).
			WithClassName(c.class).WithID(id).WithTenant(&tenant).WithConsistencyLevel(&one), nil)
		require.NoError(t, err)
		c.ledger.remove(c.class, tenant, id)
		deleted[tenant] = id
	}
	eventuallyLoaded(ctx, t, c.compose, clusterSize-1, 0, "the surviving nodes unload their idle shards")
	require.NoError(t, c.compose.StartAt(ctx, 2))
	lc := newClient(c.compose.GetWeaviateNode(3).URI())

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for tenant, id := range deleted {
			for _, n := range c.names {
				require.NoError(ct, readObject(ctx, c.cl, c.class, tenant, c.ledger.ids(c.class, tenant)[0], &n))
			}
			var notFound *objects.ObjectsClassGetNotFound
			require.ErrorAs(ct, readObject(ctx, lc, c.class, tenant, id, &lagging), &notFound,
				"%s still holds the delete it missed in %s", lagging, tenant)
		}
	}, 5*time.Minute, 2*time.Second, "async replication repairs the replica that missed the deletes")
	verify(ctx, t, c.cl, c.ledger, c.class, c.tenants, true)
	eventuallyLoaded(ctx, t, c.compose, clusterSize, 0, "every node unloads its idle shards again")
}

func TestMultiNodeTenantStatusFlip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c := startCluster(ctx, t, newCompose(t, true, clusterSize, idleTTL), "Flip", 3, 12, 3)

	firstNode := c.names[0]
	loaded, unloaded := c.tenants[0], c.tenants[1]
	require.NoError(t, readObject(ctx, c.cl, c.class, loaded, c.ledger.ids(c.class, loaded)[0], &firstNode))
	first, err := loadedShards(ctx, c.cl, c.class, firstNode)
	require.NoError(t, err)
	require.True(t, first[loaded], "the tenant just read is loaded")
	require.False(t, first[unloaded], "an idle tenant is unloaded")

	require.NoError(t, setTenantStatus(ctx, c.cl, c.class, models.TenantActivityStatusCOLD, loaded, unloaded))
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, tenant := range []string{loaded, unloaded} {
			var cold *objects.ObjectsClassGetUnprocessableEntity
			require.ErrorAs(ct, readObject(ctx, c.cl, c.class, tenant, c.ledger.ids(c.class, tenant)[0], nil), &cold)
		}
	}, time.Minute, 500*time.Millisecond, "a COLD tenant refuses reads")

	require.NoError(t, setTenantStatus(ctx, c.cl, c.class, models.TenantActivityStatusHOT, loaded, unloaded))
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, tenant := range []string{loaded, unloaded} {
			id := c.ledger.ids(c.class, tenant)[0]
			require.NoError(ct, readObject(ctx, c.cl, c.class, tenant, id, nil))
		}
	}, time.Minute, 500*time.Millisecond, "the tenants serve again once HOT")
	requireAllHot(ctx, t, c.cl, c.class, len(c.tenants))
	verify(ctx, t, c.cl, c.ledger, c.class, c.tenants, true)
	eventuallyLoaded(ctx, t, c.compose, clusterSize, 0, "every node unloads its idle shards again")
}

func TestMultiNodeReplicaMovement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	b := newCompose(t, true, clusterSize, idleTTL).WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true")
	c := startCluster(ctx, t, b, "Movement", 2, 12, 3)
	setIdleTTL(ctx, t, c.compose, clusterSize, "1s")

	resp, err := c.cl.Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
		WithContext(ctx).WithCollection(&c.class), nil)
	require.NoError(t, err)
	type move struct{ shard, source, target, typ string }
	var moves []move
	for _, s := range resp.Payload.ShardingState.Shards {
		if len(moves) == 4 {
			break
		}
		require.Len(t, s.Replicas, 2)
		typ := api.MOVE.String()
		if len(moves) == 3 {
			typ = api.COPY.String()
		}
		for _, n := range c.names {
			if !slices.Contains(s.Replicas, n) {
				moves = append(moves, move{shard: s.Shard, source: s.Replicas[0], target: n, typ: typ})
			}
		}
	}
	require.Len(t, moves, 4)

	opIDs := make([]strfmt.UUID, len(moves))
	for i, m := range moves {
		resp, err := c.cl.Replication.Replicate(replication.NewReplicateParams().WithContext(ctx).WithBody(
			&models.ReplicationReplicateReplicaRequest{
				Collection: &c.class,
				Shard:      &m.shard,
				SourceNode: &m.source,
				TargetNode: &m.target,
				Type:       &m.typ,
			}), nil)
		require.NoError(t, err, "start %s of %s %s->%s", m.typ, m.shard, m.source, m.target)
		opIDs[i] = *resp.Payload.ID
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for i, id := range opIDs {
			details, err := c.cl.Replication.ReplicationDetails(replication.NewReplicationDetailsParams().WithContext(ctx).WithID(id), nil)
			require.NoError(ct, err)
			require.NotNil(ct, details.Payload.Status)
			require.Equal(ct, "READY", details.Payload.Status.State, "%s of %s", moves[i].typ, moves[i].shard)
		}
	}, 5*time.Minute, 2*time.Second, "every op completes while idle shards unload")

	resp, err = c.cl.Replication.GetCollectionShardingState(replication.NewGetCollectionShardingStateParams().
		WithContext(ctx).WithCollection(&c.class), nil)
	require.NoError(t, err)
	replicas := map[string][]string{}
	for _, s := range resp.Payload.ShardingState.Shards {
		replicas[s.Shard] = s.Replicas
	}
	for _, m := range moves {
		require.Contains(t, replicas[m.shard], m.target)
		if m.typ == api.MOVE.String() {
			require.NotContains(t, replicas[m.shard], m.source)
		} else {
			require.Contains(t, replicas[m.shard], m.source)
		}
		target := m.target
		for _, id := range c.ledger.ids(c.class, m.shard) {
			require.NoError(t, readObject(ctx, c.cl, c.class, m.shard, id, &target), "%s replica on %s", m.typ, target)
		}
	}
	verify(ctx, t, c.cl, c.ledger, c.class, c.tenants, true)
	eventuallyLoaded(ctx, t, c.compose, clusterSize, 0, "every node unloads its idle shards again")
}

// reindexIdleTTL is short so the shards a reindex unit loads go idle while the task is still running.
const reindexIdleTTL = "1s"

func TestMultiNodeRuntimeReindexOverIdleUnloadedTenants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	b := newCompose(t, true, clusterSize, reindexIdleTTL).
		WithWeaviateEnv("RUNTIME_REINDEX_ENABLED", "true").
		WithWeaviateEnv("DISTRIBUTED_TASKS_SCHEDULER_TICK_INTERVAL_SECONDS", "1")
	const perShard = 100
	c := startCluster(ctx, t, b, "ReindexIdle", clusterSize, 300, perShard)
	uri := c.compose.GetWeaviateNode(1).URI()

	taskID := reindexhelpers.SubmitIndexUpsert(t, uri, c.class, "seq", "rangeFilters", `{}`)
	reindexhelpers.AwaitReindexFinished(t, uri, taskID, reindexhelpers.WithTimeout(5*time.Minute))
	require.Eventually(t, func() bool { return seqRangeable(ctx, c.cl, c.class) }, 30*time.Second, time.Second,
		"the schema shows seq rangeable")

	ranges := []struct {
		where string
		want  int
	}{
		{where: `{path:["seq"],operator:GreaterThanEqual,valueInt:0}`, want: perShard},
		{where: `{operator:And,operands:[{path:["seq"],operator:GreaterThanEqual,valueInt:10},{path:["seq"],operator:LessThan,valueInt:25}]}`, want: 15},
		{where: `{path:["seq"],operator:GreaterThan,valueInt:30}`, want: perShard - 31},
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, tenant := range c.tenants {
		g.Go(func() error {
			for _, r := range ranges {
				n, err := aggregateCount(gctx, c.cl, c.class, tenant, r.where)
				if err != nil {
					return err
				}
				if n != r.want {
					return fmt.Errorf("%s/%s: %s matches %d objects, want %d", c.class, tenant, r.where, n, r.want)
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
	eventuallyLoaded(ctx, t, c.compose, clusterSize, 0, "idle shards unload once the reindex finished")
}

func seqRangeable(ctx context.Context, cl *apiclient.Weaviate, class string) bool {
	resp, err := cl.Schema.SchemaObjectsGet(schema.NewSchemaObjectsGetParams().WithContext(ctx).WithClassName(class), nil)
	if err != nil {
		return false
	}
	for _, p := range resp.Payload.Properties {
		if p.Name == "seq" {
			return p.IndexRangeFilters != nil && *p.IndexRangeFilters
		}
	}
	return false
}

func aggregateCount(ctx context.Context, cl *apiclient.Weaviate, class, tenant, where string) (int, error) {
	q := fmt.Sprintf(`{Aggregate{%s(tenant:%q,where:%s){meta{count}}}}`, class, tenant, where)
	resp, err := cl.Graphql.GraphqlPost(graphql.NewGraphqlPostParams().WithContext(ctx).WithBody(&models.GraphQLQuery{Query: q}), nil)
	if err != nil {
		return 0, fmt.Errorf("aggregate %s/%s: %w", class, tenant, err)
	}
	if len(resp.Payload.Errors) > 0 {
		return 0, fmt.Errorf("aggregate %s/%s: %s", class, tenant, resp.Payload.Errors[0].Message)
	}
	raw, err := json.Marshal(resp.Payload.Data["Aggregate"])
	if err != nil {
		return 0, err
	}
	var out map[string][]struct {
		Meta struct {
			Count int `json:"count"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, fmt.Errorf("aggregate %s/%s: %w", class, tenant, err)
	}
	if len(out[class]) != 1 {
		return 0, fmt.Errorf("aggregate %s/%s: unexpected result %s", class, tenant, raw)
	}
	return out[class][0].Meta.Count, nil
}
