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

package db

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/usecases/replica"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type ffrsStateGetter struct{ replicas []string }

func (f *ffrsStateGetter) ShardOwner(class, shard string) (string, error) {
	if len(f.replicas) == 0 {
		return "", fmt.Errorf("no owner")
	}
	return f.replicas[0], nil
}

func (f *ffrsStateGetter) ShardReplicas(class, shard string) ([]string, error) {
	return f.replicas, nil
}

type ffrsResolver struct{}

func (ffrsResolver) NodeHostname(name string) (string, bool) { return "h-" + name, true }

// ffrsClient only implements SearchShard; every other method of the embedded
// interface is nil and would panic if the code under test used it.
type ffrsClient struct {
	sharding.RemoteIndexClient

	mu      sync.Mutex
	asked   []string // hostnames the coordinator actually queried
	perNode map[string][]ffrsHit
	failing map[string]bool
}

type ffrsHit struct {
	id   strfmt.UUID
	dist float32
}

func (c *ffrsClient) SearchShard(ctx context.Context, hostname, indexName, shardName string,
	searchVector []models.Vector, targetVector []string, distance float32, limit int,
	f *filters.LocalFilter, keywordRanking *searchparams.KeywordRanking, srt []filters.Sort,
	cursor *filters.Cursor, groupBy *searchparams.GroupBy, adds additional.Properties,
	targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, []helpers.ShardQueryProfile, error) {
	c.mu.Lock()
	c.asked = append(c.asked, hostname)
	c.mu.Unlock()

	if c.failing[hostname] {
		return nil, nil, nil, fmt.Errorf("%s: 503 Node not ready", hostname)
	}

	hits := c.perNode[hostname]
	objs := make([]*storobj.Object, 0, len(hits))
	dists := make([]float32, 0, len(hits))
	for _, h := range hits {
		objs = append(objs, ffrsObject(h.id))
		dists = append(dists, h.dist)
	}
	return objs, dists, nil, nil
}

func (c *ffrsClient) askedHosts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]string(nil), c.asked...)
	sort.Strings(out)
	return out
}

func ffrsObject(id strfmt.UUID) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    id,
			Class: "C",
		},
	}
}

// ffrsShard is a ShardLike that only answers vector searches.
type ffrsShard struct {
	ShardLike

	name string
	hits []ffrsHit
}

func (s *ffrsShard) ID() string { return s.name }

func (s *ffrsShard) preventShutdown() (func(), error) { return func() {}, nil }

func (s *ffrsShard) ObjectVectorSearch(ctx context.Context, searchVectors []models.Vector,
	targetVectors []string, targetDist float32, limit int, f *filters.LocalFilter,
	srt []filters.Sort, groupBy *searchparams.GroupBy, adds additional.Properties,
	targetCombination *dto.TargetCombination, properties []string,
) ([]*storobj.Object, []float32, error) {
	objs := make([]*storobj.Object, 0, len(s.hits))
	dists := make([]float32, 0, len(s.hits))
	for _, h := range s.hits {
		objs = append(objs, ffrsObject(h.id))
		dists = append(dists, h.dist)
	}
	return objs, dists, nil
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type ffrsSetup struct {
	// nodes is the shard's replica set, as the sharding state reports it.
	nodes []string
	// localNode is this coordinator's name.
	localNode string
	// localShardHits, when non-nil, installs a loaded local shard.
	localShardHits []ffrsHit
	// perNode maps a replica node name to the hits it answers with.
	perNode map[string][]ffrsHit
	// failing marks replicas that answer 503.
	failing map[string]bool
}

const ffrsShardName = "S"

func newFFRSIndex(t *testing.T, s ffrsSetup) (*Index, *ffrsClient) {
	t.Helper()

	logger := logrus.New()
	logger.Out = io.Discard

	perHost := map[string][]ffrsHit{}
	for n, h := range s.perNode {
		perHost["h-"+n] = h
	}
	failHost := map[string]bool{}
	for n := range s.failing {
		failHost["h-"+n] = true
	}
	client := &ffrsClient{perNode: perHost, failing: failHost}

	remote := sharding.NewRemoteIndex("C", &ffrsStateGetter{replicas: s.nodes}, ffrsResolver{}, client)

	replicas := make([]routerTypes.Replica, 0, len(s.nodes))
	for _, n := range s.nodes {
		r := routerTypes.Replica{NodeName: n, ShardName: ffrsShardName, HostAddr: "h-" + n}
		if n == s.localNode {
			// the router sorts the local node first
			replicas = append([]routerTypes.Replica{r}, replicas...)
		} else {
			replicas = append(replicas, r)
		}
	}
	plan := routerTypes.ReadRoutingPlan{
		LocalHostname: s.localNode,
		Shard:         ffrsShardName,
		ReplicaSet:    routerTypes.ReadReplicaSet{Replicas: replicas},
	}

	router := routerTypes.NewMockRouter(t)
	router.EXPECT().BuildReadRoutingPlan(mock.Anything).Return(plan, nil).Maybe()

	schemaGetter := schemaUC.NewMockSchemaGetter(t)
	schemaGetter.EXPECT().NodeName().Return(s.localNode).Maybe()

	idx := &Index{
		Config: IndexConfig{
			ClassName:               schema.ClassName("C"),
			ReplicationFactor:       int64(len(s.nodes)),
			ForceFullReplicasSearch: true,
		},
		logger:           logger,
		remote:           remote,
		router:           router,
		getSchema:        schemaGetter,
		shardCreateLocks: esync.NewKeyRWLocker(),
		// With the default consistency level ONE, CheckConsistency returns before
		// touching any Finder field, so a zero-value Replicator is enough here.
		replicator: &replica.Replicator{},
	}

	if s.localShardHits != nil {
		idx.shards.Store(ffrsShardName, &ffrsShard{name: ffrsShardName, hits: s.localShardHits})
	}

	return idx, client
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestForceFullReplicasSearch_FanOut pins which replicas a coordinator with
// ForceFullReplicasSearch=true actually queries.
//
// The flag's contract (PR #5295) is "query all replicas of the shard and keep
// the best distance per object". A coordinator that happens to hold a replica
// of the shard must therefore still reach the other replicas.
func TestForceFullReplicasSearch_FanOut(t *testing.T) {
	tests := []struct {
		name           string
		setup          ffrsSetup
		wantAskedHosts []string
	}{
		{
			name: "coordinator does not hold the shard: all three replicas queried",
			setup: ffrsSetup{
				nodes:     []string{"N0", "N1", "N2"},
				localNode: "N9",
				perNode: map[string][]ffrsHit{
					"N0": {{"a", 0.1}},
					"N1": {{"b", 0.2}},
					"N2": {{"c", 0.3}},
				},
			},
			wantAskedHosts: []string{"h-N0", "h-N1", "h-N2"},
		},
		{
			name: "coordinator holds a replica: the other two must still be queried",
			setup: ffrsSetup{
				nodes:          []string{"N0", "N1", "N2"},
				localNode:      "N0",
				localShardHits: []ffrsHit{{"a", 0.9}},
				perNode: map[string][]ffrsHit{
					"N1": {{"a", 0.1}},
					"N2": {{"a", 0.2}},
				},
			},
			// RED on current code: nothing is asked, the local shard answers alone.
			wantAskedHosts: []string{"h-N1", "h-N2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, client := newFFRSIndex(t, tt.setup)

			_, _, err := idx.objectVectorSearch(context.Background(),
				[]models.Vector{[]float32{1, 2, 3}}, []string{""},
				0, 10, nil, nil, nil, additional.Properties{}, nil, "", nil, nil)
			require.NoError(t, err)

			require.Equal(t, tt.wantAskedHosts, client.askedHosts())
		})
	}
}

// TestForceFullReplicasSearch_SingleShardUnionIsSortedAndLimited pins the
// merge contract for the full-replica union.
//
// adapters/repos/db/index.go:2854 returns early for a single-shard read plan,
// which skips newDistancesSorter() and the `limit` truncation at
// index.go:2866-2870. In force mode the union spans every replica, so the
// caller gets replicas*limit objects in replica-completion order.
func TestForceFullReplicasSearch_SingleShardUnionIsSortedAndLimited(t *testing.T) {
	const limit = 2

	idx, _ := newFFRSIndex(t, ffrsSetup{
		nodes:     []string{"N0", "N1", "N2"},
		localNode: "N9",
		perNode: map[string][]ffrsHit{
			// Each replica returns its own top-2, already sorted locally.
			"N0": {{"a0000000-0000-0000-0000-000000000001", 0.10}, {"b0000000-0000-0000-0000-000000000002", 0.40}},
			"N1": {{"c0000000-0000-0000-0000-000000000003", 0.20}, {"d0000000-0000-0000-0000-000000000004", 0.30}},
			"N2": {{"e0000000-0000-0000-0000-000000000005", 0.05}, {"f0000000-0000-0000-0000-000000000006", 0.50}},
		},
	})

	objs, dists, err := idx.objectVectorSearch(context.Background(),
		[]models.Vector{[]float32{1, 2, 3}}, []string{""},
		0, limit, nil, nil, nil, additional.Properties{}, nil, "", nil, nil)
	require.NoError(t, err)
	require.Len(t, dists, len(objs))

	require.LessOrEqual(t, len(objs), limit,
		"a full-replica union must be truncated to the requested limit, got %d objects", len(objs))

	require.IsNonDecreasing(t, dists, "the union must be re-sorted by distance")

	require.Equal(t, strfmt.UUID("e0000000-0000-0000-0000-000000000005"), objs[0].ID(),
		"the globally closest object must rank first")
}

// TestForceFullReplicasSearch_PartialUnionIsSilent shows that a replica that
// answers 503 during a rolling restart simply drops out of the union with no
// error and no flag on the result.
func TestForceFullReplicasSearch_PartialUnionIsSilent(t *testing.T) {
	idx, client := newFFRSIndex(t, ffrsSetup{
		nodes:     []string{"N0", "N1", "N2"},
		localNode: "N9",
		perNode: map[string][]ffrsHit{
			"N0": {{"a0000000-0000-0000-0000-000000000001", 0.10}},
			"N2": {{"e0000000-0000-0000-0000-000000000005", 0.05}},
		},
		failing: map[string]bool{"N1": true},
	})

	objs, _, err := idx.objectVectorSearch(context.Background(),
		[]models.Vector{[]float32{1, 2, 3}}, []string{""},
		0, 10, nil, nil, nil, additional.Properties{}, nil, "", nil, nil)

	require.NoError(t, err, "a 503 from one replica is swallowed")
	require.Len(t, client.askedHosts(), 3)
	require.Len(t, objs, 2)

	// Nothing in the response says a third of the replica set was skipped.
}
