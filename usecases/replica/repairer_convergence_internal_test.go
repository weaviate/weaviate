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

package replica

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

type replicaObj struct {
	updateTime int64
	deleted    bool
}

// fakeReplicas is stateful so a second repair round observes what the first one
// did, which a per-call mock cannot express. Overwrite mirrors
// (*Index).OverwriteObjects in adapters/repos/db/replication.go; locked because
// repairBatchPart fans out one goroutine per replica.
type fakeReplicas struct {
	strategy string

	mu                sync.Mutex
	state             map[string]map[strfmt.UUID]replicaObj
	fetchedObjects    int
	overwriteAttempts int
	overwritesApplied int
}

func newFakeReplicas(strategy string, nodes []string) *fakeReplicas {
	c := &fakeReplicas{strategy: strategy, state: map[string]map[strfmt.UUID]replicaObj{}}
	for _, n := range nodes {
		c.state[n] = map[strfmt.UUID]replicaObj{}
	}
	return c
}

func (c *fakeReplicas) put(node string, id strfmt.UUID, updateTime int64, deleted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state[node][id] = replicaObj{updateTime: updateTime, deleted: deleted}
}

func (c *fakeReplicas) get(node string, id strfmt.UUID) replicaObj {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state[node][id]
}

func (c *fakeReplicas) counters() (fetched, attempted, applied int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetchedObjects, c.overwriteAttempts, c.overwritesApplied
}

func (c *fakeReplicas) replica(node string, id strfmt.UUID) Replica {
	x := c.get(node, id)
	if x.deleted || x.updateTime == 0 {
		return Replica{ID: id, Deleted: x.deleted, LastUpdateTimeUnixMilli: x.updateTime}
	}
	return Replica{ID: id, Object: &storobj.Object{
		MarshallerVersion: 1,
		Object:            models.Object{ID: id, LastUpdateTimeUnix: x.updateTime},
	}}
}

func (c *fakeReplicas) fetch(node string, ids []strfmt.UUID) []Replica {
	c.mu.Lock()
	c.fetchedObjects += len(ids)
	c.mu.Unlock()

	rs := make([]Replica, len(ids))
	for i, id := range ids {
		rs[i] = c.replica(node, id)
	}
	return rs
}

func (c *fakeReplicas) overwrite(node string, ups []*objects.VObject) []types.RepairResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	timeBased := c.strategy == models.ReplicationConfigDeletionStrategyTimeBasedResolution
	var out []types.RepairResponse
	for _, u := range ups {
		c.overwriteAttempts++

		id := u.ID
		if id == "" && u.LatestObject != nil {
			id = u.LatestObject.ID
		}
		cur := c.state[node][id]
		conflict := types.RepairResponse{
			ID: id.String(), Deleted: cur.deleted, UpdateTime: cur.updateTime, Err: "conflict",
		}

		if cur.updateTime != u.StaleUpdateTime {
			if cur.updateTime == u.LastUpdateTimeUnixMilli {
				continue // already at the target version
			}
			if !cur.deleted || !timeBased || cur.updateTime > u.LastUpdateTimeUnixMilli {
				out = append(out, conflict)
				continue
			}
		}
		if !u.Deleted && cur.deleted && (!timeBased || cur.updateTime > u.LastUpdateTimeUnixMilli) {
			out = append(out, conflict)
			continue
		}

		c.state[node][id] = replicaObj{updateTime: u.LastUpdateTimeUnixMilli, deleted: u.Deleted}
		c.overwritesApplied++
	}
	return out
}

// votes builds one round of replica replies. order[0] is the caller's own copy,
// the rest are digest reads in RPC completion order: coordinator.Pull appends
// them in exactly that order, so arrival order is an input to repair, not a
// constant.
func (c *fakeReplicas) votes(order []string, ids []strfmt.UUID) []Vote {
	vs := make([]Vote, 0, len(order))
	for i, node := range order {
		local := i == 0
		digest := make([]types.RepairResponse, len(ids))
		for j, id := range ids {
			x := c.get(node, id)
			d := types.RepairResponse{ID: id.String(), UpdateTime: x.updateTime, Deleted: x.deleted}
			if local {
				// ShardPart.Digests never reports Deleted: the caller's copy is a
				// search result, so it is always live.
				d.Deleted = false
			}
			digest[j] = d
		}
		vs = append(vs, Vote{
			BatchReply: BatchReply{Sender: node, IsLocal: local, DigestData: digest},
			Count:      make([]int, len(ids)),
		})
	}
	return vs
}

func (c *fakeReplicas) newRepairer(t *testing.T) *repairer {
	t.Helper()
	rc := NewMockRClient(t)
	rc.EXPECT().FetchObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, host, _, _ string, ids []strfmt.UUID) ([]Replica, error) {
			return c.fetch(host, ids), nil
		}).Maybe()
	rc.EXPECT().OverwriteObjects(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, host, _, _ string, ups []*objects.VObject) ([]types.RepairResponse, error) {
			return c.overwrite(host, ups), nil
		}).Maybe()

	metrics, err := NewMetrics(monitoring.GetMetrics())
	require.NoError(t, err)
	logger, _ := test.NewNullLogger()

	return &repairer{
		class:               "C1",
		getDeletionStrategy: func() string { return c.strategy },
		client:              NewFinderClient(rc, logger),
		metrics:             metrics,
		logger:              logger,
	}
}

// Pins weaviate/0-weaviate-issues#385: repeated repair over an unchanging
// three-tier divergence must converge, not re-fetch content every read.
func TestRepairBatchPartConverges(t *testing.T) {
	const (
		shard      = "S1"
		numObjects = 5
		numReads   = 10

		tContent   = int64(100)
		tTombstone = int64(200)
		tWinner    = int64(300)
	)

	ids := make([]strfmt.UUID, numObjects)
	for i := range ids {
		ids[i] = strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", i))
	}

	cases := []struct {
		name     string
		order    []string // [content, digest…] in RPC completion order
		strategy string
		// Replicas a single repair round has to write. Bounds the write axis the
		// way numObjects bounds the fetch axis.
		staleReplicas int
	}{
		{
			name:     "tombstone digest arrives before the winner",
			order:    []string{"A", "B", "C"},
			strategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			// Only the content replica: writing to the tombstone is refused.
			staleReplicas: 1,
		},
		{
			name:          "tombstone digest arrives after the winner",
			order:         []string{"A", "C", "B"},
			strategy:      models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			staleReplicas: 1,
		},
		{
			name:     "tombstone digest arrives after the winner, time based",
			order:    []string{"A", "C", "B"},
			strategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			// The tombstone loses to the newer write, so it is repairable too.
			staleReplicas: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			c := newFakeReplicas(tc.strategy, []string{"A", "B", "C"})
			for _, id := range ids {
				c.put("A", id, tContent, false)
				c.put("B", id, tTombstone, true)
				c.put("C", id, tWinner, false)
			}
			r := c.newRepairer(t)

			fetchesPerRead := make([]int, 0, numReads)
			for read := 0; read < numReads; read++ {
				// A read only sees objects the content replica still serves.
				live := make([]strfmt.UUID, 0, len(ids))
				for _, id := range ids {
					if x := c.get("A", id); !x.deleted && x.updateTime != 0 {
						live = append(live, id)
					}
				}
				if len(live) == 0 {
					break
				}
				before, _, _ := c.counters()
				_, err := r.repairBatchPart(ctx, shard, live, c.votes(tc.order, live), 0)
				require.NoError(t, err)
				after, _, _ := c.counters()
				fetchesPerRead = append(fetchesPerRead, after-before)
			}

			fetched, attempted, applied := c.counters()
			t.Logf("fetches per read: %v (total %d), overwrites attempted %d applied %d",
				fetchesPerRead, fetched, attempted, applied)

			// Anti-vacuity: without a fetch, every bound below holds trivially.
			require.NotEmpty(t, fetchesPerRead, "no read was performed")
			require.Positive(t, fetched, "fixture never reached the fetch path")

			assert.Positive(t, attempted,
				"repair transferred object content but never attempted a write")
			assert.LessOrEqual(t, attempted, tc.staleReplicas*numObjects,
				"unbounded write amplification: total overwrite attempts exceed one repair round")
			assert.Zero(t, fetchesPerRead[len(fetchesPerRead)-1],
				"repair has not converged: still re-fetching content on the last read")
			assert.LessOrEqual(t, fetched, numObjects,
				"unbounded read amplification: total object fetches exceed one repair round")
		})
	}
}

// Pins the data-loss half of weaviate/0-weaviate-issues#385: DeleteOnConflict
// must not let an older tombstone destroy a newer live winner with no copy left to heal from.
func TestRepairBatchPartDeleteOnConflictKeepsNewerLiveObject(t *testing.T) {
	const (
		shard = "S1"

		tContent   = int64(100)
		tTombstone = int64(200)
		tWinner    = int64(300)
	)

	id := strfmt.UUID("00000000-0000-0000-0000-0000000000ff")
	ids := []strfmt.UUID{id}

	for _, order := range [][]string{{"A", "B", "C"}, {"A", "C", "B"}} {
		t.Run(fmt.Sprintf("digest order %v", order), func(t *testing.T) {
			ctx := context.Background()
			c := newFakeReplicas(models.ReplicationConfigDeletionStrategyDeleteOnConflict,
				[]string{"A", "B", "C"})
			c.put("A", id, tContent, false)
			c.put("B", id, tTombstone, true)
			c.put("C", id, tWinner, false)

			r := c.newRepairer(t)
			_, err := r.repairBatchPart(ctx, shard, ids, c.votes(order, ids), 0)
			require.NoError(t, err)

			t.Logf("after repair: A=%+v B=%+v C=%+v", c.get("A", id), c.get("B", id), c.get("C", id))

			assert.Equal(t, replicaObj{updateTime: tWinner}, c.get("C", id),
				"the winning live version was destroyed by an older tombstone")
			assert.False(t, c.get("A", id).deleted,
				"an older tombstone was propagated to the content replica")
		})
	}
}

// Mirror of TestRepairBatchPartDeleteOnConflictKeepsNewerLiveObject: the
// newest tombstone must never be resurrected, for any strategy or digest
// arrival order. Millisecond resolution makes update-time ties ordinary,
// so this also covers the row where the tie favours the tombstone.
func TestRepairBatchPartNewestTombstoneIsNotResurrected(t *testing.T) {
	const (
		shard = "S1"

		tContent   = int64(100)
		tLoser     = int64(200)
		tTombstone = int64(300)
	)

	id := strfmt.UUID("00000000-0000-0000-0000-0000000000ab")
	ids := []strfmt.UUID{id}

	// A is the content replica and always holds a stale live version; B always
	// holds the winning tombstone. cLive is the live version C votes with.
	cases := []struct {
		name     string
		strategy string
		cLive    int64
		want     map[string]replicaObj
	}{
		{
			// Refusing to resolve leaves the replicas divergent by design, so the
			// live versions are expected to survive here. What must hold is that
			// the tombstone is not overwritten by one of them.
			name:     "tombstone newest, no automated resolution",
			strategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			cLive:    tLoser,
			want: map[string]replicaObj{
				"A": {updateTime: tContent},
				"B": {updateTime: tTombstone, deleted: true},
				"C": {updateTime: tLoser},
			},
		},
		{
			name:     "tombstone newest, delete on conflict",
			strategy: models.ReplicationConfigDeletionStrategyDeleteOnConflict,
			cLive:    tLoser,
			want: map[string]replicaObj{
				"A": {updateTime: tTombstone, deleted: true},
				"B": {updateTime: tTombstone, deleted: true},
				"C": {updateTime: tTombstone, deleted: true},
			},
		},
		{
			name:     "tombstone newest, time based",
			strategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			cLive:    tLoser,
			want: map[string]replicaObj{
				"A": {updateTime: tTombstone, deleted: true},
				"B": {updateTime: tTombstone, deleted: true},
				"C": {updateTime: tTombstone, deleted: true},
			},
		},
		{
			name:     "tombstone ties with the newest write, no automated resolution",
			strategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			cLive:    tTombstone,
			want: map[string]replicaObj{
				"A": {updateTime: tContent},
				"B": {updateTime: tTombstone, deleted: true},
				"C": {updateTime: tTombstone},
			},
		},
		{
			name:     "tombstone ties with the newest write, delete on conflict",
			strategy: models.ReplicationConfigDeletionStrategyDeleteOnConflict,
			cLive:    tTombstone,
			want: map[string]replicaObj{
				"A": {updateTime: tTombstone, deleted: true},
				"B": {updateTime: tTombstone, deleted: true},
				"C": {updateTime: tTombstone, deleted: true},
			},
		},
		{
			name:     "tombstone ties with the newest write, time based",
			strategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			cLive:    tTombstone,
			want: map[string]replicaObj{
				"A": {updateTime: tTombstone, deleted: true},
				"B": {updateTime: tTombstone, deleted: true},
				"C": {updateTime: tTombstone, deleted: true},
			},
		},
	}

	for _, tc := range cases {
		for _, order := range [][]string{{"A", "B", "C"}, {"A", "C", "B"}} {
			t.Run(fmt.Sprintf("%s/digest order %v", tc.name, order), func(t *testing.T) {
				ctx := context.Background()
				c := newFakeReplicas(tc.strategy, []string{"A", "B", "C"})
				c.put("A", id, tContent, false)
				c.put("B", id, tTombstone, true)
				c.put("C", id, tc.cLive, false)

				vs := c.votes(order, ids)

				// The tie rows are the only ones that reach the "live at the
				// winning tombstone's own time" half of the repair condition.
				// Replica times are set directly above rather than produced by a
				// write, so nothing about when an object write advances its
				// timestamp can quietly dissolve the tie; assert it anyway, because
				// a row that stops tying still passes on final state alone.
				if tc.cLive == tTombstone {
					var tombstone, liveAtTie bool
					for _, v := range vs {
						if v.UpdateTimeAt(0) != tTombstone {
							continue
						}
						if v.DeletedAt(0) {
							tombstone = true
						} else {
							liveAtTie = true
						}
					}
					require.True(t, tombstone && liveAtTie,
						"fixture no longer ties a live replica with the winning tombstone, so the condition under test is unreachable")
				}

				r := c.newRepairer(t)
				_, err := r.repairBatchPart(ctx, shard, ids, vs, 0)
				require.NoError(t, err)

				got := map[string]replicaObj{}
				for _, node := range []string{"A", "B", "C"} {
					got[node] = c.get(node, id)
				}
				t.Logf("after repair: %+v", got)

				assert.Equal(t, tc.want, got,
					"repair outcome depends on digest arrival order or resurrects a deleted object")
			})
		}
	}
}
