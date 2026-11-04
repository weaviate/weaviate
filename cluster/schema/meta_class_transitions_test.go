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

package schema

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	hot    = models.TenantActivityStatusHOT
	cold   = models.TenantActivityStatusCOLD
	frozen = models.TenantActivityStatusFROZEN

	freezing   = command.TenantProcessRequest_ACTION_FREEZING
	unfreezing = command.TenantProcessRequest_ACTION_UNFREEZING
	opDone     = command.TenantsProcess_OP_DONE
	opAbort    = command.TenantsProcess_OP_ABORT
)

// transitionCluster keeps one metaClass per node.
type transitionCluster struct {
	t     *testing.T
	nodes []string
	metas map[string]*metaClass
	index uint64
	// empty holds the nodes whose cloud copy holds no data.
	empty map[string]bool
}

func newTransitionCluster(t *testing.T, nodes []string, factor int64, owners []string, tenants map[string]sharding.Physical) *transitionCluster {
	t.Helper()
	c := &transitionCluster{t: t, nodes: nodes, metas: map[string]*metaClass{}, index: 100}
	for _, node := range nodes {
		ss := sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{}}
		for name, p := range tenants {
			p.Name = name
			p.BelongsToNodes = append([]string(nil), owners...)
			ss.Physical[name] = p
		}
		ss.SetLocalName(node)
		c.metas[node] = &metaClass{
			Class: models.Class{
				Class:              "C",
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				ReplicationConfig:  &models.ReplicationConfig{Factor: factor},
			},
			Sharding: ss,
		}
	}
	return c
}

func (c *transitionCluster) next() uint64 { c.index++; return c.index }

func (c *transitionCluster) setStatus(status string, tenants ...string) uint64 {
	c.t.Helper()
	req := &command.UpdateTenantsRequest{ClusterNodes: c.nodes}
	for _, name := range tenants {
		req.Tenants = append(req.Tenants, &command.Tenant{Name: name, Status: status})
	}
	v := c.next()
	for _, node := range c.nodes {
		_, err := c.metas[node].UpdateTenants(node, proto.Clone(req).(*command.UpdateTenantsRequest), nil, v)
		require.NoError(c.t, err, "node %s", node)
	}
	return v
}

func (c *transitionCluster) report(from string, action command.TenantProcessRequest_Action, op command.TenantsProcess_Op, attempt uint64, tenants ...string) map[string][]*command.TenantsProcess {
	c.t.Helper()
	req := &command.TenantProcessRequest{Node: from, Action: action}
	for _, name := range tenants {
		req.TenantsProcesses = append(req.TenantsProcesses, &command.TenantsProcess{
			Op: op, Attempt: attempt, HasData: !c.empty[from], Tenant: &command.Tenant{Name: name},
		})
	}
	v := c.next()
	acted := map[string][]*command.TenantsProcess{}
	for _, node := range c.nodes {
		applied := proto.Clone(req).(*command.TenantProcessRequest)
		_, err := c.metas[node].UpdateTenantsProcess(node, applied, v)
		require.NoError(c.t, err, "a report must never fail on node %s", node)
		for _, p := range applied.TenantsProcesses {
			if p != nil {
				acted[node] = append(acted[node], p)
			}
		}
	}
	return acted
}

func (c *transitionCluster) physical(node, tenant string) sharding.Physical {
	return c.metas[node].Sharding.Physical[tenant]
}

func (c *transitionCluster) status(tenant string) string {
	c.t.Helper()
	first := c.physical(c.nodes[0], tenant)
	for _, node := range c.nodes[1:] {
		got := c.physical(node, tenant)
		require.Equal(c.t, first.ActivityStatus(), got.ActivityStatus(), "status on %s", node)
		require.Equal(c.t, first.Status, got.Status, "stored status on %s", node)
		require.ElementsMatch(c.t, first.BelongsToNodes, got.BelongsToNodes, "owners on %s", node)
		require.Equal(c.t, first.CloudAttempt, got.CloudAttempt, "cloud attempt on %s", node)
	}
	return first.ActivityStatus()
}

func (c *transitionCluster) restart(node string) {
	c.t.Helper()
	m := c.metas[node]
	data, err := json.Marshal(map[string]*metaClass{"C": {
		Class: m.Class, ClassVersion: m.ClassVersion, Sharding: m.Sharding.DeepCopy(), ShardVersion: m.ShardVersion,
	}})
	require.NoError(c.t, err)
	var restored map[string]*metaClass
	require.NoError(c.t, json.Unmarshal(data, &restored))
	restored["C"].Sharding.SetLocalName(node)
	c.metas[node] = restored["C"]
}

func TestFreezeTransition(t *testing.T) {
	nodes := []string{"n1", "n2"}
	for _, start := range []string{hot, cold} {
		t.Run("from "+start, func(t *testing.T) {
			c := newTransitionCluster(t, nodes, 2, nodes, map[string]sharding.Physical{"t": {Status: start}})

			attempt := c.setStatus(frozen, "t")
			require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"))
			require.Equal(t, start, c.physical("n1", "t").Status, "the status stays where it started")

			c.report("n1", freezing, opDone, attempt, "t")
			require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"), "n2 has not finished")

			acted := c.report("n2", freezing, opDone, attempt, "t")
			require.Equal(t, frozen, c.status("t"))
			phys := c.physical("n1", "t")
			require.Nil(t, phys.Transition)
			require.Equal(t, attempt, phys.CloudAttempt)
			require.Equal(t, nodes, phys.CloudSources)
			require.Equal(t, frozen, acted["n1"][0].Tenant.Status, "the DB removes local data")
		})

		t.Run("abort from "+start, func(t *testing.T) {
			c := newTransitionCluster(t, nodes, 2, nodes, map[string]sharding.Physical{"t": {Status: start}})
			attempt := c.setStatus(frozen, "t")

			c.report("n1", freezing, opDone, attempt, "t")
			acted := c.report("n2", freezing, opAbort, attempt, "t")

			require.Equal(t, start, c.status("t"))
			require.Zero(t, c.physical("n1", "t").CloudAttempt)
			require.Equal(t, start, acted["n1"][0].Tenant.Status, "a replica that finished still gets the abort")
		})
	}
}

func TestUnfreezeTransition(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	start := func(t *testing.T) (*transitionCluster, uint64, []string) {
		t.Helper()
		c := newTransitionCluster(t, nodes, 2, []string{"n2", "n3"}, map[string]sharding.Physical{
			"t": {Status: frozen, CloudAttempt: 50, CloudSources: []string{"n2", "n3"}},
		})
		attempt := c.setStatus(hot, "t")
		transition := c.physical("n1", "t").Transition
		require.NotNil(t, transition)
		var participants []string
		for node := range transition.Participants {
			participants = append(participants, node)
		}
		return c, attempt, participants
	}

	t.Run("every node sees the same status from the start", func(t *testing.T) {
		c, _, _ := start(t)
		require.Equal(t, models.TenantActivityStatusUNFREEZING, c.status("t"))
		require.Equal(t, frozen, c.physical("n1", "t").Status)
		require.ElementsMatch(t, []string{"n2", "n3"}, c.physical("n1", "t").BelongsToNodes, "owners change only at completion")
	})

	t.Run("each participant is paired with an old node", func(t *testing.T) {
		c, _, _ := start(t)
		var sources []string
		for _, participant := range c.physical("n1", "t").Transition.Participants {
			sources = append(sources, participant.Source)
		}
		require.ElementsMatch(t, []string{"n2", "n3"}, sources)
	})

	t.Run("all done", func(t *testing.T) {
		c, attempt, participants := start(t)
		chosen := c.physical("n1", "t").Transition.Chosen
		for _, node := range participants {
			c.report(node, unfreezing, opDone, attempt, "t")
		}
		require.Equal(t, hot, c.status("t"))
		phys := c.physical("n1", "t")
		require.ElementsMatch(t, chosen, phys.BelongsToNodes)
		require.Zero(t, phys.CloudAttempt)
		require.Empty(t, phys.CloudSources)
	})

	t.Run("any abort leaves the tenant frozen where it was", func(t *testing.T) {
		c, attempt, participants := start(t)
		c.report(participants[0], unfreezing, opDone, attempt, "t")
		acted := c.report(participants[1], unfreezing, opAbort, attempt, "t")

		require.Equal(t, frozen, c.status("t"))
		phys := c.physical("n1", "t")
		require.ElementsMatch(t, []string{"n2", "n3"}, phys.BelongsToNodes)
		require.Equal(t, uint64(50), phys.CloudAttempt)
		require.Equal(t, []string{"n2", "n3"}, phys.CloudSources)
		require.ElementsMatch(t, participants, slices.Collect(maps.Keys(acted)), "a participant clears what it may have swapped in")
		for _, processes := range acted {
			require.Equal(t, frozen, processes[0].Tenant.Status)
		}
	})
}

func TestUnfreezeDownloadsOnlyCopiesWithData(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	cases := []struct {
		name        string
		empty       []string
		wantSources []string
	}{
		{name: "every copy holds data", wantSources: []string{"n1", "n2"}},
		{name: "a copy without data is never a source", empty: []string{"n2"}, wantSources: []string{"n1", "n1"}},
		{name: "no copy holds data", empty: []string{"n1", "n2"}, wantSources: []string{"", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newTransitionCluster(t, nodes, 2, []string{"n1", "n2"}, map[string]sharding.Physical{"t": {Status: hot}})
			c.empty = map[string]bool{}
			for _, node := range tc.empty {
				c.empty[node] = true
			}
			attempt := c.setStatus(frozen, "t")
			c.report("n1", freezing, opDone, attempt, "t")
			c.report("n2", freezing, opDone, attempt, "t")
			require.Equal(t, frozen, c.status("t"))

			c.setStatus(hot, "t")
			transition := c.physical("n1", "t").Transition
			require.Len(t, transition.Participants, len(transition.Chosen), "every chosen node takes part")
			var sources []string
			for _, participant := range transition.Participants {
				sources = append(sources, participant.Source)
			}
			require.ElementsMatch(t, tc.wantSources, sources)
		})
	}
}

func TestRoutingDuringTransition(t *testing.T) {
	cases := []struct {
		name       string
		transition *sharding.Transition
		want       []string
	}{
		{name: "no transition", want: []string{"n2", "n3"}},
		{
			name:       "a freeze routes to the nodes holding the shard",
			transition: &sharding.Transition{Kind: sharding.TransitionFreeze},
			want:       []string{"n2", "n3"},
		},
		{
			name:       "an unfreeze routes to the chosen nodes",
			transition: &sharding.Transition{Kind: sharding.TransitionUnfreeze, Chosen: []string{"n1", "n3"}},
			want:       []string{"n1", "n3"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &metaClass{Sharding: sharding.State{Physical: map[string]sharding.Physical{
				"t": {BelongsToNodes: []string{"n2", "n3"}, Transition: tc.transition},
			}}}

			replicas, _, err := m.ShardReplicas("t")
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, replicas)

			for range 20 {
				owner, _, err := m.ShardOwner("t")
				require.NoError(t, err)
				require.Contains(t, tc.want, owner)
			}
		})
	}
}

func TestStaleReportsAreIgnored(t *testing.T) {
	nodes := []string{"n1", "n2"}
	newCluster := func(t *testing.T) *transitionCluster {
		return newTransitionCluster(t, nodes, 2, nodes, map[string]sharding.Physical{"t": {Status: hot}})
	}

	t.Run("a report from an earlier attempt", func(t *testing.T) {
		c := newCluster(t)
		first := c.setStatus(frozen, "t")
		c.report("n2", freezing, opAbort, first, "t")
		second := c.setStatus(frozen, "t")

		c.report("n1", freezing, opDone, first, "t")
		c.report("n2", freezing, opDone, second, "t")

		require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"), "n1 has not finished this attempt")
	})

	t.Run("a report for a re-created tenant", func(t *testing.T) {
		c := newCluster(t)
		attempt := c.setStatus(frozen, "t")
		v := c.next()
		for _, node := range nodes {
			_, err := c.metas[node].DeleteTenants(&command.DeleteTenantsRequest{Tenants: []string{"t"}}, v)
			require.NoError(t, err)
			_, err = c.metas[node].AddTenants(node, &command.AddTenantsRequest{
				ClusterNodes: nodes,
				Tenants:      []*command.Tenant{{Name: "t", Status: hot}},
			}, 2, v)
			require.NoError(t, err)
		}

		for _, node := range nodes {
			c.report(node, freezing, opDone, attempt, "t")
		}

		require.Equal(t, hot, c.status("t"))
	})

	t.Run("a second report from a participant that is done", func(t *testing.T) {
		c := newCluster(t)
		attempt := c.setStatus(frozen, "t")
		c.report("n1", freezing, opDone, attempt, "t")

		c.report("n1", freezing, opAbort, attempt, "t")

		require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"))
	})

	t.Run("a report of the wrong kind", func(t *testing.T) {
		c := newCluster(t)
		attempt := c.setStatus(frozen, "t")
		c.report("n1", unfreezing, opAbort, attempt, "t")
		require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"))
	})

	t.Run("a report from a node that is not a participant", func(t *testing.T) {
		c := newTransitionCluster(t, []string{"n1", "n2", "n3"}, 1, []string{"n1"}, map[string]sharding.Physical{"t": {Status: hot}})
		attempt := c.setStatus(frozen, "t")
		c.report("n3", freezing, opAbort, attempt, "t")
		require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"))
	})

	t.Run("a deleted tenant does not fail the batch", func(t *testing.T) {
		c := newTransitionCluster(t, []string{"n1"}, 1, []string{"n1"}, map[string]sharding.Physical{
			"t1": {Status: hot}, "t2": {Status: hot},
		})
		attempt := c.setStatus(frozen, "t1", "t2")
		_, err := c.metas["n1"].DeleteTenants(&command.DeleteTenantsRequest{Tenants: []string{"t1"}}, c.next())
		require.NoError(t, err)

		c.report("n1", freezing, opDone, attempt, "t1", "t2")

		require.Equal(t, frozen, c.status("t2"))
	})
}

func TestRestartMidTransitionFinishes(t *testing.T) {
	nodes := []string{"n1", "n2"}
	c := newTransitionCluster(t, nodes, 2, nodes, map[string]sharding.Physical{"t": {Status: hot}})
	attempt := c.setStatus(frozen, "t")
	c.report("n1", freezing, opDone, attempt, "t")

	c.restart("n2")
	require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"))

	c.report("n2", freezing, opDone, attempt, "t")
	require.Equal(t, frozen, c.status("t"))
}

func TestStatusRequestsDuringTransition(t *testing.T) {
	nodes := []string{"n1"}
	apply := func(c *transitionCluster, status string) error {
		_, err := c.metas["n1"].UpdateTenants("n1", &command.UpdateTenantsRequest{
			ClusterNodes: nodes,
			Tenants:      []*command.Tenant{{Name: "t", Status: status}},
		}, nil, c.next())
		return err
	}

	t.Run("freezing", func(t *testing.T) {
		c := newTransitionCluster(t, nodes, 1, nodes, map[string]sharding.Physical{"t": {Status: hot}})
		attempt := c.setStatus(frozen, "t")
		require.ErrorIs(t, apply(c, hot), ErrTenantTransitionalState)
		require.NoError(t, apply(c, frozen), "the status in progress is a no-op")
		require.Equal(t, attempt, c.physical("n1", "t").Transition.Attempt, "a retry does not restart the attempt")
	})

	t.Run("unfreezing", func(t *testing.T) {
		c := newTransitionCluster(t, nodes, 1, nodes, map[string]sharding.Physical{"t": {Status: frozen, CloudAttempt: 5}})
		attempt := c.setStatus(hot, "t")
		require.ErrorIs(t, apply(c, cold), ErrTenantTransitionalState)
		require.NoError(t, apply(c, hot), "the status in progress is a no-op")
		require.Equal(t, attempt, c.physical("n1", "t").Transition.Attempt)
	})
}

func TestUnfreezeFailingMidBatchKeepsEarlierTenants(t *testing.T) {
	c := newTransitionCluster(t, []string{"n1"}, 2, []string{"n1"}, map[string]sharding.Physical{
		"a": {Status: hot}, "b": {Status: frozen, CloudAttempt: 5},
	})
	req := &command.UpdateTenantsRequest{ClusterNodes: c.nodes, Tenants: []*command.Tenant{
		{Name: "a", Status: cold}, {Name: "b", Status: hot},
	}}

	_, err := c.metas["n1"].UpdateTenants("n1", req, nil, c.next())

	var partial *PartialUpdateError
	require.ErrorAs(t, err, &partial, "the store must still apply the tenants that changed")
	require.Equal(t, cold, c.status("a"))
	require.Equal(t, frozen, c.status("b"))
	require.Len(t, req.Tenants, 1)
	require.Equal(t, "a", req.Tenants[0].Name)
}

func TestRefuseDuringTransition(t *testing.T) {
	manager := func(t *testing.T, tenant sharding.Physical, alive func(string) bool) *SchemaManager {
		t.Helper()
		s := NewSchema("n1", prometheus.NewPedanticRegistry())
		tenant.Name = "t"
		cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}, ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		require.NoError(t, s.addClass(cls, &sharding.State{
			PartitioningEnabled: true,
			Physical:            map[string]sharding.Physical{"t": tenant},
		}, 1))
		return &SchemaManager{schema: s, nodeAlive: alive}
	}
	running := &sharding.Transition{Kind: sharding.TransitionFreeze}
	marshal := func(t *testing.T, m proto.Message) []byte {
		t.Helper()
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		return b
	}
	jsonOf := func(t *testing.T, v any) []byte {
		t.Helper()
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return b
	}

	cases := []struct {
		name    string
		tenant  sharding.Physical
		alive   func(string) bool
		req     func(t *testing.T) *command.ApplyRequest
		refused bool
	}{
		{
			name: "tenant delete with a running transition", tenant: sharding.Physical{Status: hot, Transition: running}, refused: true,
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_DELETE_TENANT, Class: "C", SubCommand: marshal(t, &command.DeleteTenantsRequest{Tenants: []string{"t"}})}
			},
		},
		{
			name: "tenant delete with none", tenant: sharding.Physical{Status: hot},
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_DELETE_TENANT, Class: "C", SubCommand: marshal(t, &command.DeleteTenantsRequest{Tenants: []string{"t"}})}
			},
		},
		{
			name: "replica add", tenant: sharding.Physical{Status: hot, Transition: running}, refused: true,
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_ADD_REPLICA_TO_SHARD, SubCommand: jsonOf(t, command.AddReplicaToShard{Class: "C", Shard: "t"})}
			},
		},
		{
			name: "replica delete", tenant: sharding.Physical{Status: hot, Transition: running}, refused: true,
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_DELETE_REPLICA_FROM_SHARD, SubCommand: jsonOf(t, command.DeleteReplicaFromShard{Class: "C", Shard: "t"})}
			},
		},
		{
			name: "replica move", tenant: sharding.Physical{Status: hot, Transition: running}, refused: true,
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_REPLICATION_REPLICATE, SubCommand: jsonOf(t, command.ReplicationReplicateShardRequest{SourceCollection: "C", SourceShard: "t"})}
			},
		},
		{
			name: "freeze naming a down node", tenant: sharding.Physical{Status: hot, BelongsToNodes: []string{"n1", "n2"}}, refused: true,
			alive: func(node string) bool { return node == "n1" },
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_TENANT, Class: "C", SubCommand: marshal(t, &command.UpdateTenantsRequest{Tenants: []*command.Tenant{{Name: "t", Status: frozen}}})}
			},
		},
		{
			name: "unfreeze choosing a down node", tenant: sharding.Physical{Status: frozen, BelongsToNodes: []string{"n1"}}, refused: true,
			alive: func(node string) bool { return node == "n1" },
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_TENANT, Class: "C", SubCommand: marshal(t, &command.UpdateTenantsRequest{
					ClusterNodes: []string{"n2"}, Tenants: []*command.Tenant{{Name: "t", Status: hot}},
				})}
			},
		},
		{
			name: "unfreeze choosing live nodes", tenant: sharding.Physical{Status: frozen, BelongsToNodes: []string{"n2"}},
			alive: func(node string) bool { return node == "n1" },
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_TENANT, Class: "C", SubCommand: marshal(t, &command.UpdateTenantsRequest{
					ClusterNodes: []string{"n1"}, Tenants: []*command.Tenant{{Name: "t", Status: hot}},
				})}
			},
		},
		{
			name: "a freeze retry while the freeze runs ignores liveness", tenant: sharding.Physical{Status: hot, BelongsToNodes: []string{"n1", "n2"}, Transition: running},
			alive: func(node string) bool { return node == "n1" },
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_TENANT, Class: "C", SubCommand: marshal(t, &command.UpdateTenantsRequest{Tenants: []*command.Tenant{{Name: "t", Status: frozen}}})}
			},
		},
		{
			name: "a status change that is no freeze ignores liveness", tenant: sharding.Physical{Status: hot, BelongsToNodes: []string{"n2"}},
			alive: func(string) bool { return false },
			req: func(t *testing.T) *command.ApplyRequest {
				return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_TENANT, Class: "C", SubCommand: marshal(t, &command.UpdateTenantsRequest{Tenants: []*command.Tenant{{Name: "t", Status: cold}}})}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := manager(t, tc.tenant, tc.alive).refuseDuringTransition(tc.req(t))
			if tc.refused {
				assert.ErrorIs(t, err, ErrTenantTransitionalState)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestReplicaChangesRefusedOnApplyDuringTransition(t *testing.T) {
	changes := map[string]func(m *metaClass) error{
		"add":    func(m *metaClass) error { return m.AddReplicaToShard(200, "t", "n3") },
		"delete": func(m *metaClass) error { return m.DeleteReplicaFromShard(200, "t", "n2") },
	}
	for name, change := range changes {
		for _, transition := range []*sharding.Transition{nil, {Kind: sharding.TransitionFreeze}} {
			t.Run(fmt.Sprintf("%s with transition %v", name, transition != nil), func(t *testing.T) {
				c := newTransitionCluster(t, []string{"n1"}, 2, []string{"n1", "n2"}, map[string]sharding.Physical{
					"t": {Status: hot, Transition: transition},
				})
				err := change(c.metas["n1"])
				if transition != nil {
					require.ErrorIs(t, err, ErrTenantTransitionalState)
					require.ElementsMatch(t, []string{"n1", "n2"}, c.physical("n1", "t").BelongsToNodes)
					return
				}
				require.NoError(t, err)
			})
		}
	}
}

func TestClassDeleteKnowsAboutCloudCopies(t *testing.T) {
	cases := []struct {
		name string
		phys sharding.Physical
		want bool
	}{
		{name: "hot", phys: sharding.Physical{Status: hot}},
		{name: "frozen", phys: sharding.Physical{Status: frozen}, want: true},
		{name: "freezing", phys: sharding.Physical{Status: hot, Transition: &sharding.Transition{Kind: sharding.TransitionFreeze}}, want: true},
		{name: "unfreezing", phys: sharding.Physical{Status: frozen, Transition: &sharding.Transition{Kind: sharding.TransitionUnfreeze}}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSchema("n1", prometheus.NewPedanticRegistry())
			tc.phys.Name = "t"
			require.NoError(t, s.addClass(
				&models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
				&sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{"t": tc.phys}}, 1))

			assert.Equal(t, tc.want, s.hasFrozenTenant("C"))
		})
	}
}

func TestTenantsReportTheirTransition(t *testing.T) {
	s := NewSchema("n1", prometheus.NewPedanticRegistry())
	unfreeze := &sharding.Transition{
		Kind:   sharding.TransitionUnfreeze,
		Target: cold,
		Participants: map[string]sharding.TransitionParticipant{
			"n3": {Source: "n1", Progress: sharding.ProgressStarted},
			"n2": {Source: "n4", Progress: sharding.ProgressDone},
		},
	}
	require.NoError(t, s.addClass(
		&models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
		&sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{
			"t":   {Name: "t", Status: frozen, Transition: unfreeze},
			"hot": {Name: "hot", Status: hot},
		}}, 1))
	want := &models.TenantTransition{
		Kind:         "UNFREEZE",
		TargetStatus: cold,
		Participants: []*models.TenantTransitionParticipant{
			{Node: "n2", SourceNode: "n4", Progress: "DONE"},
			{Node: "n3", SourceNode: "n1", Progress: "STARTED"},
		},
	}

	for name, names := range map[string][]string{"all": nil, "named": {"t", "hot"}} {
		t.Run(name, func(t *testing.T) {
			tenants, err := s.getTenants("C", names)
			require.NoError(t, err)
			require.Len(t, tenants, 2)
			for _, tenant := range tenants {
				if tenant.Name == "hot" {
					assert.Nil(t, tenant.Transition)
					continue
				}
				assert.Equal(t, models.TenantActivityStatusUNFREEZING, tenant.ActivityStatus)
				assert.Equal(t, want, tenant.Transition)
			}
		})
	}
}
