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

func TestReportsOtherThanDoneOrAbortAreIgnored(t *testing.T) {
	for _, op := range []command.TenantsProcess_Op{command.TenantsProcess_OP_UNSPECIFIED, command.TenantsProcess_OP_START} {
		t.Run(op.String(), func(t *testing.T) {
			c := newTransitionCluster(t, []string{"n1"}, 1, []string{"n1"}, map[string]sharding.Physical{"t": {Status: hot}})
			attempt := c.setStatus(frozen, "t")

			acted := c.report("n1", freezing, op, attempt, "t")

			require.Empty(t, acted)
			require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"), "the report must not count as done")
		})
	}
}

func TestFreezeTransition(t *testing.T) {
	nodes := []string{"n1", "n2"}
	for _, start := range []string{hot, cold} {
		t.Run("from "+start, func(t *testing.T) {
			c := newTransitionCluster(t, nodes, 2, nodes, map[string]sharding.Physical{"t": {Status: start}})

			attempt := c.setStatus(frozen, "t")
			require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"))
			require.Equal(t, start, c.physical("n1", "t").Status, "the status stays where it started")

			require.Empty(t, c.report("n1", freezing, opDone, attempt, "t"), "no DB acts before the end")
			require.Equal(t, models.TenantActivityStatusFREEZING, c.status("t"), "n2 has not finished")

			acted := c.report("n2", freezing, opDone, attempt, "t")
			require.ElementsMatch(t, nodes, slices.Collect(maps.Keys(acted)), "every owner removes its local data")
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

	t.Run("all done", func(t *testing.T) {
		c, attempt, participants := start(t)
		chosen := c.physical("n1", "t").Transition.Chosen
		var acted map[string][]*command.TenantsProcess
		for i, node := range participants {
			acted = c.report(node, unfreezing, opDone, attempt, "t")
			if i < len(participants)-1 {
				require.Empty(t, acted, "an intermediate DONE must not make a participant clear its swap")
			}
		}
		require.ElementsMatch(t, chosen, slices.Collect(maps.Keys(acted)), "the new owners load the tenant")
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
