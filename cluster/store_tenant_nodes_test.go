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

package cluster

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// TestStoreExecute_NamespacedTenantTakesLeaderNodes pins that Store.Execute appends
// the node list the leader picks for an acme tenant add, whatever list it was sent.
func TestStoreExecute_NamespacedTenantTakesLeaderNodes(t *testing.T) {
	tests := []struct {
		name      string
		homeNodes []string // n1, n2 and n3 when nil
		selector  []string // the store's NodeSelector; the store itself is Node-1
		factor    int64
		tenants   []string
		sent      []string
		wantNodes []string // every tenant's nodes, in any order
		wantErr   string
	}{
		{
			name:     "the tenants go on the first home nodes that are storage candidates",
			selector: []string{"n2", "n3"}, factor: 2,
			tenants: []string{"t1", "t2"}, sent: []string{"n2", "n1"}, wantNodes: []string{"n2", "n3"},
		},
		{
			name:     "an add with no home node among the storage candidates is refused",
			selector: []string{"n4", "n5"}, factor: 2,
			tenants: []string{"t1"}, sent: []string{"n1", "n2"}, wantErr: `namespace "acme"`,
		},
		{
			name:      "a home node in the Raft configuration but not in memberlist is a candidate",
			homeNodes: []string{"Node-1"}, factor: 1, tenants: []string{"t1"}, wantNodes: []string{"Node-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, m := newBarrierTestStore(t, func(c *Config) { c.NodeSelector = mocks.NewMockNodeSelector(tt.selector...) })
			st := srv.store
			m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			homeNodes := tt.homeNodes
			if homeNodes == nil {
				homeNodes = []string{"n1", "n2", "n3"}
			}
			require.NoError(t, m.cfg.NamespacesController.Create(command.Namespace{Name: "acme", HomeNodes: homeNodes}, 1))
			_, err := st.Execute(acmeBooksCmd(t, command.ApplyRequest_TYPE_ADD_CLASS, tt.factor))
			require.NoError(t, err)

			_, err = st.Execute(acmeTenantsCmd(t, command.ApplyRequest_TYPE_ADD_TENANT,
				models.TenantActivityStatusHOT, tt.sent, tt.tenants...))
			state, readErr := readShardingState(st.SchemaReader(), "acme:Books")
			require.NoError(t, readErr)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, state.Physical)
				return
			}
			require.NoError(t, err)
			for _, name := range tt.tenants {
				assert.ElementsMatch(t, tt.wantNodes, state.Physical[name].BelongsToNodes, name)
			}
		})
	}
}

// TestStoreExecute_NamespacedTenantWaitsForItsClassCreate appends a create of
// acme:Books behind Blocker's AddClass, which holds the FSM, on a leader that has
// confirmed its term. A tenant add or update sent then must apply after the create.
func TestStoreExecute_NamespacedTenantWaitsForItsClassCreate(t *testing.T) {
	tests := []struct {
		name    string
		typ     command.ApplyRequest_Type
		create  bool // acme:Books is created behind Blocker, and t1 added for an update
		wantErr string
	}{
		{name: "a tenant add behind its class create applies", typ: command.ApplyRequest_TYPE_ADD_TENANT, create: true},
		{name: "a tenant update behind its class create applies", typ: command.ApplyRequest_TYPE_UPDATE_TENANT, create: true},
		{name: "a tenant add for a class never created is refused", typ: command.ApplyRequest_TYPE_ADD_TENANT, wantErr: "class not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, m := newBarrierTestStore(t, func(c *Config) { c.NodeSelector = mocks.NewMockNodeSelector("n1", "n2", "n3") })
			st := srv.store
			require.NoError(t, m.cfg.NamespacesController.Create(
				command.Namespace{Name: "acme", HomeNodes: []string{"n1", "n2", "n3"}}, 1))

			entered, release := make(chan struct{}), make(chan struct{})
			m.indexer.ExpectedCalls = nil
			m.indexer.On("Close", mock.Anything).Return(nil)
			m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			m.indexer.On("AddClass", mock.MatchedBy(func(r command.AddClassRequest) bool {
				return r.Class.Class == "Blocker"
			})).Run(func(mock.Arguments) {
				close(entered)
				<-release
			}).Return(nil)
			m.indexer.On("AddClass", mock.Anything).Return(nil)
			m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			m.replicationFSM.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()

			blocker := executeAsync(st, addClassCmd(t, "Blocker"))
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the Blocker apply never started")
			}

			var appended []*command.ApplyRequest
			if tt.create {
				appended = append(appended, acmeBooksCmd(t, command.ApplyRequest_TYPE_ADD_CLASS, 1))
			}
			if tt.typ == command.ApplyRequest_TYPE_UPDATE_TENANT {
				appended = append(appended, acmeTenantsCmd(t, command.ApplyRequest_TYPE_ADD_TENANT,
					models.TenantActivityStatusHOT, []string{"n1"}, "t1"))
			}
			var futures []raft.ApplyFuture
			for _, req := range appended {
				raw, err := proto.Marshal(req)
				require.NoError(t, err)
				futures = append(futures, st.raft.Load().Apply(raw, st.applyTimeout))
			}

			// The leader picks n1, so t1 on n3 means the sent list was appended unread.
			done := executeAsync(st, acmeTenantsCmd(t, tt.typ, models.TenantActivityStatusCOLD, []string{"n3"}, "t1"))
			// Time for the tenant command to be judged, unless it waits for the create to apply.
			time.Sleep(300 * time.Millisecond)
			close(release)

			require.NoError(t, <-blocker)
			for _, f := range futures {
				require.NoError(t, f.Error())
			}
			select {
			case err := <-done:
				if tt.wantErr != "" {
					require.ErrorContains(t, err, tt.wantErr)
					return
				}
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("the tenant command never returned after the release")
			}
			state, err := readShardingState(st.SchemaReader(), "acme:Books")
			require.NoError(t, err)
			assert.Equal(t, []string{"n1"}, state.Physical["t1"].BelongsToNodes)
			assert.Equal(t, models.TenantActivityStatusCOLD, state.Physical["t1"].Status)
		})
	}
}

// TestStoreExecute_NamespacedTenantPicksAfterCatchUp appends a shrink of acme to
// n2 and n3 behind an AddClass that holds the FSM. A tenant add sent to a leader
// that has not confirmed its term must then go on n2.
func TestStoreExecute_NamespacedTenantPicksAfterCatchUp(t *testing.T) {
	srv, m := newBarrierTestStore(t, func(c *Config) { c.NodeSelector = mocks.NewMockNodeSelector("n1", "n2", "n3") })
	st := srv.store

	m.indexer.ExpectedCalls = nil
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
	m.indexer.On("AddClass", mock.Anything).
		Run(func(mock.Arguments) {
			// Hold the FSM so the log runs ahead of the state machine.
			time.Sleep(300 * time.Millisecond)
		}).Return(nil)

	require.NoError(t, m.cfg.NamespacesController.Create(
		command.Namespace{Name: "acme", HomeNodes: []string{"n1", "n2", "n3"}}, 1))
	_, err := st.Execute(acmeBooksCmd(t, command.ApplyRequest_TYPE_ADD_CLASS, 1))
	require.NoError(t, err)

	// Append without waiting, so both entries are in flight.
	for _, req := range []*command.ApplyRequest{addClassCmd(t, "InFlight"), acmeHomeNodesCmd(t, "n2", "n3")} {
		raw, err := proto.Marshal(req)
		require.NoError(t, err)
		st.raft.Load().Apply(raw, st.applyTimeout)
	}
	// A fresh leader has stamped no term yet.
	st.fsmCaughtUpTerm.Store(0)

	_, err = st.Execute(acmeTenantsCmd(t, command.ApplyRequest_TYPE_ADD_TENANT,
		models.TenantActivityStatusHOT, []string{"n1"}, "t1"))
	require.NoError(t, err)
	state, err := readShardingState(st.SchemaReader(), "acme:Books")
	require.NoError(t, err)
	assert.Equal(t, []string{"n2"}, state.Physical["t1"].BelongsToNodes)
}
