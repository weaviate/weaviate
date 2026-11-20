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
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestStoreExecute_HomeNodesUpdateOrdersAgainstClassAndTenantCommands shrinks acme to n2
// and sends a class command at factor 3 or a tenant add or unfreeze on n1. Blocker's AddClass
// blocks every later apply, so the first of the two is appended but unapplied when the second
// arrives. The second must see the first's outcome, unless it is an implicit activation or a
// tenant update with no ClusterNodes.
func TestStoreExecute_HomeNodesUpdateOrdersAgainstClassAndTenantCommands(t *testing.T) {
	const (
		createRefused = "replication factor 3 exceeds the 1 home node(s) of the namespace"
		shrinkRefused = "fewer than the replication factor of collection(s) acme:Books"
		tenantRefused = "has not yet applied the namespace's current home nodes"
	)
	tenantT1 := func(t *testing.T, typ command.ApplyRequest_Type, status string) *command.ApplyRequest {
		t.Helper()
		tenants := []*command.Tenant{{Name: "t1", Status: status}}
		var sub proto.Message = &command.AddTenantsRequest{ClusterNodes: []string{"n1"}, Tenants: tenants}
		if typ == command.ApplyRequest_TYPE_UPDATE_TENANT {
			sub = &command.UpdateTenantsRequest{ClusterNodes: []string{"n1"}, Tenants: tenants}
		}
		raw, err := proto.Marshal(sub)
		require.NoError(t, err)
		return &command.ApplyRequest{Type: typ, Class: "acme:Books", SubCommand: raw}
	}
	books := func(t *testing.T, typ command.ApplyRequest_Type, factor int64) *command.ApplyRequest {
		t.Helper()
		if typ == command.ApplyRequest_TYPE_ADD_TENANT || typ == command.ApplyRequest_TYPE_UPDATE_TENANT {
			return tenantT1(t, typ, models.TenantActivityStatusHOT)
		}
		class := &models.Class{
			Class:              "acme:Books",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			ReplicationConfig:  &models.ReplicationConfig{Factor: factor},
		}
		var sub any = command.AddClassRequest{Class: class, State: &sharding.State{PartitioningEnabled: true}}
		if typ == command.ApplyRequest_TYPE_UPDATE_CLASS {
			sub = command.UpdateClassRequest{Class: class, Origin: command.ClassUpdateOriginUser}
		}
		raw, err := json.Marshal(sub)
		require.NoError(t, err)
		return &command.ApplyRequest{Type: typ, Class: class.Class, SubCommand: raw}
	}
	setHomeNodes := func(t *testing.T, homeNodes ...string) *command.ApplyRequest {
		t.Helper()
		raw, err := json.Marshal(command.UpdateNamespaceRequest{
			Namespace: command.Namespace{Name: "acme", HomeNodes: homeNodes},
			Version:   command.NamespaceLatestCommandPolicyVersion,
		})
		require.NoError(t, err)
		return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_NAMESPACE, SubCommand: raw}
	}

	tests := []struct {
		name       string
		typ        command.ApplyRequest_Type
		update     *command.UpdateTenantsRequest // sent for acme:Books in place of typ's command when set
		held       bool                          // acme:Books exists at factor 1 before the shrink
		heldT1     string                        // t1's status on n1 before the shrink; t1 does not exist when empty
		classFirst bool                          // the class command is appended first, the shrink otherwise
		unlocked   bool                          // the second command is appended before the first applies
		wantErr    string                        // the second command's refusal; admitted when empty
	}{
		{name: "a create behind a shrink is refused", typ: command.ApplyRequest_TYPE_ADD_CLASS, wantErr: createRefused},
		{name: "a shrink behind a create is refused", typ: command.ApplyRequest_TYPE_ADD_CLASS, classFirst: true, wantErr: shrinkRefused},
		{name: "a factor raise behind a shrink is refused", typ: command.ApplyRequest_TYPE_UPDATE_CLASS, held: true, wantErr: createRefused},
		{name: "a restore behind a shrink is refused", typ: command.ApplyRequest_TYPE_RESTORE_CLASS, wantErr: createRefused},
		{name: "a tenant add behind a shrink is refused", typ: command.ApplyRequest_TYPE_ADD_TENANT, held: true, wantErr: tenantRefused},
		{
			name: "an unfreeze behind a shrink is refused", typ: command.ApplyRequest_TYPE_UPDATE_TENANT,
			held: true, heldT1: models.TenantActivityStatusFROZEN, wantErr: tenantRefused,
		},
		{
			name: "an implicit activation does not wait for a shrink", held: true, heldT1: models.TenantActivityStatusCOLD,
			update: &command.UpdateTenantsRequest{
				ImplicitUpdateRequest: true,
				ClusterNodes:          []string{"n1", "n2", "n3"},
				Tenants:               []*command.Tenant{{Name: "t1", Status: models.TenantActivityStatusHOT}},
			},
			unlocked: true,
		},
		{
			name: "an update without nodes does not wait for a shrink", held: true, heldT1: models.TenantActivityStatusCOLD,
			update: &command.UpdateTenantsRequest{
				Tenants: []*command.Tenant{{Name: "t1", Status: models.TenantActivityStatusHOT}},
			},
			unlocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, m := newBarrierTestStore(t)
			st := srv.store
			require.NoError(t, m.cfg.NamespacesController.Create(
				command.Namespace{Name: "acme", HomeNodes: []string{"n1", "n2", "n3"}}, 1))

			entered, release := make(chan struct{}), make(chan struct{})
			releaseOnce := sync.OnceFunc(func() { close(release) })
			// A failed assertion skips the release below, and srv.Close waits on the blocked apply.
			t.Cleanup(releaseOnce)
			m.indexer.ExpectedCalls = nil
			m.indexer.On("Close", mock.Anything).Return(nil)
			m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			m.indexer.On("UpdateClass", mock.Anything).Return(nil)
			m.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
			m.indexer.On("AddClass", mock.MatchedBy(func(r command.AddClassRequest) bool {
				return r.Class.Class == "Blocker"
			})).Run(func(mock.Arguments) {
				close(entered)
				<-release
			}).Return(nil)
			m.indexer.On("AddClass", mock.Anything).Return(nil)
			m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			m.indexer.On("UpdateTenants", mock.Anything, mock.Anything, mock.Anything).Return(nil)
			m.parser.On("ParseClassUpdate", mock.Anything).Return(nil, nil)

			if tt.held {
				_, err := st.Execute(books(t, command.ApplyRequest_TYPE_ADD_CLASS, 1))
				require.NoError(t, err)
			}
			if tt.heldT1 != "" {
				_, err := st.Execute(tenantT1(t, command.ApplyRequest_TYPE_ADD_TENANT, tt.heldT1))
				require.NoError(t, err)
			}

			blocker := executeAsync(st, addClassCmd(t, "Blocker"))
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the Blocker apply never started")
			}

			first, second := setHomeNodes(t, "n2"), books(t, tt.typ, 3)
			if tt.update != nil {
				raw, err := proto.Marshal(tt.update)
				require.NoError(t, err)
				second = &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_TENANT, Class: "acme:Books", SubCommand: raw}
			}
			if tt.classFirst {
				first, second = second, first
			}
			appended := st.raft.Load().LastIndex()
			firstDone := executeAsync(st, first)
			require.Eventually(t, func() bool { return st.raft.Load().LastIndex() > appended },
				10*time.Second, 10*time.Millisecond, "the first command was never appended")
			appended = st.raft.Load().LastIndex()
			secondDone := executeAsync(st, second)
			if tt.unlocked {
				require.Eventually(t, func() bool { return st.raft.Load().LastIndex() > appended },
					10*time.Second, 10*time.Millisecond, "the second command waited for the first to apply")
			} else {
				// Time for the second command to be judged and appended, unless it
				// waits for the first to apply.
				time.Sleep(300 * time.Millisecond)
			}
			releaseOnce()

			for _, done := range []<-chan error{blocker, firstDone} {
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("a command never returned after the release")
				}
			}
			select {
			case err := <-secondDone:
				if tt.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tt.wantErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the second command never returned after the release")
			}

			// A refused command must have released acme's lock, shared or not.
			select {
			case err := <-executeAsync(st, setHomeNodes(t, "n1", "n2", "n3")):
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("a later home-node update waited on a refused command's lock")
			}
		})
	}
}

func executeAsync(st *Store, cmd *command.ApplyRequest) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := st.Execute(cmd)
		done <- err
	}()
	return done
}
