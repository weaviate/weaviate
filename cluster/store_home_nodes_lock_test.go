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
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestStoreExecute_HomeNodesUpdateOrdersAgainstClassFactor shrinks namespace
// acme to one home node and sends a class command at factor 3. Blocker's
// AddClass blocks every later apply, so the first of the two is appended but
// unapplied when the second arrives. The second must see the first's outcome.
func TestStoreExecute_HomeNodesUpdateOrdersAgainstClassFactor(t *testing.T) {
	const (
		createRefused = "replication factor 3 exceeds the 1 home node(s) of the namespace"
		shrinkRefused = "fewer than the replication factor of collection(s) acme:Books"
	)
	books := func(t *testing.T, typ command.ApplyRequest_Type, factor int64) *command.ApplyRequest {
		t.Helper()
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
		held       bool // acme:Books exists at factor 1 before the shrink
		classFirst bool // the class command is appended first, the shrink otherwise
		wantErr    string
	}{
		{name: "a create behind a shrink is refused", typ: command.ApplyRequest_TYPE_ADD_CLASS, wantErr: createRefused},
		{name: "a shrink behind a create is refused", typ: command.ApplyRequest_TYPE_ADD_CLASS, classFirst: true, wantErr: shrinkRefused},
		{name: "a factor raise behind a shrink is refused", typ: command.ApplyRequest_TYPE_UPDATE_CLASS, held: true, wantErr: createRefused},
		{name: "a restore behind a shrink is refused", typ: command.ApplyRequest_TYPE_RESTORE_CLASS, wantErr: createRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, m := newBarrierTestStore(t)
			st := srv.store
			require.NoError(t, m.cfg.NamespacesController.Create(
				command.Namespace{Name: "acme", HomeNodes: []string{"n1", "n2", "n3"}}, 1))

			entered, release := make(chan struct{}), make(chan struct{})
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
			m.parser.On("ParseClassUpdate", mock.Anything).Return(nil, nil)

			if tt.held {
				_, err := st.Execute(books(t, command.ApplyRequest_TYPE_ADD_CLASS, 1))
				require.NoError(t, err)
			}

			blocker := executeAsync(st, addClassCmd(t, "Blocker"))
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the Blocker apply never started")
			}

			first, second := setHomeNodes(t, "n1"), books(t, tt.typ, 3)
			if tt.classFirst {
				first, second = second, first
			}
			appended := st.raft.Load().LastIndex()
			firstDone := executeAsync(st, first)
			require.Eventually(t, func() bool { return st.raft.Load().LastIndex() > appended },
				10*time.Second, 10*time.Millisecond, "the first command was never appended")
			secondDone := executeAsync(st, second)
			// Time for the second command to be judged and appended, unless it
			// waits for the first to apply.
			time.Sleep(300 * time.Millisecond)
			close(release)

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
				require.ErrorContains(t, err, tt.wantErr)
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
