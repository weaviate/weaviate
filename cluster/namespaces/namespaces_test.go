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

package namespaces

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// mExists reports whether the namespace is present in any state.
func mExists(m *Manager, name string) bool {
	_, ok := m.GetNamespace(name)
	return ok
}

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewManager(usecasesNamespaces.NewController(logger), stubLeftovers{}, nil, nil, nil, logger)
}

func newTestManagerWithLeftovers(t *testing.T, schema SchemaNamespaceLister, dynusers DynusersNamespaceLister, rbac RBACNamespaceLister, metrics NamespaceMetricsDeleter) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewManager(usecasesNamespaces.NewController(logger), schema, dynusers, rbac, metrics, logger)
}

// recordingDeleter records the namespaces whose metric series were dropped, in
// call order.
type recordingDeleter struct {
	deleted []string
}

func (r *recordingDeleter) DeleteNamespace(namespace string) {
	r.deleted = append(r.deleted, namespace)
}

// newTestManagerWithDeleter builds a Manager whose leftover lookups report
// nothing, with a recording deleter attached.
func newTestManagerWithDeleter(t *testing.T) (*Manager, *recordingDeleter) {
	t.Helper()
	deleter := &recordingDeleter{}
	return newTestManagerWithLeftovers(t, stubLeftovers{}, nil, nil, deleter), deleter
}

func addCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.AddNamespaceRequest{
		Namespace: cmd.Namespace{Name: name, HomeNodes: []string{"node-1"}},
		Version:   cmd.NamespaceLatestCommandPolicyVersion,
	})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload, Version: createIndex}
}

func updateCmd(t *testing.T, name, homeNode string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.UpdateNamespaceRequest{Namespace: cmd.Namespace{Name: name, HomeNodes: []string{homeNode}}})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

// createIndex and seedIndex are the RAFT indexes the seed helper records at
// create and at the flip; flipIndex is the one the test under it passes. All
// distinct so an assertion cannot confuse them.
const (
	createIndex uint64 = 1
	seedIndex   uint64 = 2
	flipIndex   uint64 = 42
)

// changeStateCmd fills both Version fields the way the real call path does:
// the sub-command carries the command policy version, the outer request
// carries the RAFT log index. expectedIndex of 0 applies unconditionally.
func changeStateCmd(t *testing.T, name string, target cmd.NamespaceState, raftIndex, expectedIndex uint64) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.ChangeNamespaceStateRequest{
		Name:                     name,
		TargetState:              target,
		Version:                  cmd.NamespaceLatestCommandPolicyVersion,
		ExpectedStateChangeIndex: expectedIndex,
	})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload, Version: raftIndex}
}

func removeEntityCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.RemoveNamespaceEntityRequest{Name: name})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

// seedNamespace creates name and transitions it to seedState. An empty
// seedState seeds nothing.
func seedNamespace(t *testing.T, m *Manager, name string, seedState cmd.NamespaceState) {
	t.Helper()
	if seedState == "" {
		return
	}
	require.NoError(t, m.Add(addCmd(t, name)))
	if seedState == cmd.NamespaceStateActive {
		return
	}
	if seedState == cmd.NamespaceStateResuming {
		// resuming is only reachable from suspended.
		require.NoError(t, m.ChangeState(changeStateCmd(t, name, cmd.NamespaceStateSuspended, seedIndex, 0)))
	}
	require.NoError(t, m.ChangeState(changeStateCmd(t, name, seedState, seedIndex, 0)))
}

func TestNewManager_RequiredArgsPanic(t *testing.T) {
	logger, _ := test.NewNullLogger()
	controller := usecasesNamespaces.NewController(logger)

	tests := []struct {
		name       string
		controller *usecasesNamespaces.Controller
		schema     SchemaNamespaceLister
	}{
		{name: "nil controller panics", controller: nil, schema: stubLeftovers{}},
		{name: "nil schema lister panics", controller: controller, schema: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() {
				NewManager(tc.controller, tc.schema, nil, nil, nil, logger)
			})
		})
	}
}

func TestManager_Add(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	assert.Equal(t, 1, m.Count())
}

func TestManager_ChangeState(t *testing.T) {
	tests := []struct {
		name          string
		seedState     cmd.NamespaceState // empty = no namespace exists
		target        cmd.NamespaceState
		expectedIndex uint64 // 0 = unconditional
		wantErr       error
	}{
		{name: "active -> deleting flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateDeleting},
		{name: "active -> active is idempotent", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateActive},
		{name: "deleting -> deleting is idempotent", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateDeleting},
		{name: "deleting -> active is forbidden", seedState: cmd.NamespaceStateDeleting, target: cmd.NamespaceStateActive, wantErr: usecasesNamespaces.ErrInvalidStateTransition},
		{name: "missing namespace returns ErrNotFound", target: cmd.NamespaceStateDeleting, wantErr: usecasesNamespaces.ErrNotFound},
		// The exhaustive from x to table lives on the controller; these rows
		// carry each state's wire spelling through the JSON sub-command.
		{name: "active -> suspended flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateSuspended},
		{name: "suspended -> resuming flips state", seedState: cmd.NamespaceStateSuspended, target: cmd.NamespaceStateResuming},
		{name: "resuming -> active flips state", seedState: cmd.NamespaceStateResuming, target: cmd.NamespaceStateActive},
		// The expected index must survive the sub-command round-trip; the
		// controller owns the exhaustive precondition table.
		{name: "matching expected index flips state", seedState: cmd.NamespaceStateActive, target: cmd.NamespaceStateSuspended, expectedIndex: createIndex},
		{name: "stale expected index is refused", seedState: cmd.NamespaceStateSuspended, target: cmd.NamespaceStateActive, expectedIndex: createIndex, wantErr: usecasesNamespaces.ErrStateChangedConcurrently},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			seedNamespace(t, m, "customer1", tc.seedState)

			err := m.ChangeState(changeStateCmd(t, "customer1", tc.target, flipIndex, tc.expectedIndex))
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)

			ns, ok := m.GetNamespace("customer1")
			require.True(t, ok)
			assert.Equal(t, tc.target, ns.State)
			if tc.target == tc.seedState {
				assert.Equal(t, seededIndex(tc.seedState), ns.StateChangeIndex,
					"same-state re-apply must leave the recorded index alone")
				return
			}
			assert.Equal(t, flipIndex, ns.StateChangeIndex,
				"the recorded index must come from the outer request, not the sub-command's policy version")
		})
	}
}

// seededIndex is the index seedNamespace leaves on a namespace: an active
// namespace still carries its create index, every other seed state is
// reached by a flip recorded at seedIndex.
func seededIndex(seedState cmd.NamespaceState) uint64 {
	if seedState == "" {
		return 0
	}
	if seedState == cmd.NamespaceStateActive {
		return createIndex
	}
	return seedIndex
}

func TestManager_RemoveEntity(t *testing.T) {
	tests := []struct {
		name      string
		seedState cmd.NamespaceState // empty = no namespace exists
		wantErr   error
	}{
		{name: "deleting namespace is removed", seedState: cmd.NamespaceStateDeleting},
		{name: "active namespace returns ErrInvalidState", seedState: cmd.NamespaceStateActive, wantErr: usecasesNamespaces.ErrInvalidState},
		{name: "missing namespace returns ErrNotFound", wantErr: usecasesNamespaces.ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, deleter := newTestManagerWithDeleter(t)
			seedNamespace(t, m, "customer1", tc.seedState)

			err := m.RemoveEntity(removeEntityCmd(t, "customer1"))
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				assert.Empty(t, deleter.deleted, "a refused removal must keep the metric series")
				return
			}
			require.NoError(t, err)
			assert.False(t, mExists(m, "customer1"))
			assert.Equal(t, []string{"customer1"}, deleter.deleted)
		})
	}

	t.Run("nil deleter is tolerated", func(t *testing.T) {
		m := newTestManager(t)
		seedNamespace(t, m, "customer1", cmd.NamespaceStateDeleting)

		require.NoError(t, m.RemoveEntity(removeEntityCmd(t, "customer1")))
		assert.False(t, mExists(m, "customer1"))
	})
}

// stubLeftovers is a leftovers reader stub.
type stubLeftovers struct {
	classes []string
	aliases []string
	users   []string
}

func (s stubLeftovers) ClassesInNamespace(string) ([]string, error) { return s.classes, nil }
func (s stubLeftovers) AliasesInNamespace(string) []string          { return s.aliases }
func (s stubLeftovers) UsersInNamespace(string) []string            { return s.users }

// stubRBACRows reports a fixed count of surviving RBAC rows (or an error).
type stubRBACRows struct {
	count int
	err   error
}

func (s stubRBACRows) CountNamespaceLocalRBAC(string) (int, error) { return s.count, s.err }

func TestManager_RemoveEntity_Leftovers(t *testing.T) {
	errCount := errors.New("rbac count failed")
	tests := []struct {
		name      string
		leftovers stubLeftovers
		rbac      RBACNamespaceLister
		wantErr   error
	}{
		{name: "no leftovers removes the entity", leftovers: stubLeftovers{}},
		{name: "leftover class blocks", leftovers: stubLeftovers{classes: []string{"customer1:Foo"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "leftover alias blocks", leftovers: stubLeftovers{aliases: []string{"customer1:Bar"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "leftover user blocks", leftovers: stubLeftovers{users: []string{"u1"}}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "leftover RBAC row blocks", rbac: stubRBACRows{count: 1}, wantErr: usecasesNamespaces.ErrNamespaceNotEmpty},
		{name: "RBAC count error blocks removal", rbac: stubRBACRows{err: errCount}, wantErr: errCount},
		{name: "no RBAC rows removes the entity", rbac: stubRBACRows{count: 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deleter := &recordingDeleter{}
			m := newTestManagerWithLeftovers(t, tc.leftovers, tc.leftovers, tc.rbac, deleter)
			require.NoError(t, m.Add(addCmd(t, "customer1")))
			require.NoError(t, m.ChangeState(changeStateCmd(t, "customer1", cmd.NamespaceStateDeleting, seedIndex, 0)))

			err := m.RemoveEntity(removeEntityCmd(t, "customer1"))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.True(t, mExists(m, "customer1"), "namespace must remain when leftovers block removal")
				assert.Empty(t, deleter.deleted, "a refused removal must keep the metric series")
				return
			}
			require.NoError(t, err)
			assert.False(t, mExists(m, "customer1"))
			assert.Equal(t, []string{"customer1"}, deleter.deleted)
		})
	}
}

func TestManager_RejectsMalformedApplyRequest(t *testing.T) {
	bad := &cmd.ApplyRequest{SubCommand: []byte("not-json")}
	tests := []struct {
		name string
		call func(*Manager) error
	}{
		{name: "Add", call: func(m *Manager) error { return m.Add(bad) }},
		{name: "Update", call: func(m *Manager) error { return m.Update(bad) }},
		{name: "ChangeState", call: func(m *Manager) error { return m.ChangeState(bad) }},
		{name: "RemoveEntity", call: func(m *Manager) error { return m.RemoveEntity(bad) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			err := tc.call(m)
			require.Error(t, err)
			assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
		})
	}
}

func TestManager_Update(t *testing.T) {
	tests := []struct {
		name       string
		updateName string
		homeNode   string
		wantErr    error
		// wantHomeNode is checked on success to confirm the controller
		// actually stored the new HomeNode (i.e. dispatch worked).
		wantHomeNode string
	}{
		{name: "happy path dispatches to controller", updateName: "customer1", homeNode: "node-2", wantHomeNode: "node-2"},
		{name: "update missing returns ErrNotFound", updateName: "never-existed", homeNode: "node-2", wantErr: usecasesNamespaces.ErrNotFound},
		{name: "update empty home_node returns ErrBadRequest", updateName: "customer1", homeNode: "", wantErr: usecasesNamespaces.ErrBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newTestManager(t)
			require.NoError(t, m.Add(addCmd(t, "customer1")))

			err := m.Update(updateCmd(t, tc.updateName, tc.homeNode))
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			got := m.controller.Get(tc.updateName)
			require.Len(t, got, 1)
			assert.Equal(t, tc.wantHomeNode, got[0].Primary())
		})
	}
}

func TestManager_Get(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))

	t.Run("happy path dispatches to controller", func(t *testing.T) {
		payload, err := json.Marshal(cmd.QueryGetNamespacesRequest{})
		require.NoError(t, err)
		raw, err := m.Get(&cmd.QueryRequest{SubCommand: payload})
		require.NoError(t, err)

		var resp cmd.QueryGetNamespacesResponse
		require.NoError(t, json.Unmarshal(raw, &resp))
		require.Len(t, resp.Namespaces, 1)
		assert.Equal(t, "customer1", resp.Namespaces[0].Name)
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		_, err := m.Get(&cmd.QueryRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.ErrorIs(t, err, usecasesNamespaces.ErrBadRequest)
	})
}

// snapshotOf serializes the given namespaces in the shape Controller.Restore
// accepts: a single home node and an explicit state.
func snapshotOf(t *testing.T, names ...string) []byte {
	t.Helper()
	out := make(map[string]*cmd.Namespace, len(names))
	for _, name := range names {
		out[name] = &cmd.Namespace{
			Name:      name,
			HomeNodes: []string{"node-1"},
			State:     cmd.NamespaceStateActive,
		}
	}
	payload, err := json.Marshal(out)
	require.NoError(t, err)
	return payload
}

// A follower that lagged past log compaction installs a snapshot instead of
// applying the removal entries, so Restore is the only place it learns that a
// namespace is gone.
func TestManager_Restore(t *testing.T) {
	t.Run("restore deletes series of namespaces absent from the snapshot", func(t *testing.T) {
		m, deleter := newTestManagerWithDeleter(t)
		require.NoError(t, m.Add(addCmd(t, "customer1")))
		require.NoError(t, m.Add(addCmd(t, "customer2")))

		require.NoError(t, m.Restore(snapshotOf(t, "customer2")))

		assert.Equal(t, []string{"customer1"}, deleter.deleted)
		assert.False(t, mExists(m, "customer1"))
	})

	t.Run("restore keeps series of namespaces present in the snapshot", func(t *testing.T) {
		m, deleter := newTestManagerWithDeleter(t)
		require.NoError(t, m.Add(addCmd(t, "customer1")))

		require.NoError(t, m.Restore(snapshotOf(t, "customer1", "customer2")))

		assert.Empty(t, deleter.deleted)
		assert.True(t, mExists(m, "customer2"), "a namespace the snapshot adds is installed")
	})

	t.Run("an empty snapshot deletes every namespace's series", func(t *testing.T) {
		m, deleter := newTestManagerWithDeleter(t)
		require.NoError(t, m.Add(addCmd(t, "customer1")))

		require.NoError(t, m.Restore(nil))

		assert.Equal(t, []string{"customer1"}, deleter.deleted)
	})

	t.Run("failed restore calls no deleter", func(t *testing.T) {
		m, deleter := newTestManagerWithDeleter(t)
		require.NoError(t, m.Add(addCmd(t, "customer1")))

		require.Error(t, m.Restore([]byte("not-json")))

		assert.Empty(t, deleter.deleted)
		assert.True(t, mExists(m, "customer1"), "a rejected snapshot leaves state untouched")
	})

	t.Run("nil deleter is tolerated", func(t *testing.T) {
		m := newTestManager(t)
		require.NoError(t, m.Add(addCmd(t, "customer1")))

		require.NoError(t, m.Restore(snapshotOf(t, "customer2")))
		assert.False(t, mExists(m, "customer1"))
	})
}
