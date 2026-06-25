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

package namespacecleanup

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// stubNamespaces returns a fixed deleting list.
type stubNamespaces struct{ deleting []string }

func (s stubNamespaces) ListDeleting() []string { return s.deleting }

// stubSchema returns the configured per-namespace residuals.
type stubSchema struct {
	classes    map[string][]string
	aliases    map[string][]string
	classesErr error
}

func (s stubSchema) ClassesInNamespace(ns string) ([]string, error) {
	if s.classesErr != nil {
		return nil, s.classesErr
	}
	return s.classes[ns], nil
}
func (s stubSchema) AliasesInNamespace(ns string) []string { return s.aliases[ns] }

// stubUsers returns the configured per-namespace user IDs.
type stubUsers struct{ users map[string][]string }

func (s stubUsers) UsersInNamespace(ns string) []string { return s.users[ns] }

// recordedCall captures a single RAFT call for ordering assertions.
type recordedCall struct {
	op   string
	arg  string
	from string // namespace context, when applicable
}

// stubRaft records calls and lets tests inject errors per op.
type stubRaft struct {
	calls []recordedCall

	deleteUsersErr   map[string]error
	deleteAliasErr   map[string]error
	deleteClassErr   map[string]error
	removeEntityErr  map[string]error
	removeEntityCall map[string]int

	// onCall fires at the start of every RAFT method with the op name; tests
	// use it to cancel the tick context mid-flight.
	onCall func(op string)
}

func (s *stubRaft) fireOnCall(op string) {
	if s.onCall != nil {
		s.onCall(op)
	}
}

func newStubRaft() *stubRaft {
	return &stubRaft{
		deleteUsersErr:   map[string]error{},
		deleteAliasErr:   map[string]error{},
		deleteClassErr:   map[string]error{},
		removeEntityErr:  map[string]error{},
		removeEntityCall: map[string]int{},
	}
}

func (s *stubRaft) DeleteUsersInNamespace(_ context.Context, name string) error {
	s.fireOnCall("users")
	s.calls = append(s.calls, recordedCall{op: "users", arg: name, from: name})
	return s.deleteUsersErr[name]
}

func (s *stubRaft) DeleteAlias(_ context.Context, alias string) (uint64, error) {
	s.fireOnCall("alias")
	s.calls = append(s.calls, recordedCall{op: "alias", arg: alias})
	return 0, s.deleteAliasErr[alias]
}

func (s *stubRaft) DeleteClass(_ context.Context, name string) (uint64, error) {
	s.fireOnCall("class")
	s.calls = append(s.calls, recordedCall{op: "class", arg: name})
	return 0, s.deleteClassErr[name]
}

func (s *stubRaft) RemoveNamespaceEntity(_ context.Context, name string) (uint64, error) {
	s.fireOnCall("entity")
	s.calls = append(s.calls, recordedCall{op: "entity", arg: name, from: name})
	s.removeEntityCall[name]++
	return 0, s.removeEntityErr[name]
}

func (s *stubRaft) DeleteRoles(names ...string) error {
	s.fireOnCall("delete_roles")
	for _, n := range names {
		s.calls = append(s.calls, recordedCall{op: "delete_roles", arg: n})
	}
	return nil
}

func (s *stubRaft) RevokeRolesForUser(user string, roles ...string) error {
	s.fireOnCall("revoke_roles")
	s.calls = append(s.calls, recordedCall{op: "revoke_roles", arg: user})
	return nil
}

// stubRBAC reports the RBAC rows bearing each namespace for the cascade.
type stubRBAC struct {
	// roles maps a stored role name to its presence; only the keys matter.
	roles []string
	// subjectsByAuth maps an auth method to the logical subjects that hold roles.
	subjectsByAuth map[authentication.AuthType][]string
	// rolesBySubject maps a logical subject to the role names it holds.
	rolesBySubject map[string][]string
}

func (s stubRBAC) NamespaceLocalRBAC(namespace string) ([]string, []rbac.NamespaceSubject, error) {
	var roles []string
	for _, r := range s.roles {
		if namespacing.NamespaceFromQualified(r) == namespace {
			roles = append(roles, r)
		}
	}
	var subjects []rbac.NamespaceSubject
	for authType, subs := range s.subjectsByAuth {
		for _, sub := range subs {
			if namespacing.NamespaceFromQualified(sub) == namespace {
				subjects = append(subjects, rbac.NamespaceSubject{ID: sub, AuthType: authType})
			}
		}
	}
	return roles, subjects, nil
}

func (s stubRBAC) GetRolesForUserOrGroup(user string, authMethod authentication.AuthType, isGroup bool) (map[string][]authorization.Policy, error) {
	out := map[string][]authorization.Policy{}
	for _, r := range s.rolesBySubject[user] {
		out[r] = nil
	}
	return out, nil
}

func newTestCoordinator(t *testing.T,
	nsLister namespaceLister,
	schema schemaLister,
	raft raftExecutor,
	isLeader func() bool,
) *Coordinator {
	t.Helper()
	return newTestCoordinatorRBAC(t, nsLister, schema, raft, nil, isLeader)
}

func newTestCoordinatorRBAC(t *testing.T,
	nsLister namespaceLister,
	schema schemaLister,
	raft raftExecutor,
	rbac RBACLister,
	isLeader func() bool,
) *Coordinator {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	// Default: one user per deleting namespace so the skip-on-empty branch
	// stays inert. Skip-path tests build the Coordinator directly.
	users := stubUsers{users: map[string][]string{}}
	if listing, ok := nsLister.(stubNamespaces); ok {
		for _, ns := range listing.deleting {
			users.users[ns] = []string{ns + ":default-user"}
		}
	}
	return NewCoordinator(nsLister, schema, users, raft, rbac, isLeader, logger)
}

func alwaysLeader() bool { return true }

func TestCoordinator_NewCoordinator_PanicsOnNilArgs(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nsLister := stubNamespaces{}
	schema := stubSchema{}
	users := stubUsers{}
	raft := newStubRaft()

	tests := []struct {
		name     string
		ns       namespaceLister
		schema   schemaLister
		users    userLister
		raft     raftExecutor
		isLeader func() bool
	}{
		{name: "nil namespace lister", ns: nil, schema: schema, users: users, raft: raft, isLeader: alwaysLeader},
		{name: "nil schema lister", ns: nsLister, schema: nil, users: users, raft: raft, isLeader: alwaysLeader},
		{name: "nil user lister", ns: nsLister, schema: schema, users: nil, raft: raft, isLeader: alwaysLeader},
		{name: "nil raft executor", ns: nsLister, schema: schema, users: users, raft: nil, isLeader: alwaysLeader},
		{name: "nil isLeader", ns: nsLister, schema: schema, users: users, raft: raft, isLeader: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() {
				NewCoordinator(tc.ns, tc.schema, tc.users, tc.raft, nil, tc.isLeader, logger)
			})
		})
	}
}

// TestCoordinator_Tick_CleansNamespaceRBAC pins the cascade: a deleting
// namespace's local roles and its direct principals' grouping rows (including
// assignments to global roles) are removed via RAFT, while global roles and
// out-of-namespace subjects are left untouched.
//
// Journey (c) — a global subject holding a local role — is not pinned here: it
// falls out of Raft.DeleteRoles' apply path (RemoveFilteredGroupingPolicy on the
// role object), which the stub can't model. The acceptance suite asserts it
// end-to-end against a real store.
func TestCoordinator_Tick_CleansNamespaceRBAC(t *testing.T) {
	raft := newStubRaft()
	rbac := stubRBAC{
		roles: []string{"customer1:editor", "global-auditor"},
		subjectsByAuth: map[authentication.AuthType][]string{
			authentication.AuthTypeDb:   {"customer1:alice", "bob"},
			authentication.AuthTypeOIDC: {"customer1:carol"},
		},
		rolesBySubject: map[string][]string{
			"customer1:alice": {"customer1:editor", "viewer"},
			"customer1:carol": {"admin"},
		},
	}
	ns := stubNamespaces{deleting: []string{"customer1"}}
	c := newTestCoordinatorRBAC(t, ns, stubSchema{}, raft, rbac, alwaysLeader)

	require.NoError(t, c.Tick(context.Background()))

	var revoked, deleted []string
	var entityIdx, lastRBACIdx int
	for i, call := range raft.calls {
		switch call.op {
		case "revoke_roles":
			revoked = append(revoked, call.arg)
			lastRBACIdx = i
		case "delete_roles":
			deleted = append(deleted, call.arg)
			lastRBACIdx = i
		case "entity":
			entityIdx = i
		}
	}

	// Namespaced subjects revoked (db + oidc); the global subject "bob" is not.
	assert.ElementsMatch(t, []string{"db:customer1:alice", "oidc:customer1:carol"}, revoked)
	// Only the local role is deleted; the global role survives.
	assert.Equal(t, []string{"customer1:editor"}, deleted)
	// Entity removal runs after every RBAC row is cleaned.
	assert.Greater(t, entityIdx, lastRBACIdx, "entity must be removed after RBAC cleanup")
}

// TestCoordinator_Tick_RBACDisabledSkipsCleanup pins the nil-RBAC guard: with no
// RBAC store, the cascade issues no role/grouping deletes and still removes the
// entity.
func TestCoordinator_Tick_RBACDisabledSkipsCleanup(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"customer1"}}
	c := newTestCoordinatorRBAC(t, ns, stubSchema{}, raft, nil, alwaysLeader)

	require.NoError(t, c.Tick(context.Background()))

	for _, call := range raft.calls {
		assert.NotContains(t, []string{"revoke_roles", "delete_roles"}, call.op)
	}
	assert.Equal(t, 1, raft.removeEntityCall["customer1"])
}

func TestCoordinator_Tick_EmptyDeletingSetIsNoop(t *testing.T) {
	raft := newStubRaft()
	c := newTestCoordinator(t, stubNamespaces{}, stubSchema{}, raft, alwaysLeader)
	c.Tick(context.Background())
	assert.Empty(t, raft.calls)
}

func TestCoordinator_Tick_NotLeaderReturnsBeforeAnyCall(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"alpha"}}
	c := newTestCoordinator(t, ns, stubSchema{}, raft, func() bool { return false })
	c.Tick(context.Background())
	assert.Empty(t, raft.calls)
}

// Safety-net redrain fires only when leftover users remain.
func TestCoordinator_Tick_RedrainOnlyWhenUsersRemain(t *testing.T) {
	cases := []struct {
		name             string
		users            map[string][]string
		wantUsersOpFired bool
	}{
		{
			name:             "empty user set skips safety-net redrain",
			users:            map[string][]string{},
			wantUsersOpFired: false,
		},
		{
			name:             "leftover users trigger safety-net redrain",
			users:            map[string][]string{"alpha": {"alpha:leftover"}},
			wantUsersOpFired: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raft := newStubRaft()
			ns := stubNamespaces{deleting: []string{"alpha"}}
			logger, _ := test.NewNullLogger()
			c := NewCoordinator(ns, stubSchema{}, stubUsers{users: tc.users}, raft, nil, alwaysLeader, logger)

			c.Tick(context.Background())

			fired := false
			for _, call := range raft.calls {
				if call.op == "users" {
					fired = true
					break
				}
			}
			assert.Equal(t, tc.wantUsersOpFired, fired)
		})
	}
}

func TestCoordinator_Tick_OrderingAcrossPhases(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"alpha"}}
	schema := stubSchema{
		classes: map[string][]string{"alpha": {"alpha:Foo", "alpha:Bar"}},
		aliases: map[string][]string{"alpha": {"alpha:A1", "alpha:A2"}},
	}
	c := newTestCoordinator(t, ns, schema, raft, alwaysLeader)
	c.Tick(context.Background())

	// Expected order: users first; then both aliases; then both classes; then RemoveNamespaceEntity.
	require.Len(t, raft.calls, 6)
	assert.Equal(t, recordedCall{op: "users", arg: "alpha", from: "alpha"}, raft.calls[0])
	assert.Equal(t, "alias", raft.calls[1].op)
	assert.Equal(t, "alias", raft.calls[2].op)
	assert.Equal(t, "class", raft.calls[3].op)
	assert.Equal(t, "class", raft.calls[4].op)
	assert.Equal(t, recordedCall{op: "entity", arg: "alpha", from: "alpha"}, raft.calls[5])
}

func TestCoordinator_Tick_RemoveEntityNotEmptyIsSwallowed(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "wrapped sentinel", err: fmt.Errorf("apply: %w", namespaces.ErrNamespaceNotEmpty)},
		{name: "string fallback", err: errors.New("apply: " + namespaces.ErrNamespaceNotEmpty.Error())},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raft := newStubRaft()
			raft.removeEntityErr["alpha"] = tc.err
			ns := stubNamespaces{deleting: []string{"alpha"}}
			c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)
			// Tick must not panic, must not return; the retry will happen
			// at the next tick.
			require.NoError(t, c.Tick(context.Background()))
			assert.Equal(t, 1, raft.removeEntityCall["alpha"])
		})
	}
}

// TestCoordinator_Tick_LeaderFlipBetweenPhasesAborts walks an isLeader
// sequence: every call returns true except the configured one. The
// coordinator must abort the tick once it sees false.
//
// isLeader call sequence per Tick over a single namespace with N aliases
// and M classes:
//
//	1: Tick top-of-loop guard
//	2: cleanupOne entry guard
//	3..2+N: before each DeleteAlias
//	3+N..2+N+M: before each DeleteClass
//	3+N+M: before RemoveNamespaceEntity
func TestCoordinator_Tick_LeaderFlipBetweenPhasesAborts(t *testing.T) {
	tests := []struct {
		name           string
		falseAtCallNum int // 1-based index of the isLeader call that returns false
		schema         stubSchema
		wantOps        []string // RAFT ops the coordinator must have issued before aborting
	}{
		{
			name:           "flip on Tick guard issues nothing",
			falseAtCallNum: 1,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{},
		},
		{
			name:           "flip on cleanupOne entry issues nothing",
			falseAtCallNum: 2,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{},
		},
		{
			name:           "flip before first alias stops after users",
			falseAtCallNum: 3,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1", "alpha:A2"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{"users"},
		},
		{
			name:           "flip before second alias stops after first alias",
			falseAtCallNum: 4,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1", "alpha:A2"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{"users", "alias"},
		},
		{
			name:           "flip before first class stops after aliases",
			falseAtCallNum: 4,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo", "alpha:Bar"}},
			},
			wantOps: []string{"users", "alias"},
		},
		{
			name:           "flip before remove entity stops after classes",
			falseAtCallNum: 5,
			schema: stubSchema{
				aliases: map[string][]string{"alpha": {"alpha:A1"}},
				classes: map[string][]string{"alpha": {"alpha:Foo"}},
			},
			wantOps: []string{"users", "alias", "class"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raft := newStubRaft()
			ns := stubNamespaces{deleting: []string{"alpha"}}
			calls := 0
			isLeader := func() bool {
				calls++
				return calls != tc.falseAtCallNum
			}
			c := newTestCoordinator(t, ns, tc.schema, raft, isLeader)
			require.NoError(t, c.Tick(context.Background()))

			assert.Equal(t, tc.wantOps, opsOf(raft.calls))
			assert.NotContains(t, opsOf(raft.calls), "entity",
				"RemoveNamespaceEntity must not run on a flipped tick")
		})
	}
}

// TestCoordinator_Tick_ErrorBlastRadius covers how the tick contains errors.
// A schema or per-namespace RAFT error aborts only the failing namespace and
// leaves its entity in place; ErrNotLeader aborts the whole tick so the new
// leader can pick up.
func TestCoordinator_Tick_ErrorBlastRadius(t *testing.T) {
	tests := []struct {
		name              string
		deleting          []string
		schema            stubSchema
		setupRaft         func(*stubRaft)
		wantEntityRemoved map[string]int // expected RemoveNamespaceEntity count per namespace
		wantUntouchedNS   []string       // namespaces that must not appear in any recorded call
	}{
		{
			name:              "schema error aborts only the failing namespace",
			deleting:          []string{"alpha"},
			schema:            stubSchema{classesErr: errors.New("read schema failed")},
			wantEntityRemoved: map[string]int{"alpha": 0},
		},
		{
			name:     "per-namespace raft error continues to next namespace",
			deleting: []string{"alpha", "beta"},
			schema:   stubSchema{},
			setupRaft: func(s *stubRaft) {
				s.deleteUsersErr["alpha"] = errors.New("boom")
			},
			wantEntityRemoved: map[string]int{"alpha": 0, "beta": 1},
		},
		{
			name:     "ErrNotLeader aborts the whole tick",
			deleting: []string{"alpha", "beta"},
			schema:   stubSchema{},
			setupRaft: func(s *stubRaft) {
				s.deleteUsersErr["alpha"] = types.ErrNotLeader
			},
			wantEntityRemoved: map[string]int{"alpha": 0, "beta": 0},
			wantUntouchedNS:   []string{"beta"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raft := newStubRaft()
			if tc.setupRaft != nil {
				tc.setupRaft(raft)
			}
			ns := stubNamespaces{deleting: tc.deleting}
			c := newTestCoordinator(t, ns, tc.schema, raft, alwaysLeader)
			require.NoError(t, c.Tick(context.Background()))

			for n, want := range tc.wantEntityRemoved {
				assert.Equal(t, want, raft.removeEntityCall[n],
					"namespace %q: unexpected RemoveNamespaceEntity count", n)
			}
			for _, n := range tc.wantUntouchedNS {
				for _, call := range raft.calls {
					assert.NotEqual(t, n, call.from,
						"namespace %q must not be touched after the tick aborted", n)
				}
			}
		})
	}
}

// TestCoordinator_Tick_RejectsConcurrentRun forces a Tick to overlap with
// itself by holding the ongoing flag set. The second call must short-
// circuit with an error and must not enter cleanup.
func TestCoordinator_Tick_RejectsConcurrentRun(t *testing.T) {
	raft := newStubRaft()
	ns := stubNamespaces{deleting: []string{"alpha"}}
	c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)

	c.ongoing.Store(true)
	defer c.ongoing.Store(false)

	err := c.Tick(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already ongoing")
	assert.Empty(t, raft.calls, "Tick must not enter cleanup while another run is ongoing")
}

// TestCoordinator_Tick_StopsOnContextCancellation pins that a cancelled tick
// context halts the tick: when already cancelled, between namespaces, and
// partway through one namespace's class deletions.
func TestCoordinator_Tick_StopsOnContextCancellation(t *testing.T) {
	t.Run("already-cancelled ctx does no work", func(t *testing.T) {
		raft := newStubRaft()
		ns := stubNamespaces{deleting: []string{"alpha", "beta"}}
		c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, c.Tick(ctx))
		assert.Empty(t, raft.calls, "no namespace may be touched once ctx is cancelled")
	})

	t.Run("cancellation between namespaces stops the loop", func(t *testing.T) {
		raft := newStubRaft()
		ns := stubNamespaces{deleting: []string{"alpha", "beta"}}
		ctx, cancel := context.WithCancel(context.Background())
		// Cancel on alpha's first call; the loop guard must then skip beta.
		raft.onCall = func(string) { cancel() }
		c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)
		require.NoError(t, c.Tick(ctx))

		require.NotEmpty(t, raft.calls)
		for _, call := range raft.calls {
			assert.NotEqual(t, "beta", call.arg, "beta must be untouched after cancellation")
			assert.NotEqual(t, "beta", call.from, "beta must be untouched after cancellation")
		}
		assert.Equal(t, 0, raft.removeEntityCall["beta"])
	})

	t.Run("cancellation mid-namespace stops before remaining classes", func(t *testing.T) {
		raft := newStubRaft()
		ns := stubNamespaces{deleting: []string{"alpha"}}
		schema := stubSchema{classes: map[string][]string{"alpha": {"alpha:Foo", "alpha:Bar", "alpha:Baz"}}}
		ctx, cancel := context.WithCancel(context.Background())
		var cancelled bool
		raft.onCall = func(op string) {
			if op == "class" && !cancelled {
				cancelled = true
				cancel()
			}
		}
		c := newTestCoordinator(t, ns, schema, raft, alwaysLeader)
		require.NoError(t, c.Tick(ctx))

		// users + the in-flight class only; the inner guard aborts before
		// the rest and before RemoveNamespaceEntity.
		assert.Equal(t, []string{"users", "class"}, opsOf(raft.calls))
		assert.Equal(t, 0, raft.removeEntityCall["alpha"],
			"entity removal must not run when the tick was cancelled mid-cleanup")
	})

	t.Run("cancellation before the user delete issues no writes", func(t *testing.T) {
		// Cancel on the cleanupSingleNamespace entry isLeader call (#2), just
		// before the user delete; the pre-write guard must abort cleanly.
		raft := newStubRaft()
		ns := stubNamespaces{deleting: []string{"alpha"}}
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		isLeader := func() bool {
			calls++
			if calls == 2 {
				cancel()
			}
			return true
		}
		c := newTestCoordinator(t, ns, stubSchema{}, raft, isLeader)
		require.NoError(t, c.Tick(ctx))
		assert.Empty(t, raft.calls, "no RAFT write may run once ctx is cancelled before the user delete")
	})

	t.Run("cancellation after the user delete stops before entity removal", func(t *testing.T) {
		// No aliases or classes, so the only guard after the user delete is
		// the one before RemoveNamespaceEntity.
		raft := newStubRaft()
		ns := stubNamespaces{deleting: []string{"alpha"}}
		ctx, cancel := context.WithCancel(context.Background())
		raft.onCall = func(op string) {
			if op == "users" {
				cancel()
			}
		}
		c := newTestCoordinator(t, ns, stubSchema{}, raft, alwaysLeader)
		require.NoError(t, c.Tick(ctx))

		assert.Equal(t, []string{"users"}, opsOf(raft.calls))
		assert.Equal(t, 0, raft.removeEntityCall["alpha"],
			"entity removal must not run when ctx is cancelled before it")
	})
}

func opsOf(calls []recordedCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.op)
	}
	return out
}
