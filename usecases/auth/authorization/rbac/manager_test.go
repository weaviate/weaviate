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

package rbac

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestSnapshotAndRestore(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies func(*Manager) error
		wantErr       bool
	}{
		{
			name: "empty policies",
			setupPolicies: func(m *Manager) error {
				return nil
			},
		},
		{
			name: "with role and policy",
			setupPolicies: func(m *Manager) error {
				// Add a role and policy
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("admin"))
				return err
			},
		},
		{
			name: "multiple roles and policies",
			setupPolicies: func(m *Manager) error {
				// Add multiple roles and policies
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("editor"), "collections/*", authorization.UPDATE, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("admin"))
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("editor"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup logger with hook for testing
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			// Get initial policies before our test setup
			initialPolicies, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			initialGroupingPolicies, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)

			// Setup policies if needed
			if tt.setupPolicies != nil {
				err := tt.setupPolicies(m)
				require.NoError(t, err)
			}

			// Take snapshot
			snapshotData, err := m.Snapshot()
			require.NoError(t, err)
			require.NotNil(t, snapshotData)

			// Create a new manager for restore
			m2, err := setupTestManager(t, logger)
			require.NoError(t, err)

			// Restore from snapshot
			err = m2.Restore(snapshotData)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Get final policies after our test setup
			finalPolicies, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			finalGroupingPolicies, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)

			// Get restored policies
			restoredPolicies, err := m2.casbin.GetPolicy()
			require.NoError(t, err)
			restoredGroupingPolicies, err := m2.casbin.GetGroupingPolicy()
			require.NoError(t, err)

			// Compare only the delta of policies we added
			addedPolicies := getPolicyDelta(initialPolicies, finalPolicies)
			restoredAddedPolicies := getPolicyDelta(initialPolicies, restoredPolicies)
			assert.ElementsMatch(t, addedPolicies, restoredAddedPolicies)

			// Compare only the delta of grouping policies we added
			addedGroupingPolicies := getPolicyDelta(initialGroupingPolicies, finalGroupingPolicies)
			restoredAddedGroupingPolicies := getPolicyDelta(initialGroupingPolicies, restoredGroupingPolicies)
			assert.ElementsMatch(t, addedGroupingPolicies, restoredAddedGroupingPolicies)
		})
	}
}

// getPolicyDelta returns the policies that are in b but not in a
func getPolicyDelta(a, b [][]string) [][]string {
	delta := make([][]string, 0)
	for _, policyB := range b {
		found := false
		for _, policyA := range a {
			if equalPolicies(policyA, policyB) {
				found = true
				break
			}
		}
		if !found {
			delta = append(delta, policyB)
		}
	}
	return delta
}

// equalPolicies compares two policies for equality
func equalPolicies(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSnapshotNilCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := &Manager{
		casbin: nil,
		logger: logger,
	}

	snapshotData, err := m.Snapshot()
	require.NoError(t, err)
	assert.Nil(t, snapshotData)
}

func TestRestoreNilCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := &Manager{
		casbin: nil,
		logger: logger,
	}

	err := m.Restore([]byte("{}"))
	require.NoError(t, err)
}

func TestRestoreInvalidData(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	// Test with invalid JSON
	err = m.Restore([]byte("invalid json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode json")

	// Test with empty data
	err = m.Restore([]byte("{}"))
	require.NoError(t, err)
}

func TestRestoreEmptyData(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.READ, authorization.SchemaDomain)
	require.NoError(t, err)

	policies, err := m.casbin.GetPolicy()
	require.NoError(t, err)
	require.Len(t, policies, 5)

	err = m.Restore([]byte{})
	require.NoError(t, err)

	// nothing overwritten
	policies, err = m.casbin.GetPolicy()
	require.NoError(t, err)
	require.Len(t, policies, 5)
}

// TestRestoreInvalidatesEnforceCache verifies that Restore() properly
// invalidates the enforce cache so that concurrent Enforce() calls during
// Restore() do not re-populate the cache with stale results that persist
// after Restore() completes.
func TestRestoreInvalidatesEnforceCache(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	principal := &models.Principal{
		Username: "cache-user",
		UserType: models.UserTypeInput(authentication.AuthTypeDb),
	}
	user := conv.UserNameWithTypeFromId("cache-user", authentication.AuthTypeDb)
	role := conv.PrefixRoleName("cache-role")
	resource := "collections/TestClass"

	// Add a policy granting READ (but no user assignment yet).
	_, err = m.casbin.AddNamedPolicy("p", role, resource, authorization.READ, authorization.SchemaDomain)
	require.NoError(t, err)

	// Snapshot before assigning the user — this snapshot has the policy but
	// no user-to-role mapping.
	data, err := m.Snapshot()
	require.NoError(t, err)

	// Now assign the user to the role and warm the enforce cache.
	_, err = m.casbin.AddRoleForUser(user, role)
	require.NoError(t, err)
	require.NoError(t, m.casbin.InvalidateCache())

	allowed, err := m.checkPermissions(principal, resource, authorization.READ)
	require.NoError(t, err)
	require.True(t, allowed)

	// Hammer checkPermissions() concurrently during Restore(). Without the
	// restoreLock in Restore() and checkPermissions(), a concurrent reader can
	// re-cache a stale "true" after LoadPolicy() clears the cache.
	const concurrentReaders = 10
	done := make(chan struct{})
	var wg sync.WaitGroup

	stopWorkers := func() {
		select {
		case <-done:
		default:
			close(done)
		}
		wg.Wait()
	}
	defer stopWorkers()

	for range concurrentReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				// Return values are intentionally ignored — we only care that
				// concurrent calls don't re-populate the cache with stale entries.
				m.checkPermissions(principal, resource, authorization.READ)
			}
		}()
	}

	err = m.Restore(data)
	require.NoError(t, err)

	stopWorkers()

	// After Restore and all concurrent readers have stopped, the user should
	// not have access. If the cache still holds a stale "true", this fails.
	allowed, err = m.checkPermissions(principal, resource, authorization.READ)
	require.NoError(t, err)
	assert.False(t, allowed, "enforce cache was not invalidated during Restore; stale cached result returned")
}

func TestRemovePermissions(t *testing.T) {
	const role = "test-role"
	p1 := &authorization.Policy{Resource: "collections/Foo", Verb: authorization.READ, Domain: authorization.SchemaDomain}
	p2 := &authorization.Policy{Resource: "collections/Bar", Verb: authorization.READ, Domain: authorization.SchemaDomain}
	absentA := &authorization.Policy{Resource: "collections/AbsentA", Verb: authorization.READ, Domain: authorization.SchemaDomain}
	absentB := &authorization.Policy{Resource: "collections/AbsentB", Verb: authorization.READ, Domain: authorization.SchemaDomain}

	tests := []struct {
		name        string
		initial     []*authorization.Policy
		remove      []*authorization.Policy
		wantPresent []*authorization.Policy
		wantAbsent  []*authorization.Policy
	}{
		{
			// First permission absent must not abort the batch: P1 and P2 are
			// still requested and must be removed.
			name:       "first permission absent, rest present",
			initial:    []*authorization.Policy{p1, p2},
			remove:     []*authorization.Policy{absentA, p1, p2},
			wantAbsent: []*authorization.Policy{p1, p2},
		},
		{
			// A real removal followed by an absent permission must still be
			// persisted (SavePolicy must not be skipped).
			name:       "present then absent persists durably",
			initial:    []*authorization.Policy{p1},
			remove:     []*authorization.Policy{p1, absentA},
			wantAbsent: []*authorization.Policy{p1},
		},
		{
			name:        "all permissions absent is a no-op",
			initial:     []*authorization.Policy{p1},
			remove:      []*authorization.Policy{absentA, absentB},
			wantPresent: []*authorization.Policy{p1},
		},
		{
			name:       "all present happy path",
			initial:    []*authorization.Policy{p1, p2},
			remove:     []*authorization.Policy{p1, p2},
			wantAbsent: []*authorization.Policy{p1, p2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			initial := make([]authorization.Policy, len(tt.initial))
			for i, p := range tt.initial {
				initial[i] = *p
			}
			require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{role: initial}))

			require.NoError(t, m.RemovePermissions(role, tt.remove))

			assertPermissions := func(t *testing.T) {
				for _, p := range tt.wantAbsent {
					has, err := m.HasPermission(role, p)
					require.NoError(t, err)
					assert.False(t, has, "permission %v should be removed", p)
				}
				for _, p := range tt.wantPresent {
					has, err := m.HasPermission(role, p)
					require.NoError(t, err)
					assert.True(t, has, "permission %v should still be present", p)
				}
			}

			// In-memory state.
			assertPermissions(t)

			// Durable state: reload from the policy file. A skipped SavePolicy
			// would let removed permissions reappear here.
			require.NoError(t, m.casbin.LoadPolicy())
			assertPermissions(t)
		})
	}
}

func TestDeleteRoles(t *testing.T) {
	const (
		roleA   = "role-a"
		roleB   = "role-b"
		absentA = "absent-a"
		absentB = "absent-b"
	)
	perm := authorization.Policy{Resource: authorization.CollectionsMetadata("Foo")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}

	tests := []struct {
		name        string
		create      []string
		delete      []string
		wantAbsent  []string
		wantPresent []string
	}{
		{
			// An absent role early in the batch must not abort it: roleA and
			// roleB are still requested and must be deleted.
			name:       "absent role first, rest present",
			create:     []string{roleA, roleB},
			delete:     []string{absentA, roleA, roleB},
			wantAbsent: []string{roleA, roleB},
		},
		{
			// A real delete followed by an absent role must still be persisted
			// (SavePolicy/InvalidateCache must not be skipped).
			name:       "present then absent persists durably",
			create:     []string{roleA},
			delete:     []string{roleA, absentA},
			wantAbsent: []string{roleA},
		},
		{
			name:        "all roles absent is a no-op",
			create:      []string{roleA},
			delete:      []string{absentA, absentB},
			wantPresent: []string{roleA},
		},
		{
			name:       "all present happy path",
			create:     []string{roleA, roleB},
			delete:     []string{roleA, roleB},
			wantAbsent: []string{roleA, roleB},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			roles := make(map[string][]authorization.Policy, len(tt.create))
			for _, name := range tt.create {
				roles[name] = []authorization.Policy{perm}
			}
			require.NoError(t, m.CreateRolesPermissions(roles))

			require.NoError(t, m.DeleteRoles(tt.delete...))

			assertRoles := func(t *testing.T) {
				got, err := m.GetRoles(append(append([]string{}, tt.wantAbsent...), tt.wantPresent...)...)
				require.NoError(t, err)
				for _, name := range tt.wantAbsent {
					_, ok := got[name]
					assert.False(t, ok, "role %q should be deleted", name)
				}
				for _, name := range tt.wantPresent {
					_, ok := got[name]
					assert.True(t, ok, "role %q should still be present", name)
				}
			}

			// In-memory state.
			assertRoles(t)

			// Durable state: reload from the policy file. A skipped SavePolicy
			// would let deleted roles reappear here.
			require.NoError(t, m.casbin.LoadPolicy())
			assertRoles(t)
		})
	}
}

func TestSnapshotAndRestoreUpgrade(t *testing.T) {
	tests := []struct {
		name              string
		policiesInput     [][]string
		policiesExpected  [][]string
		groupingsInput    [][]string
		groupingsExpected [][]string
	}{
		{
			name: "assign users",
			policiesInput: [][]string{
				{"role:some_role", "users/.*", "U", "users"},
			},
			policiesExpected: [][]string{
				{"role:some_role", "users/.*", "A", "users"},
				// build-in roles are added after restore
				{"role:viewer", "*", authorization.READ, "*"},
				{"role:read-only", "*", authorization.READ, "*"},
				{"role:admin", "*", conv.VALID_VERBS, "*"},
				{"role:root", "*", conv.VALID_VERBS, "*"},
			},
		},
		{
			name: "build-in",
			policiesInput: [][]string{
				{"role:viewer", "*", "R", "*"},
				{"role:admin", "*", "(C)|(R)|(U)|(D)", "*"},
			},
			policiesExpected: [][]string{
				{"role:viewer", "*", "R", "*"},
				{"role:read-only", "*", "R", "*"},
				{"role:admin", "*", conv.VALID_VERBS, "*"},
				// build-in roles are added after restore
				{"role:root", "*", conv.VALID_VERBS, "*"},
			},
		},
		{
			name: "users",
			policiesInput: [][]string{
				{"role:admin", "*", "(C)|(R)|(U)|(D)", "*"}, // present to iterate over all roles in downgrade
			},
			policiesExpected: [][]string{
				{"role:admin", "*", "(C)|(R)|(U)|(D)|(A)", "*"},
				// build-in roles are added after restore
				{"role:viewer", "*", authorization.READ, "*"},
				{"role:read-only", "*", authorization.READ, "*"},
				{"role:root", "*", conv.VALID_VERBS, "*"},
			},
			groupingsInput: [][]string{
				{"user:test-user", "role:admin"},
			},
			groupingsExpected: [][]string{
				{"db:test-user", "role:admin"},
				{"oidc:test-user", "role:admin"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			sh := snapshot{Version: 0, GroupingPolicy: tt.groupingsInput, Policy: tt.policiesInput}

			bytes, err := json.Marshal(sh)
			require.NoError(t, err)

			err = m.Restore(bytes)
			require.NoError(t, err)

			finalPolicies, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			assert.ElementsMatch(t, finalPolicies, tt.policiesExpected)

			finalGroupingPolicies, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)
			assert.Equal(t, finalGroupingPolicies, tt.groupingsExpected)
		})
	}
}
