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
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("customAdmin"), "*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("customAdmin"))
				return err
			},
		},
		{
			name: "multiple roles and policies",
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("customAdmin"), "*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("editor"), "collections/*", authorization.UPDATE, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("customAdmin"))
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

// TestManager_DeleteRoles_MultiRoleBatchPersistsAcrossReload pins that an
// already-absent role in a batch delete does not drop persistence of the roles
// (and their assignments) removed alongside it: the cleanup cascade calls
// DeleteRoles with many names, and a concurrent delete can leave one already
// gone. Reloading the policy from disk (a restart) must still see every removed
// role and assignment gone.
func TestManager_DeleteRoles_MultiRoleBatchPersistsAcrossReload(t *testing.T) {
	const (
		roleA = "batchRoleA"
		roleB = "batchRoleB"
		roleC = "batchRoleC"
	)
	allRoles := []string{roleA, roleB, roleC}
	user := conv.UserNameWithTypeFromId("batch-user", authentication.AuthTypeDb)

	tests := []struct {
		name       string
		preRemoved []string // roles deleted (and persisted) before the batch
	}{
		{name: "no pre-removed role", preRemoved: nil},
		{name: "some roles already absent", preRemoved: []string{roleB}},
		{name: "all roles already absent", preRemoved: allRoles},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			for _, r := range allRoles {
				require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
					r: {{Resource: "data/collections/Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
				}))
			}
			// Assign every role so the batch must also persist assignment removal.
			require.NoError(t, m.AddRolesForUser(user, allRoles))

			if len(tt.preRemoved) > 0 {
				require.NoError(t, m.DeleteRoles(tt.preRemoved...))
			}

			require.NoError(t, m.DeleteRoles(allRoles...))

			// Drop in-memory state and reload from the persisted policy file,
			// which is what a restart does.
			require.NoError(t, m.casbin.LoadPolicy())

			got, err := m.GetRoles()
			require.NoError(t, err)
			for _, r := range allRoles {
				_, exists := got[r]
				require.Falsef(t, exists, "role %q must stay deleted across reload", r)
			}

			assignments, err := m.GetRolesForUserOrGroup("batch-user", authentication.AuthTypeDb, false)
			require.NoError(t, err)
			require.Empty(t, assignments, "assignments must stay removed across reload")
		})
	}
}

// TestManager_CountNamespaceLocalRBAC pins the filtering: a namespace-local
// role and assignments to its direct principals count (even when the assigned
// role is global), while global roles and out-of-namespace subjects do not.
func TestManager_CountNamespaceLocalRBAC(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
		"customer1:editor": {{Resource: "data/collections/customer1:Movies/shards/*/objects/*", Verb: "R", Domain: authorization.DataDomain}},
		"auditor":          {{Resource: "data/collections/Movies/shards/*/objects/*", Verb: "R", Domain: authorization.DataDomain}},
	}))
	// Namespaced db user with a local role, namespaced oidc user with a global
	// role, and a global db user with the global role.
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("customer1:alice", authentication.AuthTypeDb), []string{"customer1:editor"}))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("customer1:carol", authentication.AuthTypeOIDC), []string{"auditor"}))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("bob", authentication.AuthTypeDb), []string{"auditor"}))
	// A namespace-named group is still a global assignment: it must not count,
	// else the namespace could never be removed (the cascade leaves it).
	require.NoError(t, m.AddRolesForUser(conv.PrefixGroupName("customer1:team"), []string{"auditor"}))

	// 1 local role + 2 namespaced assignments (alice, carol); auditor + bob + group excluded.
	got, err := m.CountNamespaceLocalRBAC("customer1")
	require.NoError(t, err)
	assert.Equal(t, 3, got)

	// The cascade enumeration surfaces the two namespaced users, never the
	// namespace-named group: a group is global, so it must never be treated as a
	// namespace-local subject the cascade would try to revoke from.
	_, subjects, err := m.NamespaceLocalRBAC("customer1")
	require.NoError(t, err)
	ids := make([]string, len(subjects))
	for i, s := range subjects {
		ids[i] = s.ID
	}
	assert.ElementsMatch(t, []string{"customer1:alice", "customer1:carol"}, ids)

	other, err := m.CountNamespaceLocalRBAC("customer2")
	require.NoError(t, err)
	assert.Equal(t, 0, other)

	// Revoking a namespaced subject's global-role assignment — what the delete
	// cascade does — drives the gate down: carol holds only the global auditor
	// role, so revoking it drops the count by one.
	require.NoError(t, m.RevokeRolesForUser(conv.UserNameWithTypeFromId("customer1:carol", authentication.AuthTypeOIDC), "auditor"))
	got, err = m.CountNamespaceLocalRBAC("customer1")
	require.NoError(t, err)
	assert.Equal(t, 2, got)
}

// TestManager_NamespaceLocalRBAC_FailsClosedOnUnparseableRow pins the gate's
// fail-closed contract: an unparseable grouping subject must surface as an
// error, not be skipped — a silent undercount would let the removal-block gate
// read zero and remove a namespace while an assignment survives.
func TestManager_NamespaceLocalRBAC_FailsClosedOnUnparseableRow(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	// Inject a grouping row whose subject has no auth-type prefix, so
	// GetUserAndPrefix can't parse it.
	_, err = m.casbin.AddRoleForUser("malformed-no-prefix", conv.PrefixRoleName("auditor"))
	require.NoError(t, err)

	_, _, err = m.NamespaceLocalRBAC("customer1")
	require.Error(t, err)

	_, err = m.CountNamespaceLocalRBAC("customer1")
	require.Error(t, err)
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

// TestPrettyPermissionsResources_NamespaceStripping exercises the pretty-
// printer used in audit logs on namespace enabled clusters: a namespace-bound
// principal sees short entity names (own namespace stripped), while a
// global principal (and any NS-disabled principal, since their Namespace
// is always "") sees the raw qualified names. A foreign prefix is left
// intact so the embedded ":" remains visible in the audit trail.
func TestPrettyPermissionsResources_NamespaceStripping(t *testing.T) {
	nsCaller := &models.Principal{Username: "customer1:alice", Namespace: "customer1"}
	globalCaller := &models.Principal{Username: "admin", IsGlobalOperator: true}

	strPtr := func(s string) *string { return &s }

	type row struct {
		domain     string
		perm       *models.Permission
		wantNS     string
		wantGlobal string
	}

	rows := []row{
		{
			domain:     "Collections",
			perm:       &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
			wantNS:     "[Domain: collections, Collection: Movies]",
			wantGlobal: "[Domain: collections, Collection: customer1:Movies]",
		},
		{
			domain: "Aliases",
			perm: &models.Permission{Aliases: &models.PermissionAliases{
				Collection: strPtr("customer1:Movies"),
				Alias:      strPtr("customer1:Top10"),
			}},
			wantNS:     "[Domain: aliases, Collection: Movies, Alias: Top10]",
			wantGlobal: "[Domain: aliases, Collection: customer1:Movies, Alias: customer1:Top10]",
		},
		{
			domain:     "Users",
			perm:       &models.Permission{Users: &models.PermissionUsers{Users: strPtr("customer1:bob")}},
			wantNS:     "[Domain: users, User: bob]",
			wantGlobal: "[Domain: users, User: customer1:bob]",
		},
		{
			domain: "Replicate",
			perm: &models.Permission{Replicate: &models.PermissionReplicate{
				Collection: strPtr("customer1:Movies"),
				Shard:      strPtr("shard-1"),
			}},
			wantNS:     "[Domain: replicate, Collection: Movies, Shard: shard-1]",
			wantGlobal: "[Domain: replicate, Collection: customer1:Movies, Shard: shard-1]",
		},
		{
			domain:     "Backups",
			perm:       &models.Permission{Backups: &models.PermissionBackups{Collection: strPtr("customer1:Movies")}},
			wantNS:     "[Domain: backups,Collection: Movies]",
			wantGlobal: "[Domain: backups,Collection: customer1:Movies]",
		},
		{
			domain: "Nodes",
			perm: &models.Permission{Nodes: &models.PermissionNodes{
				Verbosity:  strPtr("verbose"),
				Collection: strPtr("customer1:Movies"),
			}},
			wantNS:     "[Domain: nodes, Verbosity: verbose, Collection: Movies]",
			wantGlobal: "[Domain: nodes, Verbosity: verbose, Collection: customer1:Movies]",
		},
		{
			domain:     "Roles",
			perm:       &models.Permission{Roles: &models.PermissionRoles{Role: strPtr("admin")}},
			wantNS:     "[Domain: roles, Role: admin]",
			wantGlobal: "[Domain: roles, Role: admin]",
		},
		{
			domain: "Tenants",
			perm: &models.Permission{Tenants: &models.PermissionTenants{
				Collection: strPtr("customer1:Movies"),
				Tenant:     strPtr("t1"),
			}},
			wantNS:     "[Domain: tenants, Collection: Movies, Tenant: t1]",
			wantGlobal: "[Domain: tenants, Collection: customer1:Movies, Tenant: t1]",
		},
		{
			// Collection nil while Tenant is set must not panic: the
			// block dereferenced *Collection while guarding only Tenant.
			domain: "Tenants_nil_collection",
			perm: &models.Permission{Tenants: &models.PermissionTenants{
				Tenant: strPtr("t1"),
			}},
			wantNS:     "[Domain: tenants, Tenant: t1]",
			wantGlobal: "[Domain: tenants, Tenant: t1]",
		},
	}

	for _, r := range rows {
		t.Run(r.domain+"/ns_caller_strips", func(t *testing.T) {
			require.Equal(t, r.wantNS, prettyPermissionsResources(nsCaller, r.perm))
		})
		t.Run(r.domain+"/global_caller_raw", func(t *testing.T) {
			require.Equal(t, r.wantGlobal, prettyPermissionsResources(globalCaller, r.perm))
		})
	}

	// Foreign-namespace prefix must remain intact so the embedded ":"
	// stays in the audit trail.
	t.Run("ns_caller_foreign_namespace_kept", func(t *testing.T) {
		perm := &models.Permission{Data: &models.PermissionData{
			Collection: strPtr("customer2:Movies"),
			Tenant:     strPtr("*"),
			Object:     strPtr("*"),
		}}
		require.Equal(t,
			"[Domain: data, Collection: customer2:Movies, Tenant: *, Object: *]",
			prettyPermissionsResources(nsCaller, perm),
		)
	})
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

// TestCheckPermissions_OperatorWithNamespaceTreatedAsGlobal pins that the
// enforce path derives confinement via namespacing.ConfinedNamespace: a global
// operator is unconfined even if a namespace is set on its principal, so it
// keeps access to operator-only domains. A namespace-bound (non-operator)
// principal with the same role is still denied those domains.
func TestCheckPermissions_OperatorWithNamespaceTreatedAsGlobal(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	const subject = "operator-user"
	_, err = m.casbin.AddRoleForUser(
		conv.UserNameWithTypeFromId(subject, authentication.AuthTypeDb),
		conv.PrefixRoleName(authorization.Root),
	)
	require.NoError(t, err)

	// cluster/* is an operator-only domain: denied to confined callers.
	resource := authorization.Cluster()

	tests := []struct {
		name      string
		principal *models.Principal
		want      bool
	}{
		{
			name:      "operator without namespace",
			principal: &models.Principal{Username: subject, UserType: models.UserTypeInputDb, IsGlobalOperator: true},
			want:      true,
		},
		{
			name:      "operator with stray namespace stays unconfined",
			principal: &models.Principal{Username: subject, UserType: models.UserTypeInputDb, IsGlobalOperator: true, Namespace: "customer1"},
			want:      true,
		},
		{
			name:      "namespaced non-operator stays confined",
			principal: &models.Principal{Username: subject, UserType: models.UserTypeInputDb, Namespace: "customer1"},
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, err := m.checkPermissions(tt.principal, resource, authorization.READ)
			require.NoError(t, err)
			assert.Equal(t, tt.want, allowed)
		})
	}
}
