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
