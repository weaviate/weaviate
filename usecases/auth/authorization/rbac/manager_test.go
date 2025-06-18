//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rbac

import (
	"encoding/json"
	"testing"

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
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", models.UserTypeInputDb), conv.PrefixRoleName("admin"))
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
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", models.UserTypeInputDb), conv.PrefixRoleName("admin"))
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", models.UserTypeInputDb), conv.PrefixRoleName("editor"))
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
	require.Len(t, policies, 4)

	err = m.Restore([]byte{})
	require.NoError(t, err)

	// nothing overwritten
	policies, err = m.casbin.GetPolicy()
	require.NoError(t, err)
	require.Len(t, policies, 4)
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
