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
	"bufio"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

func TestDowngradeRoles(t *testing.T) {
	tests := []struct {
		name             string
		lines            []string
		expectedPolicies [][]string
	}{
		{
			name: "skip build in roles",
			lines: []string{
				"p, role:other_role, data/collections/.*/shards/.*/objects/.*, R, data",
				"p, role:viewer, *, R, *",
			},
			expectedPolicies: [][]string{
				{"role:other_role", "data/collections/.*/shards/.*/objects/.*", " R", "data"},
			},
		},
		{
			name: "downgrade user if it has a assign",
			lines: []string{
				"p, role:some_role, users/.*, A, users",
			},
			expectedPolicies: [][]string{
				{"p, role:some_role, users/.*, U, users"},
			},
		},
		{
			name: "mixed upgrade and not upgrade and skip",
			lines: []string{
				"p, role:some_role, users/.*, A, users",
				"p, role:other_role, data/collections/.*/shards/.*/objects/.*, R, data",
				"p, role:viewer, *, R, *",
			},
			expectedPolicies: [][]string{
				{"p, role:some_role, users/.*, U, users, "},
				{"role:other_role", "data/collections/.*/shards/.*/objects/.*", " R", "data"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := t.TempDir()
			tmpFile, err := os.CreateTemp(path, "upgrade-temp-*.tmp")
			require.NoError(t, err)

			writer := bufio.NewWriter(tmpFile)
			for _, line := range tt.lines {
				_, err := fmt.Fprintln(writer, line)
				require.NoError(t, err)
			}
			require.NoError(t, writer.Flush())
			require.NoError(t, tmpFile.Sync())
			require.NoError(t, tmpFile.Close())

			m, err := model.NewModelFromString(MODEL)
			require.NoError(t, err)

			enforcer, err := casbin.NewSyncedCachedEnforcer(m)
			require.NoError(t, err)
			enforcer.SetAdapter(fileadapter.NewAdapter(tmpFile.Name()))
			require.NoError(t, enforcer.LoadPolicy())

			require.NoError(t, downgradeRolesFrom130(enforcer))
			policies, _ := enforcer.GetPolicy()
			require.Len(t, policies, len(tt.expectedPolicies))
			for i, policy := range tt.expectedPolicies {
				require.Equal(t, tt.expectedPolicies[i], policy)
			}
		})
	}
}

func TestDowngradeGroupings(t *testing.T) {
	path := t.TempDir()
	tmpFile, err := os.Create(path + "/version")
	require.NoError(t, err)
	writer := bufio.NewWriter(tmpFile)
	_, err = fmt.Fprint(writer, "1.30.0")
	require.NoError(t, err)
	require.NoError(t, writer.Flush())
	require.NoError(t, tmpFile.Sync())
	require.NoError(t, tmpFile.Close())

	tests := []struct {
		name               string
		rolesToAdd         []string
		assignments        map[string]string
		expectedAfterWards map[string]string
	}{
		{
			name:               "only internal - will only be added as db",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"db:" + conv.InternalPlaceHolder: "role:test"},
			expectedAfterWards: map[string]string{"user:" + conv.InternalPlaceHolder: "role:test"},
		},
		{
			name:               "db user will be re added as user",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"db:something": "role:test"},
			expectedAfterWards: map[string]string{"user:something": "role:test"},
		},
		{
			name:               "oidc user will be skipped",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"oidc:something": "role:test"},
			expectedAfterWards: map[string]string{},
		},
		{
			name:               "both added - will only be added once as user",
			rolesToAdd:         []string{"role:test"},
			assignments:        map[string]string{"db:something": "role:test", "oidc:something": "role:test"},
			expectedAfterWards: map[string]string{"user:something": "role:test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := model.NewModelFromString(MODEL)
			require.NoError(t, err)

			enforcer, err := casbin.NewSyncedCachedEnforcer(m)
			require.NoError(t, err)

			for _, role := range tt.rolesToAdd {
				_, err := enforcer.AddNamedPolicy("p", role, "*", "R", "*")
				require.NoError(t, err)

			}

			for user, role := range tt.assignments {
				_, err := enforcer.AddRoleForUser(user, role)
				require.NoError(t, err)
			}

			require.NoError(t, downgradesAssignmentsFrom130(enforcer, path))
			roles, _ := enforcer.GetAllSubjects()
			require.Len(t, roles, len(tt.rolesToAdd))
			for user, role := range tt.expectedAfterWards {
				users, err := enforcer.GetUsersForRole(role)
				require.NoError(t, err)
				require.True(t, slices.Contains(users, user))
			}
		})
	}
}
