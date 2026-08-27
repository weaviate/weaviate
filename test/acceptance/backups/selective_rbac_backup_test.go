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

package backups

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	adminUser = "admin-user"
	adminKey  = "admin-key"
	backend   = "filesystem"
)

// TestSelectiveRBACBackupRestore covers includeUsers and includeRoles end to end on a
// single node. Unit tests pin the filter and the plumbing separately; nothing before this
// proved that a subset selected at the API survives a real backup and comes back.
func TestSelectiveRBACBackupRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).
		WithRBAC().WithRbacRoots(adminUser).
		WithBackendFilesystem().WithDbUsers().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// A backup needs at least one class, so every subtest carries this one.
	par := articles.ParagraphsClass()

	t.Run("RestoreReplacesTheStore", func(t *testing.T) {
		backupID := "selective-replace"
		seedRBAC(t)
		defer cleanupRBAC(t)

		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		createSelectiveBackup(t, par.Class, backupID,
			[]string{roleName(1), roleName(2)},
			[]string{userName(1), userName(2)})

		// Nothing is deleted here. The restore itself is what removes roles 3 to 5 and
		// users 3 to 5, because it clears the store and reloads it from the blob.
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		restoreAll(t, par.Class, backupID)

		gotRoles := customRoleNames(t)
		assert.ElementsMatch(t, []string{roleName(1), roleName(2)}, gotRoles)

		gotUsers := dynamicUserNames(t)
		assert.ElementsMatch(t, []string{userName(1), userName(2)}, gotUsers)
	})

	t.Run("BackupCarriesOnlyTheSelection", func(t *testing.T) {
		backupID := "selective-filter"
		seedRBAC(t)
		defer cleanupRBAC(t)

		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		createSelectiveBackup(t, par.Class, backupID,
			[]string{roleName(1), roleName(2)},
			[]string{userName(1), userName(2)})

		// Deleting everything first is what makes the next assertions about the
		// selection rather than about the restore. Without it, an absent role could
		// mean either that the filter excluded it or that the restore removed it.
		deleteAllSeeded(t)
		require.Empty(t, customRoleNames(t))
		require.Empty(t, dynamicUserNames(t))

		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		restoreAll(t, par.Class, backupID)

		assert.ElementsMatch(t, []string{roleName(1), roleName(2)}, customRoleNames(t))
		assert.ElementsMatch(t, []string{userName(1), userName(2)}, dynamicUserNames(t))

		// A selected role keeps the assignment to a selected user.
		assert.Contains(t, roleNamesForUser(t, userName(1)), roleName(1))

		// The assignment from a selected role to an unselected user is also in the
		// blob, so the grant returns even though the user did not.
		assert.Contains(t, helper.GetUserForRoles(t, roleName(2), adminKey), userName(4))
	})

	t.Run("WholeClusterBackupCarriesEverything", func(t *testing.T) {
		backupID := "whole-cluster"
		seedRBAC(t)
		defer cleanupRBAC(t)

		helper.CreateClassAuth(t, par, adminKey)
		defer helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))

		// No selection at all. This is the branch that must stay byte-compatible with
		// the behaviour that shipped before includeRoles existed.
		createSelectiveBackup(t, par.Class, backupID, nil, nil)

		deleteAllSeeded(t)
		helper.DeleteClassWithAuthz(t, par.Class, helper.CreateAuth(adminKey))
		restoreAll(t, par.Class, backupID)

		assert.ElementsMatch(t, allRoleNames(), customRoleNames(t))
		assert.ElementsMatch(t, allUserNames(), dynamicUserNames(t))
	})
}

func roleName(i int) string { return fmt.Sprintf("backup-role-%d", i) }

func userName(i int) string { return fmt.Sprintf("backup-user-%d", i) }

func allRoleNames() []string {
	names := make([]string, 0, 5)
	for i := 1; i <= 5; i++ {
		names = append(names, roleName(i))
	}
	return names
}

func allUserNames() []string {
	names := make([]string, 0, 5)
	for i := 1; i <= 5; i++ {
		names = append(names, userName(i))
	}
	return names
}

// seedRBAC builds five users, five custom roles, and one assignment per pair. Role 2 is
// additionally assigned to user 4, so a selected role holds a grant to a user the
// selection leaves out.
func seedRBAC(t *testing.T) {
	t.Helper()
	for i := 1; i <= 5; i++ {
		name := roleName(i)
		helper.CreateRole(t, adminKey, &models.Role{
			Name: &name,
			Permissions: []*models.Permission{
				helper.NewCollectionsPermission().
					WithAction(authorization.ReadCollections).
					WithCollection(fmt.Sprintf("Collection%d", i)).
					Permission(),
			},
		})
		helper.CreateUser(t, userName(i), adminKey)
		helper.AssignRoleToUser(t, adminKey, name, userName(i))
	}
	helper.AssignRoleToUser(t, adminKey, roleName(2), userName(4))
}

// cleanupRBAC removes whatever survived the subtest. Each subtest leaves the cluster in a
// different state, so every delete tolerates an already-absent target.
func cleanupRBAC(t *testing.T) {
	t.Helper()
	existingRoles := customRoleNames(t)
	for _, name := range existingRoles {
		helper.DeleteRole(t, adminKey, name)
	}
	for _, name := range dynamicUserNames(t) {
		helper.DeleteUser(t, name, adminKey)
	}
}

func deleteAllSeeded(t *testing.T) {
	t.Helper()
	for _, name := range customRoleNames(t) {
		helper.DeleteRole(t, adminKey, name)
	}
	for _, name := range dynamicUserNames(t) {
		helper.DeleteUser(t, name, adminKey)
	}
}

// customRoleNames returns the seeded roles only. Built-in roles are always present and
// are re-created on restore from configuration, so they say nothing about the selection.
func customRoleNames(t *testing.T) []string {
	t.Helper()
	seeded := make(map[string]struct{}, 5)
	for _, name := range allRoleNames() {
		seeded[name] = struct{}{}
	}
	var got []string
	for _, role := range helper.GetRoles(t, adminKey) {
		if role.Name == nil {
			continue
		}
		if _, ok := seeded[*role.Name]; ok {
			got = append(got, *role.Name)
		}
	}
	return got
}

// dynamicUserNames returns the seeded users only. The admin is a static API key user and
// is never part of a dynamic user backup.
func dynamicUserNames(t *testing.T) []string {
	t.Helper()
	seeded := make(map[string]struct{}, 5)
	for _, name := range allUserNames() {
		seeded[name] = struct{}{}
	}
	var got []string
	for _, user := range helper.ListAllUsers(t, adminKey) {
		if user.UserID == nil {
			continue
		}
		if _, ok := seeded[*user.UserID]; ok {
			got = append(got, *user.UserID)
		}
	}
	return got
}

func roleNamesForUser(t *testing.T, user string) []string {
	t.Helper()
	var names []string
	for _, role := range helper.GetRolesForUser(t, user, adminKey, false) {
		if role.Name != nil {
			names = append(names, *role.Name)
		}
	}
	return names
}

// createSelectiveBackup posts a backup carrying includeRoles and includeUsers, which the
// shared helper does not expose, then waits for it to finish.
func createSelectiveBackup(t *testing.T, className, backupID string, roles, users []string) {
	t.Helper()
	params := backups.NewBackupsCreateParams().
		WithBackend(backend).
		WithBody(&models.BackupCreateRequest{
			ID:           backupID,
			Include:      []string{className},
			IncludeRoles: roles,
			IncludeUsers: users,
			Config:       helper.DefaultBackupConfig(),
		})
	resp, err := helper.Client(t).Backups.BackupsCreate(params, auth())
	require.Nil(t, err)
	require.NotNil(t, resp.Payload)
	require.Equal(t, "", resp.Payload.Error)

	helper.ExpectBackupEventuallyCreated(t, backupID, backend, auth(),
		helper.WithPollInterval(helper.MinPollInterval), helper.WithDeadline(helper.MaxDeadline))
}

// restoreAll restores with both RBAC options set, since the API defaults both to
// noRestore and would otherwise leave roles and users untouched.
func restoreAll(t *testing.T, className, backupID string) {
	t.Helper()
	all := "all"
	cfg := helper.DefaultRestoreConfig()
	cfg.RolesOptions = &all
	cfg.UsersOptions = &all

	resp, err := helper.RestoreBackupWithAuthz(t, cfg, className, backend, backupID, map[string]string{}, auth())
	require.Nil(t, err)
	require.NotNil(t, resp.Payload)
	require.Equal(t, "", resp.Payload.Error)

	helper.ExpectBackupEventuallyRestored(t, backupID, backend, auth(),
		helper.WithPollInterval(helper.MinPollInterval), helper.WithDeadline(helper.MaxDeadline))
}

func auth() runtime.ClientAuthInfoWriter { return helper.CreateAuth(adminKey) }
