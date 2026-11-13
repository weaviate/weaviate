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

package authz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestAuthZBackupPrincipalsJourney runs as a non-root caller: each users and
// roles backup and restore is denied, then allowed once the matching
// manage_backups grant is added. Per-ID grants cover create only; restore
// replaces the subsystem's whole store, so it requires the kind wildcard.
//
// The caller is a static API-key user because a users restore replaces the
// dynamic-user store. The roles restore runs last because it replaces the
// custom-role store, the caller's own role included.
func TestAuthZBackupPrincipalsJourney(t *testing.T) {
	const (
		adminUser    = "admin-user"
		adminKey     = "admin-key"
		operatorUser = "operator-user"
		operatorKey  = "operator-key"
		backend      = "filesystem"
		operatorRole = "backup-operator"
		dynamicUser  = "alice"
		customRole   = "editor"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(operatorUser, operatorKey).
		WithRBAC().WithRbacRoots(adminUser).
		WithBackendFilesystem().WithDbUsers().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	par := articles.ParagraphsClass()
	helper.CreateClassAuth(t, par, adminKey)
	helper.CreateUser(t, dynamicUser, adminKey)
	helper.CreateRole(t, adminKey, &models.Role{
		Name:        String(customRole),
		Permissions: []*models.Permission{{Action: String(authorization.ReadCollections), Collections: &models.PermissionCollections{Collection: String(par.Class)}}},
	})
	helper.CreateRole(t, adminKey, &models.Role{
		Name:        String(operatorRole),
		Permissions: []*models.Permission{helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(par.Class).Permission()},
	})
	helper.AssignRoleToUser(t, adminKey, operatorRole, operatorUser)

	operator := helper.CreateAuth(operatorKey)
	admin := helper.CreateAuth(adminKey)
	create := func(id string, include, users, roles []string) error {
		_, err := helper.Client(t).Backups.BackupsCreate(backups.NewBackupsCreateParams().WithBackend(backend).WithBody(&models.BackupCreateRequest{
			ID: id, Include: include, IncludeUsers: users, IncludeRoles: roles, Config: helper.DefaultBackupConfig(),
		}), operator)
		return err
	}
	restore := func(id, usersOption, rolesOption string) error {
		cfg := helper.DefaultRestoreConfig()
		cfg.UsersOptions, cfg.RolesOptions = &usersOption, &rolesOption
		_, err := helper.Client(t).Backups.BackupsRestore(backups.NewBackupsRestoreParams().WithBackend(backend).WithID(id).WithBody(&models.BackupRestoreRequest{
			Config: cfg,
		}), operator)
		return err
	}
	requireCreateForbidden := func(t *testing.T, err error) {
		t.Helper()
		var forbidden *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "want 403, got %v", err)
	}
	requireRestoreForbidden := func(t *testing.T, err error) {
		t.Helper()
		var forbidden *backups.BackupsRestoreForbidden
		require.True(t, errors.As(err, &forbidden), "want 403, got %v", err)
	}

	// Class-less backups taken by root, for the restore steps.
	for id, body := range map[string]*models.BackupCreateRequest{
		"root-users": {ID: "root-users", Exclude: []string{par.Class}, IncludeUsers: []string{dynamicUser}, Config: helper.DefaultBackupConfig()},
		"root-roles": {ID: "root-roles", Exclude: []string{par.Class}, IncludeRoles: []string{customRole}, Config: helper.DefaultBackupConfig()},
	} {
		_, err := helper.Client(t).Backups.BackupsCreate(backups.NewBackupsCreateParams().WithBackend(backend).WithBody(body), admin)
		require.NoError(t, err, id)
		helper.ExpectBackupEventuallyCreated(t, id, backend, admin)
	}

	t.Run("a collection-only caller cannot back up a named user, even class-less", func(t *testing.T) {
		requireCreateForbidden(t, create("op-users-denied", nil, []string{dynamicUser}, nil))
	})

	t.Run("a collection-only caller cannot back up a named role", func(t *testing.T) {
		requireCreateForbidden(t, create("op-roles-denied", []string{par.Class}, nil, []string{customRole}))
	})

	t.Run("a collection-only caller cannot restore users", func(t *testing.T) {
		requireRestoreForbidden(t, restore("root-users", models.RestoreConfigUsersOptionsAll, models.RestoreConfigRolesOptionsNoRestore))
	})

	t.Run("a collection-only caller cannot restore roles", func(t *testing.T) {
		requireRestoreForbidden(t, restore("root-roles", models.RestoreConfigUsersOptionsNoRestore, models.RestoreConfigRolesOptionsAll))
	})

	t.Run("grant manage_backups on the user and the role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, operatorRole,
			helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithUser(dynamicUser).Permission(),
			helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithRole(customRole).Permission(),
		)
	})

	t.Run("the grants read back with user or role set and no collection", func(t *testing.T) {
		var gotUser, gotRole bool
		for _, p := range helper.GetRoleByName(t, adminKey, operatorRole).Permissions {
			if p.Backups == nil || *p.Action != authorization.ManageBackups {
				continue
			}
			switch {
			case p.Backups.User != nil:
				require.Equal(t, dynamicUser, *p.Backups.User)
				require.Nil(t, p.Backups.Collection)
				require.Nil(t, p.Backups.Role)
				gotUser = true
			case p.Backups.Role != nil:
				require.Equal(t, customRole, *p.Backups.Role)
				require.Nil(t, p.Backups.Collection)
				gotRole = true
			}
		}
		require.True(t, gotUser && gotRole)
	})

	t.Run("the caller backs up the named user in a class-less backup", func(t *testing.T) {
		require.NoError(t, create("op-users", nil, []string{dynamicUser}, nil))
		// Class-less backups still need the collections wildcard to poll.
		helper.ExpectBackupEventuallyCreated(t, "op-users", backend, admin)
	})

	t.Run("the caller backs up the named role", func(t *testing.T) {
		require.NoError(t, create("op-roles", []string{par.Class}, nil, []string{customRole}))
		helper.ExpectBackupEventuallyCreated(t, "op-roles", backend, admin)
	})

	t.Run("per-ID grants do not allow a users restore", func(t *testing.T) {
		requireRestoreForbidden(t, restore("root-users", models.RestoreConfigUsersOptionsAll, models.RestoreConfigRolesOptionsNoRestore))
	})

	t.Run("per-ID grants do not allow a roles restore", func(t *testing.T) {
		requireRestoreForbidden(t, restore("root-roles", models.RestoreConfigUsersOptionsNoRestore, models.RestoreConfigRolesOptionsAll))
	})

	t.Run("grant the users and roles wildcards", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, operatorRole,
			helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithUser("*").Permission(),
			helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithRole("*").Permission(),
		)
	})

	t.Run("the caller restores users", func(t *testing.T) {
		helper.DeleteUser(t, dynamicUser, adminKey)
		require.NoError(t, restore("root-users", models.RestoreConfigUsersOptionsAll, models.RestoreConfigRolesOptionsNoRestore))
		helper.ExpectBackupEventuallyRestored(t, "root-users", backend, admin)
		require.Equal(t, dynamicUser, *helper.GetUser(t, dynamicUser, adminKey).UserID)
	})

	t.Run("the caller restores roles", func(t *testing.T) {
		helper.DeleteRole(t, adminKey, customRole)
		require.NoError(t, restore("root-roles", models.RestoreConfigUsersOptionsNoRestore, models.RestoreConfigRolesOptionsAll))
		helper.ExpectBackupEventuallyRestored(t, "root-roles", backend, admin)
		require.Equal(t, customRole, *helper.GetRoleByName(t, adminKey, customRole).Name)
	})
}
