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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzAliases(t *testing.T) {
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"
	limitedUser := "limited-user"
	limitedKey := "limited-key"
	customRole := "custom"
	onlyAliasesStartingWithAliasRole := "only-aliases"

	_, down := composeUpShared(t)
	defer down()

	clsA := articles.ArticlesClass()

	helper.CreateClassAuth(t, articles.ParagraphsClass(), adminKey)
	helper.CreateClassAuth(t, clsA, adminKey)
	helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("article1").Object()}, adminKey)

	alias1 := "AliasParagraph1"
	alias2 := "AliasParagraph2"
	alias3 := "AliasParagraph3"
	otherAlias1 := "OtherAliasForParagraph1"

	for _, alias := range []string{alias1, alias2, alias3, otherAlias1} {
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: articles.ParagraphsClass().Class}, adminKey)
	}

	// make custom role with read_aliases to read all aliases
	helper.CreateRole(t, adminKey, &models.Role{Name: &customRole, Permissions: []*models.Permission{
		helper.NewAliasesPermission().WithAction(authorization.ReadAliases).Permission(),
	}})

	// create a role that is able to list aliases starting with Alias*
	helper.CreateRole(t, adminKey, &models.Role{Name: &onlyAliasesStartingWithAliasRole, Permissions: []*models.Permission{
		helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithAlias("Alias*").Permission(),
	}})

	t.Run("fail to get aliaes without minimal read_aliases", func(t *testing.T) {
		resp, err := helper.Client(t).Schema.AliasesGet(schema.NewAliasesGetParams(), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Payload)
		assert.Len(t, resp.Payload.Aliases, 0)
	})

	t.Run("assign custom role to custom user", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, customRole, customUser)
	})

	t.Run("get minimal aliases with read_aliases", func(t *testing.T) {
		resp, err := helper.Client(t).Schema.AliasesGet(schema.NewAliasesGetParams(), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Aliases, 4)
	})

	t.Run("assign only aliases role to limited user and try to get aliases", func(t *testing.T) {
		helper.AssignRoleToUser(t, adminKey, onlyAliasesStartingWithAliasRole, limitedUser)
		resp, err := helper.Client(t).Schema.AliasesGet(schema.NewAliasesGetParams(), helper.CreateAuth(limitedKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Aliases, 3)
	})

	t.Run("to get one alias with onlyAliasesStartingWithAliasRole role", func(t *testing.T) {
		params := schema.NewAliasesGetAliasParams().WithAliasName(alias1)
		resp, err := helper.Client(t).Schema.AliasesGetAlias(params, helper.CreateAuth(limitedKey))
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
	})
}

func TestAuthzUpdateAliasRequiresPermissionOnCurrentTarget(t *testing.T) {
	adminKey := "admin-key"
	malloryUser, malloryKey := "custom-user2", "custom-key2"
	readerUser, readerKey := "test-user", "test-key"
	ingestUser, ingestKey := "write-user", "write-key"

	const (
		victimClass   = "CompanyOrders"
		scratchClass  = "MalloryScratch"
		aliasName     = "Orders"
		scratchRole   = "scratch-owner"
		readerRole    = "orders-reader"
		ingestRole    = "ingest-all"
		bothSidesRole = "orders-mover"
	)

	_, down := composeUpShared(t)
	defer down()

	for _, class := range []string{victimClass, scratchClass} {
		helper.CreateClassAuth(t, &models.Class{Class: class}, adminKey)
	}
	helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: victimClass}, adminKey)

	scratchPermissions := []*models.Permission{
		helper.NewAliasesPermission().WithAction(authorization.UpdateAliases).WithCollection(scratchClass).Permission(),
		helper.NewAliasesPermission().WithAction(authorization.DeleteAliases).WithCollection(scratchClass).Permission(),
		helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(scratchClass).Permission(),
	}
	helper.CreateRole(t, adminKey, &models.Role{Name: String(scratchRole), Permissions: scratchPermissions})
	helper.AssignRoleToUser(t, adminKey, scratchRole, malloryUser)
	helper.CreateRole(t, adminKey, &models.Role{Name: String(readerRole), Permissions: append([]*models.Permission{
		helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithCollection(victimClass).WithAlias(aliasName).Permission(),
	}, scratchPermissions...)})
	helper.AssignRoleToUser(t, adminKey, readerRole, readerUser)
	helper.CreateRole(t, adminKey, &models.Role{Name: String(ingestRole), Permissions: []*models.Permission{
		helper.NewDataPermission().WithAction(authorization.CreateData).WithCollection("*").Permission(),
	}})
	helper.AssignRoleToUser(t, adminKey, ingestRole, ingestUser)

	assertTarget := func(t *testing.T, want string) {
		t.Helper()
		got := helper.GetAliasWithAuthz(t, aliasName, helper.CreateAuth(adminKey))
		require.Equal(t, want, got.Class)
	}

	t.Run("caller who cannot read the alias gets the missing-alias answer", func(t *testing.T) {
		_, missingErr := helper.UpdateAliasAuthWithReturn(t, "NoSuchAlias", scratchClass, malloryKey)
		var missing *schema.AliasesUpdateNotFound
		require.ErrorAs(t, missingErr, &missing)

		_, err := helper.UpdateAliasAuthWithReturn(t, aliasName, scratchClass, malloryKey)
		var denied *schema.AliasesUpdateNotFound
		require.ErrorAs(t, err, &denied)
		assert.Equal(t, missing, denied)
		assert.Equal(t, missingErr.Error(), err.Error())
		assertTarget(t, victimClass)
	})

	t.Run("caller who can read the alias gets forbidden", func(t *testing.T) {
		_, err := helper.UpdateAliasAuthWithReturn(t, aliasName, scratchClass, readerKey)
		var forbidden *schema.AliasesUpdateForbidden
		require.ErrorAs(t, err, &forbidden)
		assertTarget(t, victimClass)
	})

	t.Run("writes through the alias still land in the original collection", func(t *testing.T) {
		require.NoError(t, helper.CreateObjectAuth(t, &models.Object{Class: aliasName, Properties: map[string]any{}}, ingestKey))

		res, err := helper.ListObjectsAuth(t, scratchClass, malloryKey)
		require.NoError(t, err)
		assert.Empty(t, res.Objects)
		res, err = helper.ListObjectsAuth(t, victimClass, adminKey)
		require.NoError(t, err)
		assert.Len(t, res.Objects, 1)
	})

	t.Run("delete_aliases on the new target alone cannot delete the alias", func(t *testing.T) {
		_, err := helper.DeleteAliasAuthWithReturn(t, aliasName, malloryKey)
		var forbidden *schema.AliasesDeleteForbidden
		var notFound *schema.AliasesDeleteNotFound
		require.True(t, errors.As(err, &forbidden) || errors.As(err, &notFound),
			"want 403 or 404, got %v", err)
		assertTarget(t, victimClass)
	})

	t.Run("update_aliases on both targets moves the alias", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{Name: String(bothSidesRole), Permissions: []*models.Permission{
			helper.NewAliasesPermission().WithAction(authorization.UpdateAliases).WithCollection(victimClass).WithAlias(aliasName).Permission(),
		}})
		helper.AssignRoleToUser(t, adminKey, bothSidesRole, malloryUser)

		_, err := helper.UpdateAliasAuthWithReturn(t, aliasName, scratchClass, malloryKey)
		require.NoError(t, err)
		assertTarget(t, scratchClass)
	})
}

// Restoring a collection must not move or create aliases the caller holds no
// alias permission on. The caller below holds only manage_backups and
// delete_collections on her own collections.
func TestAuthzRestoreAliasesRequireAliasPermission(t *testing.T) {
	adminKey := "admin-key"
	malloryUser, malloryKey := "custom-user2", "custom-key2"

	const (
		backend     = "filesystem"
		backupRole  = "mallory-backup-owner"
		malloryColl = "Mallory*"
	)

	_, down := composeUpShared(t)
	defer down()

	helper.CreateRole(t, adminKey, &models.Role{Name: String(backupRole), Permissions: []*models.Permission{
		helper.NewBackupPermission().WithAction(authorization.ManageBackups).WithCollection(malloryColl).Permission(),
		helper.NewCollectionsPermission().WithAction(authorization.DeleteCollections).WithCollection(malloryColl).Permission(),
	}})
	helper.AssignRoleToUser(t, adminKey, backupRole, malloryUser)

	backUp := func(t *testing.T, class string) string {
		t.Helper()
		id := fmt.Sprintf("alias-restore-%d", time.Now().UnixNano())
		_, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), class, backend, id, helper.CreateAuth(malloryKey))
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, id, backend, helper.CreateAuth(adminKey))
		return id
	}

	// The restore is either refused up front or completes; any other outcome
	// fails here rather than letting the alias assertion pass by accident.
	restore := func(t *testing.T, class, id string, overwriteAlias bool) {
		t.Helper()
		helper.DeleteClassWithAuthz(t, class, helper.CreateAuth(malloryKey))
		params := backups.NewBackupsRestoreParams().WithBackend(backend).WithID(id).
			WithBody(&models.BackupRestoreRequest{
				Include:        []string{class},
				Config:         helper.DefaultRestoreConfig(),
				OverwriteAlias: overwriteAlias,
			})
		_, err := helper.Client(t).Backups.BackupsRestore(params, helper.CreateAuth(malloryKey))
		if err != nil {
			var forbidden *backups.BackupsRestoreForbidden
			require.ErrorAs(t, err, &forbidden)
			return
		}
		helper.ExpectBackupEventuallyRestored(t, id, backend, helper.CreateAuth(adminKey))
	}

	t.Run("overwriteAlias keeps an alias the caller cannot update", func(t *testing.T) {
		const victimClass, scratchClass, aliasName = "CompanyOrders", "MalloryScratch", "Orders"
		for _, class := range []string{victimClass, scratchClass} {
			helper.CreateClassAuth(t, &models.Class{Class: class}, adminKey)
		}
		helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: scratchClass}, adminKey)
		id := backUp(t, scratchClass)
		helper.UpdateAliasWithAuthz(t, aliasName, victimClass, helper.CreateAuth(adminKey))

		restore(t, scratchClass, id, true)

		got := helper.GetAliasWithAuthz(t, aliasName, helper.CreateAuth(adminKey))
		assert.Equal(t, victimClass, got.Class)
	})

	t.Run("restore does not recreate an alias the caller cannot create", func(t *testing.T) {
		const archiveClass, aliasName = "MalloryArchive", "Invoices"
		helper.CreateClassAuth(t, &models.Class{Class: archiveClass}, adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: archiveClass}, adminKey)
		id := backUp(t, archiveClass)
		helper.DeleteAliasWithAuthz(t, aliasName, helper.CreateAuth(adminKey))

		restore(t, archiveClass, id, false)

		_, err := helper.GetAliasAuthWithReturn(t, aliasName, adminKey)
		var notFound *schema.AliasesGetAliasNotFound
		require.ErrorAs(t, err, &notFound)
	})
}
