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
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestAuthzAliasesHideForbiddenAlias pins that a caller without alias
// permissions cannot tell an existing alias from a missing one.
func TestAuthzAliasesHideForbiddenAlias(t *testing.T) {
	adminKey := "admin-key"
	noPermKey := "custom-key"
	readerUser := "read-user"
	readerKey := "read-key"
	readerRole := "alias-reader"

	compose, down := composeUpShared(t)
	defer down()

	target := articles.ParagraphsClass()
	helper.CreateClassAuth(t, target, adminKey)
	existing := "PublicView"
	missing := "Payroll"
	helper.CreateAliasAuth(t, &models.Alias{Alias: existing, Class: target.Class}, adminKey)

	do := func(t *testing.T, method, path, key string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().URI(), path), nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(body)
	}

	tests := []struct {
		name         string
		method       string
		existingPath string
		missingPath  string
		wantStatus   int
	}{
		{
			name:         "get alias",
			method:       http.MethodGet,
			existingPath: "/aliases/" + existing,
			missingPath:  "/aliases/" + missing,
			wantStatus:   http.StatusNotFound,
		},
		{
			name:         "delete alias",
			method:       http.MethodDelete,
			existingPath: "/aliases/" + existing,
			missingPath:  "/aliases/" + missing,
			wantStatus:   http.StatusNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existingStatus, existingBody := do(t, tt.method, tt.existingPath, noPermKey)
			missingStatus, missingBody := do(t, tt.method, tt.missingPath, noPermKey)

			assert.Equal(t, tt.wantStatus, missingStatus)
			assert.Equal(t, missingStatus, existingStatus)
			assert.Equal(t, missingBody, existingBody)
			assert.NotContains(t, existingBody, target.Class)
			helper.GetAliasWithAuthz(t, existing, helper.CreateAuth(adminKey))
		})
	}

	scopedReaders := []struct {
		name string
		user string
		key  string
		perm *models.Permission
	}{
		{
			name: "reader scoped to other aliases",
			user: "limited-user",
			key:  "limited-key",
			perm: helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithAlias("Other*").Permission(),
		},
		{
			name: "reader scoped to another collection",
			user: "write-user",
			key:  "write-key",
			perm: helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithCollection("OtherCollection").Permission(),
		},
	}
	for _, sr := range scopedReaders {
		t.Run(sr.name, func(t *testing.T) {
			role := "scoped-" + sr.user
			helper.CreateRole(t, adminKey, &models.Role{Name: &role, Permissions: []*models.Permission{sr.perm}})
			helper.AssignRoleToUser(t, adminKey, role, sr.user)

			for _, method := range []string{http.MethodGet, http.MethodDelete} {
				existingStatus, existingBody := do(t, method, "/aliases/"+existing, sr.key)
				missingStatus, missingBody := do(t, method, "/aliases/"+missing, sr.key)

				assert.Equal(t, http.StatusNotFound, missingStatus, method)
				assert.Equal(t, missingStatus, existingStatus, method)
				assert.Equal(t, missingBody, existingBody, method)
			}
			helper.GetAliasWithAuthz(t, existing, helper.CreateAuth(adminKey))
		})
	}

	t.Run("delete by a caller who may read the alias is forbidden", func(t *testing.T) {
		helper.CreateRole(t, adminKey, &models.Role{Name: &readerRole, Permissions: []*models.Permission{
			helper.NewAliasesPermission().WithAction(authorization.ReadAliases).Permission(),
		}})
		helper.AssignRoleToUser(t, adminKey, readerRole, readerUser)

		status, _ := do(t, http.MethodDelete, "/aliases/"+existing, readerKey)
		assert.Equal(t, http.StatusForbidden, status)
		status, _ = do(t, http.MethodDelete, "/aliases/"+missing, readerKey)
		assert.Equal(t, http.StatusNotFound, status)
		helper.GetAliasWithAuthz(t, existing, helper.CreateAuth(adminKey))
	})
}
