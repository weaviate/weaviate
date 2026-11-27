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
	"fmt"
	"io"
	"net/http"
	"strings"
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

	t.Run("caller without permission on the alias gets forbidden whether it exists or not", func(t *testing.T) {
		_, missingErr := helper.UpdateAliasAuthWithReturn(t, "NoSuchAlias", scratchClass, malloryKey)
		var missing *schema.AliasesUpdateForbidden
		require.ErrorAs(t, missingErr, &missing)

		_, err := helper.UpdateAliasAuthWithReturn(t, aliasName, scratchClass, malloryKey)
		var denied *schema.AliasesUpdateForbidden
		require.ErrorAs(t, err, &denied)
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
		require.ErrorAs(t, err, &forbidden)
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

// TestAuthzAliasesForbiddenWithoutPermission pins that a caller without
// permission gets the same 403 whether the alias exists or not, and a caller
// with permission gets 404 for a missing alias and the real answer otherwise.
func TestAuthzAliasesForbiddenWithoutPermission(t *testing.T) {
	adminKey := "admin-key"

	compose, down := composeUpShared(t)
	defer down()

	target := articles.ParagraphsClass()
	helper.CreateClassAuth(t, target, adminKey)
	existing := "PublicView"
	missing := "Payroll"
	helper.CreateAliasAuth(t, &models.Alias{Alias: existing, Class: target.Class}, adminKey)

	do := func(t *testing.T, method, alias, key string) (int, string) {
		t.Helper()
		var body io.Reader
		if method == http.MethodPut {
			body = strings.NewReader(fmt.Sprintf(`{"class":%q}`, target.Class))
		}
		url := fmt.Sprintf("http://%s/v1/aliases/%s", compose.GetWeaviate().URI(), alias)
		req, err := http.NewRequest(method, url, body)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(respBody)
	}

	allAliases := func(actions ...string) []*models.Permission {
		perms := make([]*models.Permission, len(actions))
		for i, action := range actions {
			perms[i] = helper.NewAliasesPermission().WithAction(action).Permission()
		}
		return perms
	}

	// want maps a method to its status for the existing and the missing alias.
	type statuses map[string][2]int
	forbiddenEverywhere := statuses{
		http.MethodGet:    {http.StatusForbidden, http.StatusForbidden},
		http.MethodPut:    {http.StatusForbidden, http.StatusForbidden},
		http.MethodDelete: {http.StatusForbidden, http.StatusForbidden},
	}
	tests := []struct {
		name  string
		user  string
		key   string
		perms []*models.Permission
		want  statuses
	}{
		{
			name: "no permissions",
			user: "custom-user",
			key:  "custom-key",
			want: forbiddenEverywhere,
		},
		{
			name: "permissions on other aliases",
			user: "limited-user",
			key:  "limited-key",
			perms: []*models.Permission{
				helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithAlias("Other*").Permission(),
				helper.NewAliasesPermission().WithAction(authorization.UpdateAliases).WithAlias("Other*").Permission(),
				helper.NewAliasesPermission().WithAction(authorization.DeleteAliases).WithAlias("Other*").Permission(),
			},
			want: forbiddenEverywhere,
		},
		{
			name: "permissions on another collection",
			user: "write-user",
			key:  "write-key",
			perms: []*models.Permission{
				helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithCollection("OtherCollection").Permission(),
				helper.NewAliasesPermission().WithAction(authorization.UpdateAliases).WithCollection("OtherCollection").Permission(),
				helper.NewAliasesPermission().WithAction(authorization.DeleteAliases).WithCollection("OtherCollection").Permission(),
			},
			want: forbiddenEverywhere,
		},
		{
			name:  "read on every alias",
			user:  "read-user",
			key:   "read-key",
			perms: allAliases(authorization.ReadAliases),
			want: statuses{
				http.MethodGet:    {http.StatusOK, http.StatusNotFound},
				http.MethodPut:    {http.StatusForbidden, http.StatusForbidden},
				http.MethodDelete: {http.StatusForbidden, http.StatusForbidden},
			},
		},
		{
			name: "read on the alias's collection only",
			user: "custom-user2",
			key:  "custom-key2",
			perms: []*models.Permission{
				helper.NewAliasesPermission().WithAction(authorization.ReadAliases).WithCollection(target.Class).Permission(),
			},
			want: statuses{
				http.MethodGet:    {http.StatusOK, http.StatusForbidden},
				http.MethodPut:    {http.StatusForbidden, http.StatusForbidden},
				http.MethodDelete: {http.StatusForbidden, http.StatusForbidden},
			},
		},
		{
			name:  "update and delete on every alias",
			user:  "test-user",
			key:   "test-key",
			perms: allAliases(authorization.UpdateAliases, authorization.DeleteAliases),
			want: statuses{
				http.MethodGet:    {http.StatusForbidden, http.StatusForbidden},
				http.MethodPut:    {http.StatusOK, http.StatusNotFound},
				http.MethodDelete: {http.StatusNoContent, http.StatusNotFound},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.perms) > 0 {
				role := "alias-" + tt.user
				helper.CreateRole(t, adminKey, &models.Role{Name: &role, Permissions: tt.perms})
				helper.AssignRoleToUser(t, adminKey, role, tt.user)
			}
			for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
				want := tt.want[method]
				existingStatus, existingBody := do(t, method, existing, tt.key)
				missingStatus, missingBody := do(t, method, missing, tt.key)

				assert.Equal(t, want[0], existingStatus, "%s existing: %s", method, existingBody)
				assert.Equal(t, want[1], missingStatus, "%s missing: %s", method, missingBody)
				if existingStatus == http.StatusForbidden && missingStatus == http.StatusForbidden {
					// The bodies differ only in the alias name the caller sent.
					assert.Equal(t, strings.ReplaceAll(missingBody, missing, existing), existingBody, method)
					if method != http.MethodPut { // PUT echoes the target the caller sent
						assert.NotContains(t, existingBody, target.Class, method)
					}
				}
				if method == http.MethodDelete && existingStatus == http.StatusNoContent {
					helper.CreateAliasAuth(t, &models.Alias{Alias: existing, Class: target.Class}, adminKey)
				}
			}
			helper.GetAliasWithAuthz(t, existing, helper.CreateAuth(adminKey))
		})
	}
}
