//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package authz

import (
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
	adminUser := "admin-user"
	adminKey := "admin-key"

	customUser := "custom-user"
	customKey := "custom-key"
	limitedUser := "limited-user"
	limitedKey := "limited-key"
	customRole := "custom"
	onlyAliasesStartingWithAliasRole := "only-aliases"

	_, down := composeUp(t,
		map[string]string{adminUser: adminKey},
		map[string]string{customUser: customKey, limitedUser: limitedKey},
		nil,
	)
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
