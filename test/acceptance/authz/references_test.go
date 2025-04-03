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

package authz

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestAuthZReferencesOperations(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	// adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)

	_, teardown := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer teardown()

	tenants := []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}}

	paragraphsCls := articles.ParagraphsClass()
	paragraphsCls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}
	helper.CreateClassAuth(t, paragraphsCls, adminKey)
	helper.CreateTenantsAuth(t, paragraphsCls.Class, tenants, adminKey)

	articlesCls := articles.ArticlesClass()
	articlesCls.MultiTenancyConfig = &models.MultiTenancyConfig{
		Enabled: true,
	}
	helper.CreateClassAuth(t, articlesCls, adminKey)
	helper.CreateTenantsAuth(t, articlesCls.Class, tenants, adminKey)
	refProp := "hasParagraphs"

	roleName := "ref-ops-test"

	paragraphObjs := make([]*models.Object, 0)
	paragraphObjs = append(paragraphObjs, articles.NewParagraph().WithID(UUID1).WithTenant(tenants[0].Name).Object())
	paragraphObjs = append(paragraphObjs, articles.NewParagraph().WithID(UUID2).WithTenant(tenants[1].Name).Object())
	helper.CreateObjectsBatchAuth(t, paragraphObjs, adminKey)

	ref := &models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + UUID1.String())}

	articleObjs := make([]*models.Object, 0)
	articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 1").WithTenant(tenants[0].Name).Object())
	articleObjs = append(articleObjs, articles.NewArticle().WithTitle("Article 2").WithTenant(tenants[1].Name).Object())
	helper.CreateObjectsBatchAuth(t, articleObjs, adminKey)

	addRefInTenant1 := func() (*objects.ObjectsClassReferencesCreateOK, error) {
		return helper.AddReferenceReturn(t, ref, articleObjs[0].ID, articleObjs[0].Class, refProp, tenants[0].Name, customAuth)
	}

	replaceRefInTenant1 := func() (*objects.ObjectsClassReferencesPutOK, error) {
		return helper.ReplaceReferencesReturn(t, []*models.SingleRef{ref}, articleObjs[0].ID, articleObjs[0].Class, refProp, tenants[0].Name, customAuth)
	}

	deleteRefInTenant1 := func() (*objects.ObjectsClassReferencesDeleteNoContent, error) {
		return helper.DeleteReferenceReturn(t, ref, articleObjs[0].ID, articleObjs[0].Class, refProp, tenants[0].Name, customAuth)
	}

	t.Run("Reference create (POST)", func(t *testing.T) {
		var forbidden *objects.ObjectsClassReferencesCreateForbidden
		t.Run("fails with no permissions", func(t *testing.T) {
			_, err := addRefInTenant1()
			require.ErrorAs(t, err, &forbidden)
		})

		t.Run(fmt.Sprintf("succeeds with permissions on %s only", tenants[0].Name), func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: addReferencePermissions(articlesCls.Class, paragraphsCls.Class, tenants[0].Name),
			}
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer func() {
				helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
				helper.DeleteRole(t, adminKey, roleName)
			}()
			_, err := addRefInTenant1()
			require.NoError(t, err)
		})

		t.Run(fmt.Sprintf("fails with permissions on %s only", tenants[1].Name), func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: addReferencePermissions(articlesCls.Class, paragraphsCls.Class, tenants[1].Name),
			}
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer func() {
				helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
				helper.DeleteRole(t, adminKey, roleName)
			}()
			_, err := addRefInTenant1()
			require.ErrorAs(t, err, &forbidden)
		})
	})

	t.Run("Reference replace (PUT)", func(t *testing.T) {
		var forbidden *objects.ObjectsClassReferencesPutForbidden
		t.Run("fails with no permissions", func(t *testing.T) {
			_, err := replaceRefInTenant1()
			require.ErrorAs(t, err, &forbidden)
		})

		t.Run(fmt.Sprintf("succeeds with permissions on %s only", tenants[0].Name), func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: addReferencePermissions(articlesCls.Class, paragraphsCls.Class, tenants[0].Name),
			}
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer func() {
				helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
				helper.DeleteRole(t, adminKey, roleName)
			}()
			_, err := replaceRefInTenant1()
			require.NoError(t, err)
		})

		t.Run(fmt.Sprintf("fails with permissions on %s only", tenants[1].Name), func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: addReferencePermissions(articlesCls.Class, paragraphsCls.Class, tenants[1].Name),
			}
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer func() {
				helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
				helper.DeleteRole(t, adminKey, roleName)
			}()
			_, err := replaceRefInTenant1()
			require.ErrorAs(t, err, &forbidden)
		})
	})

	t.Run("Reference delete (DELETE)", func(t *testing.T) {
		var forbidden *objects.ObjectsClassReferencesDeleteForbidden
		t.Run("fails with no permissions", func(t *testing.T) {
			_, err := deleteRefInTenant1()
			require.ErrorAs(t, err, &forbidden)
		})

		t.Run(fmt.Sprintf("succeeds with permissions on %s only", tenants[0].Name), func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: deleteReferencePermissions(articlesCls.Class, paragraphsCls.Class, tenants[0].Name),
			}
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer func() {
				helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
				helper.DeleteRole(t, adminKey, roleName)
			}()
			_, err := deleteRefInTenant1()
			require.NoError(t, err)
		})

		t.Run(fmt.Sprintf("fails with permissions on %s only", tenants[1].Name), func(t *testing.T) {
			role := &models.Role{
				Name:        &roleName,
				Permissions: deleteReferencePermissions(articlesCls.Class, paragraphsCls.Class, tenants[1].Name),
			}
			helper.CreateRole(t, adminKey, role)
			helper.AssignRoleToUser(t, adminKey, roleName, customUser)
			defer func() {
				helper.RevokeRoleFromUser(t, adminKey, roleName, customUser)
				helper.DeleteRole(t, adminKey, roleName)
			}()
			_, err := deleteRefInTenant1()
			require.ErrorAs(t, err, &forbidden)
		})
	})
}
