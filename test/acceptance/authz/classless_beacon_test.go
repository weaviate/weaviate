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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// Each request repeats because Go map order varies per search.
func TestClasslessBeaconDoesNotNameMultiTenantCollections(t *testing.T) {
	const (
		book     = "ClasslessBeaconBook"
		film     = "ClasslessBeaconFilm"
		article  = "ClasslessBeaconArticle"
		payroll  = "ClasslessBeaconPayroll"
		hrCases  = "ClasslessBeaconHRCases"
		repeats  = 30
		userName = "custom-user"
		userKey  = "custom-key"
	)
	adminKey := sharedRootKey
	adminAuth := helper.CreateAuth(adminKey)
	userAuth := helper.CreateAuth(userKey)

	_, down := composeUpShared(t)
	defer down()

	textProp := []*models.Property{{Name: "title", DataType: schema.DataTypeText.PropString()}}
	classes := []*models.Class{
		{Class: book, Properties: textProp},
		{Class: film, Properties: textProp},
		{Class: article, Properties: []*models.Property{{Name: "related", DataType: []string{book, film}}}},
		{Class: payroll, Properties: textProp, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
		{Class: hrCases, Properties: textProp, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
	}
	for _, c := range classes {
		helper.CreateClassAuth(t, c, adminKey)
	}
	defer func() {
		for _, c := range classes {
			deleteObjectClass(t, c.Class, adminAuth)
		}
	}()
	helper.CreateTenantsAuth(t, payroll, []*models.Tenant{{Name: "t1"}}, adminKey)
	helper.CreateTenantsAuth(t, hrCases, []*models.Tenant{{Name: "t1"}}, adminKey)

	bookID := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
		Class: book, ID: bookID, Properties: map[string]any{"title": "b"},
	}, adminKey))
	articleID := strfmt.UUID("0b1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e")
	require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
		Class: article, ID: articleID, Properties: map[string]any{},
	}, adminKey))

	roleName := t.Name() + "-role"
	readData, createData := authorization.ReadData, authorization.CreateData
	articleName, all := article, "*"
	// A class-less beacon needs READ on every collection.
	helper.CreateRole(t, adminKey, &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{
			{Action: &readData, Data: &models.PermissionData{Collection: &all}},
			{Action: &createData, Data: &models.PermissionData{Collection: &articleName}},
		},
	})
	helper.AssignRoleToUser(t, adminKey, roleName, userName)

	missingID := strfmt.UUID("9c8b7a6d-5e4f-4a3b-8c2d-1e0f9a8b7c6d")
	// Validate requires an id; the failed creates never store it.
	newID := strfmt.UUID("6d5c4b3a-2f1e-4d0c-9b8a-7f6e5d4c3b2a")
	withRef := func(id strfmt.UUID) *models.Object {
		return &models.Object{Class: article, ID: newID, Properties: map[string]any{
			"related": []any{map[string]any{"beacon": "weaviate://localhost/" + id.String()}},
		}}
	}
	requireNoLeak := func(t *testing.T, msg string) {
		t.Helper()
		lower := strings.ToLower(msg)
		require.NotContains(t, lower, strings.ToLower(payroll), msg)
		require.NotContains(t, lower, strings.ToLower(hrCases), msg)
		require.NotContains(t, lower, "multi-tenancy", msg)
	}

	t.Run("validate with a missing id names no collection", func(t *testing.T) {
		for range repeats {
			_, err := helper.Client(t).Objects.ObjectsValidate(
				objects.NewObjectsValidateParams().WithBody(withRef(missingID)), userAuth)
			var unprocessable *objects.ObjectsValidateUnprocessableEntity
			require.True(t, errors.As(err, &unprocessable), "%v", err)
			msg := unprocessable.Payload.Error[0].Message
			requireNoLeak(t, msg)
			require.Contains(t, msg, "no object with id", msg)
		}
	})

	t.Run("create with a missing id names no collection", func(t *testing.T) {
		for range repeats {
			_, err := helper.Client(t).Objects.ObjectsCreate(
				objects.NewObjectsCreateParams().WithBody(withRef(missingID)), userAuth)
			var unprocessable *objects.ObjectsCreateUnprocessableEntity
			require.True(t, errors.As(err, &unprocessable), "%v", err)
			msg := unprocessable.Payload.Error[0].Message
			requireNoLeak(t, msg)
			require.Contains(t, msg, "no object with id", msg)
		}
	})

	t.Run("validate with an existing id passes despite multi-tenant collections", func(t *testing.T) {
		for range repeats {
			_, err := helper.Client(t).Objects.ObjectsValidate(
				objects.NewObjectsValidateParams().WithBody(withRef(bookID)), userAuth)
			require.NoError(t, err)
		}
	})

	classless := &models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + bookID.String())}

	t.Run("add reference with an existing id passes despite multi-tenant collections", func(t *testing.T) {
		for range repeats {
			_, err := helper.AddReferenceReturn(t, classless, articleID, article, "related", "", adminAuth)
			require.NoError(t, err)
		}
	})

	t.Run("replace references with an existing id passes despite multi-tenant collections", func(t *testing.T) {
		for range repeats {
			_, err := helper.ReplaceReferencesReturn(t, []*models.SingleRef{classless}, articleID, article, "related", "", adminAuth)
			require.NoError(t, err)
		}
	})
}
