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

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// A guessed collection name, existing or not, must get the same answer.
func TestRefTargetRequiresRead(t *testing.T) {
	const (
		book     = "RefReadBook"
		film     = "RefReadFilm"
		article  = "RefReadArticle"
		payroll  = "RefReadPayroll"
		secret   = "RefReadSecret"
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
		{Class: secret, Properties: textProp},
		{Class: payroll, Properties: textProp, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
		{Class: article, Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
			{Name: "related", DataType: []string{book, film}},
		}},
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

	var (
		bookID    = strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b51")
		filmID    = strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b52")
		secretID  = strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b53")
		articleID = strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b54")
		missingID = strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b55")
	)
	for class, id := range map[string]strfmt.UUID{book: bookID, film: filmID, secret: secretID, article: articleID} {
		require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
			Class: class, ID: id, Properties: map[string]any{"title": "x"},
		}, adminKey))
	}

	roleName := t.Name() + "-role"
	readData, createData, updateData := authorization.ReadData, authorization.CreateData, authorization.UpdateData
	articleName, bookName := article, book
	helper.CreateRole(t, adminKey, &models.Role{
		Name: &roleName,
		Permissions: []*models.Permission{
			{Action: &readData, Data: &models.PermissionData{Collection: &articleName}},
			{Action: &createData, Data: &models.PermissionData{Collection: &articleName}},
			{Action: &updateData, Data: &models.PermissionData{Collection: &articleName}},
			{Action: &readData, Data: &models.PermissionData{Collection: &bookName}},
		},
	})
	helper.AssignRoleToUser(t, adminKey, roleName, userName)

	withRef := func(id strfmt.UUID, beacon string) *models.Object {
		return &models.Object{Class: article, ID: id, Properties: map[string]any{
			"related": []any{map[string]any{"beacon": beacon}},
		}}
	}
	// Each write returns the denial message, or "" when the write succeeded.
	writes := map[string]func(t *testing.T, beacon string) string{
		"validate": func(t *testing.T, beacon string) string {
			_, err := helper.Client(t).Objects.ObjectsValidate(
				objects.NewObjectsValidateParams().WithBody(withRef(missingID, beacon)), userAuth)
			var forbidden *objects.ObjectsValidateForbidden
			return denial(t, err, &forbidden, func() string { return forbidden.Payload.Error[0].Message })
		},
		"create": func(t *testing.T, beacon string) string {
			_, err := helper.Client(t).Objects.ObjectsCreate(
				objects.NewObjectsCreateParams().WithBody(withRef("", beacon)), userAuth)
			var forbidden *objects.ObjectsCreateForbidden
			return denial(t, err, &forbidden, func() string { return forbidden.Payload.Error[0].Message })
		},
		"replace": func(t *testing.T, beacon string) string {
			_, err := helper.Client(t).Objects.ObjectsClassPut(objects.NewObjectsClassPutParams().
				WithClassName(article).WithID(articleID).WithBody(withRef(articleID, beacon)), userAuth)
			var forbidden *objects.ObjectsClassPutForbidden
			return denial(t, err, &forbidden, func() string { return forbidden.Payload.Error[0].Message })
		},
		"patch": func(t *testing.T, beacon string) string {
			_, err := helper.Client(t).Objects.ObjectsClassPatch(objects.NewObjectsClassPatchParams().
				WithClassName(article).WithID(articleID).WithBody(withRef(articleID, beacon)), userAuth)
			var forbidden *objects.ObjectsClassPatchForbidden
			return denial(t, err, &forbidden, func() string { return forbidden.Payload.Error[0].Message })
		},
		"batch": func(t *testing.T, beacon string) string {
			res, err := helper.Client(t).Batch.BatchObjectsCreate(batch.NewBatchObjectsCreateParams().
				WithBody(batch.BatchObjectsCreateBody{Objects: []*models.Object{withRef("", beacon)}}), userAuth)
			require.NoError(t, err)
			if errs := res.Payload[0].Result.Errors; errs != nil {
				msg := errs.Error[0].Message
				require.Contains(t, msg, "forbidden", msg)
				return msg
			}
			return ""
		},
	}

	denied := map[string]string{
		"a target collection the caller cannot read":    "weaviate://localhost/" + film + "/" + filmID.String(),
		"a multi-tenant collection outside the targets": "weaviate://localhost/" + payroll + "/" + missingID.String(),
		"a collection that holds the id":                "weaviate://localhost/" + secret + "/" + secretID.String(),
		"a collection that does not exist":              "weaviate://localhost/RefReadNope/" + missingID.String(),
		"no class, without READ on every collection":    "weaviate://localhost/" + bookID.String(),
	}
	for writeName, write := range writes {
		for name, beacon := range denied {
			t.Run(writeName+": "+name+" is denied", func(t *testing.T) {
				msg := write(t, beacon)
				require.NotEmpty(t, msg, "write succeeded")
				require.NotContains(t, strings.ToLower(msg), "multi-tenancy", msg)
			})
		}
		t.Run(writeName+": a target collection the caller reads is allowed", func(t *testing.T) {
			require.Empty(t, write(t, "weaviate://localhost/"+book+"/"+bookID.String()))
		})
	}
}

// denial returns the 403 message err carries, "" for no error, and fails on any other error.
func denial[T any](t *testing.T, err error, target *T, message func() string) string {
	t.Helper()
	if err == nil {
		return ""
	}
	require.True(t, errors.As(err, target), "%v", err)
	return message()
}
