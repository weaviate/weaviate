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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZNearObjectBeacon(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	customUser := "custom-user"
	customKey := "custom-key"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviate().
		WithApiKey().WithUserApiKey(adminUser, adminKey).WithUserApiKey(customUser, customKey).
		WithRBAC().WithRbacRoots(adminUser).
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	papersClass := "AuthzNearObjectPapers"
	booksClass := "AuthzNearObjectBooks"
	paperID := strfmt.UUID("45000000-0000-0000-0000-000000000001")
	bookID := strfmt.UUID("45000000-0000-0000-0000-000000000002")

	t.Run("create both collections and their objects", func(t *testing.T) {
		for _, class := range []string{papersClass, booksClass} {
			helper.CreateClassAuth(t, &models.Class{
				Class:      class,
				Vectorizer: "none",
				Properties: []*models.Property{{Name: "label", DataType: []string{"text"}}},
			}, adminKey)
		}
		require.Nil(t, helper.CreateObjectAuth(t, &models.Object{
			Class:      papersClass,
			ID:         paperID,
			Properties: map[string]interface{}{"label": "paper"},
			Vector:     models.C11yVector{0.2, -0.4, 0.8, 0.4},
		}, adminKey))
		require.Nil(t, helper.CreateObjectAuth(t, &models.Object{
			Class:      booksClass,
			ID:         bookID,
			Properties: map[string]interface{}{"label": "book"},
			Vector:     models.C11yVector{1, 0, 0, 0},
		}, adminKey))
	})

	t.Run("assign a role limited to the books collection's data", func(t *testing.T) {
		role := &models.Role{
			Name: String("near-object-books-only"),
			Permissions: []*models.Permission{
				{
					Action:      String(authorization.ReadCollections),
					Collections: &models.PermissionCollections{Collection: String("*")},
				},
				{
					Action: String(authorization.ReadData),
					Data:   &models.PermissionData{Collection: String(booksClass)},
				},
				{
					Action: String(authorization.CreateData),
					Data:   &models.PermissionData{Collection: String(booksClass)},
				},
			},
		}
		helper.CreateRole(t, adminKey, role)
		helper.AssignRoleToUser(t, adminKey, *role.Name, customUser)
	})

	t.Run("direct read of an object in the papers collection is denied", func(t *testing.T) {
		_, err := helper.GetObjectAuth(t, papersClass, paperID, customKey, "vector")
		require.NotNil(t, err)
		var forbidden *objects.ObjectsClassGetForbidden
		require.True(t, errors.As(err, &forbidden))
	})

	crossBeacon := fmt.Sprintf("weaviate://localhost/%s/%s", papersClass, paperID)

	t.Run("Get with a cross-collection nearObject beacon is denied the same way", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(nearObject: {beacon: %q}) { label _additional { distance } } } }`,
			booksClass, crossBeacon)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.NotEmpty(t, resp.Payload.Errors)
		assert.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
		require.Nil(t, resp.Payload.Data["Get"].(map[string]interface{})[booksClass])
	})

	t.Run("Aggregate with a cross-collection nearObject beacon is denied", func(t *testing.T) {
		query := fmt.Sprintf(`{ Aggregate { %s(objectLimit: 1, nearObject: {beacon: %q}) { meta { count } } } }`,
			booksClass, crossBeacon)
		resp, err := queryGQL(t, query, customKey)
		require.Nil(t, err)
		require.NotEmpty(t, resp.Payload.Errors)
		assert.Contains(t, resp.Payload.Errors[0].Message, "forbidden")
	})

	t.Run("Get with a same-collection beacon keeps working", func(t *testing.T) {
		sameBeacon := fmt.Sprintf("weaviate://localhost/%s/%s", booksClass, bookID)
		query := fmt.Sprintf(`{ Get { %s(nearObject: {beacon: %q}) { label } } }`, booksClass, sameBeacon)
		resp := assertGQL(t, query, customKey)
		results := resp.Data["Get"].(map[string]interface{})[booksClass].([]interface{})
		require.Len(t, results, 1)
	})

	t.Run("Get with a same-collection nearObject id keeps working", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(nearObject: {id: %q}) { label } } }`, booksClass, bookID)
		resp := assertGQL(t, query, customKey)
		results := resp.Data["Get"].(map[string]interface{})[booksClass].([]interface{})
		require.Len(t, results, 1)
	})

	t.Run("a caller with data access to both collections keeps cross-collection beacons", func(t *testing.T) {
		query := fmt.Sprintf(`{ Get { %s(nearObject: {beacon: %q}) { label } } }`, booksClass, crossBeacon)
		resp := assertGQL(t, query, adminKey)
		results := resp.Data["Get"].(map[string]interface{})[booksClass].([]interface{})
		require.Len(t, results, 1)
	})
}
