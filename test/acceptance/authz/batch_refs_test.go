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
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthZBatchRefAuthZCalls(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)

	compose, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{}, nil)
	defer down()

	containers := compose.Containers()
	require.Len(t, containers, 1) // started only one node

	// add classes with object
	className1 := "AuthZBatchObjREST1"
	className2 := "AuthZBatchObjREST2"
	deleteObjectClass(t, className1, adminAuth)
	deleteObjectClass(t, className2, adminAuth)
	defer deleteObjectClass(t, className1, adminAuth)
	defer deleteObjectClass(t, className2, adminAuth)

	c1 := &models.Class{
		Class: className1,
		Properties: []*models.Property{
			{
				Name:     "ref",
				DataType: []string{className2},
			},
		},
	}
	c2 := &models.Class{
		Class: className2,
		Properties: []*models.Property{
			{
				Name:     "prop2",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	require.Nil(t, createClass(t, c2, adminAuth))
	require.Nil(t, createClass(t, c1, adminAuth))

	// add an object to each class
	helper.CreateObjectAuth(t, &models.Object{ID: UUID1, Class: className1}, adminKey)
	helper.CreateObjectAuth(t, &models.Object{ID: UUID1, Class: className2}, adminKey)

	ls := newLogScanner(containers[0].Container())
	ls.GetAuthzLogs(t) // startup and object class creation logs that are irrelevant

	from := beaconStart + className1 + "/" + UUID1.String() + "/ref"
	to := beaconStart + UUID1

	var refs []*models.BatchReference
	for i := 0; i < 30; i++ {
		refs = append(refs, &models.BatchReference{
			From: strfmt.URI(from), To: strfmt.URI(to),
		})
	}

	params := batch.NewBatchReferencesCreateParams().WithBody(refs)
	res, err := helper.Client(t).Batch.BatchReferencesCreate(params, adminAuth)
	require.NoError(t, err)
	require.NotNil(t, res.Payload)

	authZlogs := ls.GetAuthzLogs(t)
	require.LessOrEqual(t, len(authZlogs), 4)
}

func TestAuthZBatchRefAuthZTenantFiltering(t *testing.T) {
	adminUser := "admin-user"
	adminKey := "admin-key"
	adminAuth := helper.CreateAuth(adminKey)
	customUser := "custom-user"
	customKey := "custom-key"
	customAuth := helper.CreateAuth(customKey)

	compose, down := composeUp(t, map[string]string{adminUser: adminKey}, map[string]string{customUser: customKey}, nil)
	defer down()

	containers := compose.Containers()
	require.Len(t, containers, 1) // started only one node

	// add classes with object
	className1 := "AuthZBatchRefREST1"
	className2 := "AuthZBatchRefREST2"
	defer deleteObjectClass(t, className1, adminAuth)
	defer deleteObjectClass(t, className2, adminAuth)

	c1 := &models.Class{
		Class: className1,
		Properties: []*models.Property{
			{
				Name:     "ref",
				DataType: []string{className2},
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	c2 := &models.Class{
		Class: className2,
		Properties: []*models.Property{
			{
				Name:     "prop2",
				DataType: schema.DataTypeText.PropString(),
			},
		},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}
	require.Nil(t, createClass(t, c2, adminAuth))
	require.Nil(t, createClass(t, c1, adminAuth))

	tenants := []*models.Tenant{{Name: "tenant1"}, {Name: "tenant2"}}
	require.Nil(t, createTenant(t, className1, tenants, adminKey))
	require.Nil(t, createTenant(t, className2, tenants, adminKey))

	// add an object to each class
	helper.CreateObjectAuth(t, &models.Object{ID: UUID1, Class: className1}, adminKey)
	helper.CreateObjectAuth(t, &models.Object{ID: UUID1, Class: className2}, adminKey)

	addReferences := func(class, tenant string) (*batch.BatchReferencesCreateOK, error) {
		from := beaconStart + class + "/" + UUID1.String() + "/ref"
		to := beaconStart + UUID1

		var refs []*models.BatchReference
		for i := 0; i < 30; i++ {
			refs = append(refs, &models.BatchReference{
				From: strfmt.URI(from), To: strfmt.URI(to), Tenant: tenant,
			})
		}

		params := batch.NewBatchReferencesCreateParams().WithBody(refs)
		return helper.Client(t).Batch.BatchReferencesCreate(params, customAuth)
	}

	assertError := func(err error, expected *batch.BatchReferencesCreateForbidden) {
		require.Error(t, err)
		if !errors.As(err, &expected) {
			t.Fatalf("expected error of type %T, got %T: %v", expected, err, err)
		}
		require.ErrorAs(t, err, &expected)
	}
	var errForbidden *batch.BatchReferencesCreateForbidden

	t.Run("Fail to batch references without any permissions", func(t *testing.T) {
		_, err := addReferences(className1, tenants[0].Name)
		assertError(err, errForbidden)
	})

	permissions := batchReferencesPermissions(className1, className2, tenants[0].Name)
	role := &models.Role{
		Name:        String("test-role"),
		Permissions: permissions,
	}
	helper.CreateRole(t, adminKey, role)
	helper.AssignRoleToUser(t, adminKey, *role.Name, customUser)

	t.Run(fmt.Sprintf("Succeed to batch references from %s to %s within %s", className1, className2, tenants[0].Name), func(t *testing.T) {
		_, err := addReferences(className1, tenants[0].Name)
		require.NoError(t, err)
	})

	t.Run(fmt.Sprintf("Fail to batch from %s to %s references within %s", className1, className2, tenants[1].Name), func(t *testing.T) {
		_, err := addReferences(className1, tenants[1].Name)
		assertError(err, errForbidden)
	})

	helper.RemovePermissions(t, adminKey, *role.Name, permissions[2])
	t.Run(fmt.Sprintf("Fail to batch from %s to %s references within %s due to missing %s", className1, className2, tenants[0].Name, authorization.ReadData), func(t *testing.T) {
		_, err := addReferences(className1, tenants[0].Name)
		assertError(err, errForbidden)
	})
	helper.AddPermissions(t, adminKey, *role.Name, permissions[2])

	helper.RemovePermissions(t, adminKey, *role.Name, permissions[1])
	t.Run(fmt.Sprintf("Fail to batch from %s to %s references within %s due to missing %s", className1, className2, tenants[0].Name, authorization.UpdateData), func(t *testing.T) {
		_, err := addReferences(className1, tenants[0].Name)
		assertError(err, errForbidden)
	})
	helper.AddPermissions(t, adminKey, *role.Name, permissions[1])
}
