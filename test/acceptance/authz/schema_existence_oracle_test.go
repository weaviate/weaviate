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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// A caller with no roles must not be able to tell a missing collection from a
// single-tenant or multi-tenant one through the tenants or aliases endpoints.
func TestAuthzSchemaExistenceOracle(t *testing.T) {
	const (
		adminKey  = "admin-key"
		noRoleKey = "custom-key"

		missingClass = "OracleMissing"
		singleClass  = "OracleSingleTenant"
		multiClass   = "OracleMultiTenant"
	)

	_, down := composeUpShared(t)
	defer down()

	helper.CreateClassAuth(t, &models.Class{Class: singleClass}, adminKey)
	helper.CreateClassAuth(t, &models.Class{
		Class:              multiClass,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}, adminKey)
	helper.CreateTenantsAuth(t, multiClass, []*models.Tenant{{Name: "T1"}, {Name: "T2"}}, adminKey)

	classes := []string{missingClass, singleClass, multiClass}

	t.Run("REST tenants list is identical for all three cases", func(t *testing.T) {
		for _, class := range classes {
			res, err := readTenants(t, class, noRoleKey)
			require.NoError(t, err, class)
			assert.Empty(t, res.Payload, class)
		}
	})

	t.Run("gRPC tenants list is identical for all three cases", func(t *testing.T) {
		for _, class := range classes {
			res, err := readTenantsGRPC(t, context.Background(), class, noRoleKey)
			require.NoError(t, err, class)
			assert.Empty(t, res.Tenants, class)
		}
	})

	t.Run("admin still sees the real errors", func(t *testing.T) {
		_, err := readTenants(t, missingClass, adminKey)
		require.Error(t, err)
		_, err = readTenants(t, singleClass, adminKey)
		require.Error(t, err)
		res, err := readTenants(t, multiClass, adminKey)
		require.NoError(t, err)
		assert.Len(t, res.Payload, 2)
	})

	t.Run("aliases filter by class is identical for missing and existing", func(t *testing.T) {
		for _, class := range classes {
			res, err := helper.GetAliasesAuthWithReturn(t, &class, noRoleKey)
			require.NoError(t, err, class)
			assert.Empty(t, res.Payload.Aliases, class)
		}
	})

	t.Run("get and delete of an alias the caller cannot read look like a missing alias", func(t *testing.T) {
		const alias = "OracleAlias"
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: singleClass}, adminKey)

		_, missingGet := helper.GetAliasAuthWithReturn(t, "OracleNoSuchAlias", noRoleKey)
		_, existingGet := helper.GetAliasAuthWithReturn(t, alias, noRoleKey)
		require.Error(t, missingGet)
		require.Error(t, existingGet)
		assert.Equal(t, missingGet.Error(), existingGet.Error())

		_, missingDel := helper.DeleteAliasAuthWithReturn(t, "OracleNoSuchAlias", noRoleKey)
		_, existingDel := helper.DeleteAliasAuthWithReturn(t, alias, noRoleKey)
		require.Error(t, missingDel)
		require.Error(t, existingDel)
		assert.Equal(t, missingDel.Error(), existingDel.Error())
	})
}
