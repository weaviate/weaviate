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

package filter_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		Name   string
		Config rbacconf.Config
		Items  []*models.Object
	}{
		{
			Name:   "rbac enabled, no objects",
			Items:  []*models.Object{},
			Config: rbacconf.Config{Enabled: true},
		},
		{
			Name:   "rbac disenabled, no objects",
			Items:  []*models.Object{},
			Config: rbacconf.Config{Enabled: false},
		},
	}

	authorizer := mocks.NewMockAuthorizer()
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			resourceFilter := filter.New[*models.Object](authorizer, tt.Config)
			filteredObjects := resourceFilter.Filter(
				context.Background(),
				&models.Principal{Username: "user"},
				tt.Items,
				authorization.READ,
				func(obj *models.Object) string {
					return ""
				},
			)

			require.Equal(t, len(tt.Items), len(filteredObjects))
		})
	}
}

// TestFilterSameParentShortcut runs the filter against real RBAC roles. The
// shortcut, one check on the shared parent, must not let a tenant grant cover
// the collection itself.
func TestFilterSameParentShortcut(t *testing.T) {
	const user, roleName = "user", "filter-test-role"
	cls := "Cls"
	tenantGrant := func(tenant string) *models.Permission {
		return &models.Permission{
			Action:  &authorization.ReadTenants,
			Tenants: &models.PermissionTenants{Collection: &cls, Tenant: &tenant},
		}
	}
	collectionGrant := &models.Permission{
		Action:      &authorization.ReadCollections,
		Collections: &models.PermissionCollections{Collection: &cls},
	}

	tests := []struct {
		name  string
		grant *models.Permission
		items []string
		want  []string
	}{
		{
			name:  "grant on all tenants does not cover the collection",
			grant: tenantGrant("*"),
			items: authorization.CollectionsMetadata("Cls"),
			want:  []string{},
		},
		{
			name:  "grant on all tenants covers every tenant",
			grant: tenantGrant("*"),
			items: authorization.ShardsMetadata("Cls", "T1", "T2"),
			want:  authorization.ShardsMetadata("Cls", "T1", "T2"),
		},
		{
			name:  "grant on all tenants covers the tenant but not the collection in a mixed list",
			grant: tenantGrant("*"),
			items: append(authorization.ShardsMetadata("Cls", "T1"), authorization.CollectionsMetadata("Cls")...),
			want:  authorization.ShardsMetadata("Cls", "T1"),
		},
		{
			name:  "grant on one tenant covers only that tenant",
			grant: tenantGrant("T1"),
			items: authorization.ShardsMetadata("Cls", "T1", "T2"),
			want:  authorization.ShardsMetadata("Cls", "T1"),
		},
		{
			name:  "grant on one tenant does not cover the collection",
			grant: tenantGrant("T1"),
			items: authorization.CollectionsMetadata("Cls"),
			want:  []string{},
		},
		{
			name:  "collection grant covers the collection",
			grant: collectionGrant,
			items: authorization.CollectionsMetadata("Cls"),
			want:  authorization.CollectionsMetadata("Cls"),
		},
		{
			name:  "collection grant covers only its own collection",
			grant: collectionGrant,
			items: authorization.CollectionsMetadata("Cls", "Other"),
			want:  authorization.CollectionsMetadata("Cls"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			authN := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{user}}}
			m, err := rbac.New(filepath.Join(t.TempDir(), "policy.csv"), rbacconf.Config{Enabled: true}, authN, false, nil, logger)
			require.NoError(t, err)
			name := roleName
			policies, err := conv.RolesToPolicies(&models.Role{Name: &name, Permissions: []*models.Permission{tt.grant}})
			require.NoError(t, err)
			require.NoError(t, m.UpdateRolesPermissions(policies))
			require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId(user, authentication.AuthTypeDb), []string{roleName}))

			got := filter.New[string](m, rbacconf.Config{Enabled: true}).Filter(
				context.Background(),
				&models.Principal{Username: user, UserType: models.UserTypeInputDb},
				tt.items,
				authorization.READ,
				func(resource string) string { return resource },
			)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}
