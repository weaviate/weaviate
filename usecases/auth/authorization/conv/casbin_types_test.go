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

package conv

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type innerTest struct {
	permissionAction string
	testDescription  string
	policyVerb       string
}

var (
	foo = authorization.String("Foo")
	bar = authorization.String("bar")
	baz = authorization.String("baz")

	createDesc = "create"
	readDesc   = "read"
	updateDesc = "update"
	deleteDesc = "delete"
	manageDesc = "manage"

	createVerb              = authorization.CREATE
	readVerb                = authorization.READ
	updateVerb              = authorization.UPDATE
	deleteVerb              = authorization.DELETE
	assignAndRevokeUserVerb = authorization.USER_ASSIGN_AND_REVOKE
	manageVerb              = CRUD

	rolesTestsR = []innerTest{
		{permissionAction: authorization.ReadRoles, testDescription: readDesc, policyVerb: authorization.VerbWithScope(readVerb, authorization.ROLE_SCOPE_MATCH)},
	}
	rolesTestsCUD = []innerTest{
		{permissionAction: authorization.CreateRoles, testDescription: createVerb, policyVerb: authorization.VerbWithScope(createVerb, authorization.ROLE_SCOPE_ALL)},
		{permissionAction: authorization.UpdateRoles, testDescription: updateDesc, policyVerb: authorization.VerbWithScope(updateVerb, authorization.ROLE_SCOPE_ALL)},
		{permissionAction: authorization.DeleteRoles, testDescription: deleteDesc, policyVerb: authorization.VerbWithScope(deleteVerb, authorization.ROLE_SCOPE_ALL)},
	}
	clusterTests = []innerTest{
		{permissionAction: authorization.ReadCluster, testDescription: readDesc, policyVerb: readVerb},
	}
	nodesTests = []innerTest{
		{permissionAction: authorization.ReadNodes, testDescription: readDesc, policyVerb: readVerb},
	}
	backupsTests = []innerTest{
		{permissionAction: authorization.ManageBackups, testDescription: manageDesc, policyVerb: manageVerb},
	}
	collectionsTests = []innerTest{
		{permissionAction: authorization.CreateCollections, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadCollections, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateCollections, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteCollections, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	objectsDataTests = []innerTest{
		{permissionAction: authorization.CreateData, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadData, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateData, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteData, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	tenantsActionTests = []innerTest{
		{permissionAction: authorization.CreateTenants, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadTenants, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateTenants, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteTenants, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	userTests = []innerTest{
		{permissionAction: authorization.AssignAndRevokeUsers, testDescription: manageDesc, policyVerb: assignAndRevokeUserVerb},
		{permissionAction: authorization.CreateUsers, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadUsers, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateUsers, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteUsers, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
)

func Test_policy(t *testing.T) {
	tests := []struct {
		name       string
		permission *models.Permission
		policy     *authorization.Policy
		tests      []innerTest
	}{
		{
			name: "all roles",
			permission: &models.Permission{
				Roles: &models.PermissionRoles{Role: authorization.All, Scope: authorization.String(models.PermissionRolesScopeMatch)},
			},
			policy: &authorization.Policy{
				Resource: CasbinRoles("*"),
				Domain:   authorization.RolesDomain,
				Verb:     authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH),
			},
			tests: rolesTestsR,
		},
		{
			name: "a role",
			permission: &models.Permission{
				Roles: &models.PermissionRoles{Role: authorization.String("admin"), Scope: authorization.String(models.PermissionRolesScopeMatch)},
			},
			policy: &authorization.Policy{
				Resource: CasbinRoles("admin"),
				Domain:   authorization.RolesDomain,
				Verb:     authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_MATCH),
			},
			tests: rolesTestsR,
		},
		{
			name: "a role with scope all",
			permission: &models.Permission{
				Roles: &models.PermissionRoles{Role: authorization.String("admin"), Scope: authorization.String(models.PermissionRolesScopeAll)},
			},
			policy: &authorization.Policy{
				Resource: CasbinRoles("admin"),
				Domain:   authorization.RolesDomain,
				Verb:     authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			},
			tests: rolesTestsCUD,
		},
		{
			name:       "cluster",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinClusters(),
				Domain:   authorization.ClusterDomain,
			},
			tests: clusterTests,
		},
		{
			name: "minimal nodes",
			permission: &models.Permission{
				Nodes: &models.PermissionNodes{
					Verbosity: authorization.String("minimal"),
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinNodes("minimal", "doesntmatter"),
				Domain:   authorization.NodesDomain,
			},
			tests: nodesTests,
		},
		{
			name: "verbose nodes for all collections",
			permission: &models.Permission{
				Nodes: &models.PermissionNodes{
					Verbosity:  authorization.String("verbose"),
					Collection: authorization.All,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinNodes("verbose", "*"),
				Domain:   authorization.NodesDomain,
			},
			tests: nodesTests,
		},
		{
			name: "verbose nodes for one collections",
			permission: &models.Permission{
				Nodes: &models.PermissionNodes{
					Verbosity:  authorization.String("verbose"),
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinNodes("verbose", "Foo"),
				Domain:   authorization.NodesDomain,
			},
			tests: nodesTests,
		},
		{
			name: "all backends",
			permission: &models.Permission{
				Backups: &models.PermissionBackups{},
			},
			policy: &authorization.Policy{
				Resource: CasbinBackups("*"),
				Domain:   authorization.BackupsDomain,
			},
			tests: backupsTests,
		},
		{
			name: "a backend",
			permission: &models.Permission{
				Backups: &models.PermissionBackups{
					Collection: authorization.String("ABC"),
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinBackups("ABC"),
				Domain:   authorization.BackupsDomain,
			},
			tests: backupsTests,
		},
		{
			name: "all collections",
			permission: &models.Permission{
				Collections: &models.PermissionCollections{},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "#"),
				Domain:   authorization.SchemaDomain,
			},
			tests: collectionsTests,
		},
		{
			name: "a collection",
			permission: &models.Permission{
				Collections: &models.PermissionCollections{
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("Foo", "#"),
				Domain:   authorization.SchemaDomain,
			},
			tests: collectionsTests,
		},
		{
			name: "all tenants in all collections",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "*"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "all tenants in a collection",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("Foo", "*"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "a tenant in all collections",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Tenant: bar,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "bar"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "a tenant in a collection",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
					Tenant:     bar,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("Foo", "bar"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "all objects in all collections ST",
			permission: &models.Permission{
				Data: &models.PermissionData{},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("*", "*", "*"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "all objects in a collection ST",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("Foo", "*", "*"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "an object in all collections ST",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Object: baz,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("*", "*", "baz"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "an object in a collection ST",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Object:     baz,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("Foo", "*", "baz"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "all objects in all tenants in a collection MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("Foo", "*", "*"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "all objects in a tenant in all collections MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Tenant: bar,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("*", "bar", "*"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "all objects in a tenant in a collection MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     bar,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("Foo", "bar", "*"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "an object in all tenants in all collections MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Object: baz,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("*", "*", "baz"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "an object in all tenants in a collection MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Object:     baz,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("Foo", "*", "baz"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "an object in a tenant in all collections MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Tenant: bar,
					Object: baz,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("*", "bar", "baz"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "an object in a tenant in a collection MT",
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     bar,
					Object:     baz,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinData("Foo", "bar", "baz"),
				Domain:   authorization.DataDomain,
			},
			tests: objectsDataTests,
		},
		{
			name: "a tenant",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("Foo", ""),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "all tenants in all collections",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "*"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "all tenants in a collection",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("Foo", "*"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "a tenant in all collections",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Tenant: bar,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "bar"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
		{
			name: "a tenant in a collection",
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
					Tenant:     bar,
				},
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("Foo", "bar"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsActionTests,
		},
	}
	for _, tt := range tests {
		for _, ttt := range tt.tests {
			t.Run(fmt.Sprintf("%s %s", ttt.testDescription, tt.name), func(t *testing.T) {
				tt.permission.Action = authorization.String(ttt.permissionAction)
				tt.policy.Verb = ttt.policyVerb

				policy, err := policy(tt.permission)
				require.Nil(t, err)
				require.Equal(t, tt.policy, policy)
			})
		}
	}
}

func Test_permission(t *testing.T) {
	tests := []struct {
		name       string
		policy     []string
		permission *models.Permission
		tests      []innerTest
	}{
		{
			name:   "all roles",
			policy: []string{"p", "/*", "", authorization.RolesDomain},
			permission: &models.Permission{
				Roles: &models.PermissionRoles{Role: authorization.String("*"), Scope: authorization.String(models.PermissionRolesScopeMatch)},
			},
			tests: rolesTestsR,
		},
		{
			name:   "all roles",
			policy: []string{"p", "/*", authorization.ROLE_SCOPE_MATCH, authorization.RolesDomain},
			permission: &models.Permission{
				Roles: authorization.AllRoles,
			},
			tests: rolesTestsCUD,
		},
		{
			name:   "a role",
			policy: []string{"p", "/custom", authorization.ROLE_SCOPE_MATCH, authorization.RolesDomain},
			permission: &models.Permission{
				Roles: &models.PermissionRoles{Role: authorization.String("custom"), Scope: authorization.String(models.PermissionRolesScopeMatch)},
			},
			tests: rolesTestsR,
		},
		{
			name:   "a role",
			policy: []string{"p", "/custom", authorization.ROLE_SCOPE_MATCH, authorization.RolesDomain},
			permission: &models.Permission{
				Roles: &models.PermissionRoles{Role: authorization.String("custom"), Scope: authorization.String(models.PermissionRolesScopeAll)},
			},
			tests: rolesTestsCUD,
		},
		{
			name:   "all users",
			policy: []string{"p", "/*", "", authorization.UsersDomain},
			permission: &models.Permission{
				Users: authorization.AllUsers,
			},
			tests: userTests,
		},
		{
			name:       "cluster",
			policy:     []string{"p", "/*", "", authorization.ClusterDomain},
			permission: &models.Permission{},
			tests:      clusterTests,
		},
		{
			name:   "minimal nodes",
			policy: []string{"p", "/verbosity/minimal", "", authorization.NodesDomain},
			permission: &models.Permission{
				Nodes: &models.PermissionNodes{
					Verbosity: authorization.String("minimal"),
				},
			},
			tests: nodesTests,
		},
		{
			name:   "verbose nodes over all collections",
			policy: []string{"p", "/verbosity/verbose/collections/*", "", authorization.NodesDomain},
			permission: &models.Permission{
				Nodes: &models.PermissionNodes{
					Collection: authorization.All,
					Verbosity:  authorization.String("verbose"),
				},
			},
			tests: nodesTests,
		},
		{
			name:   "verbose nodes over one collection",
			policy: []string{"p", "/verbosity/verbose/collections/Foo", "", authorization.NodesDomain},
			permission: &models.Permission{
				Nodes: &models.PermissionNodes{
					Collection: authorization.String("Foo"),
					Verbosity:  authorization.String("verbose"),
				},
			},
			tests: nodesTests,
		},
		{
			name:   "all tenants",
			policy: []string{"p", "/collections/*/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Tenants: authorization.AllTenants,
			},
			tests: tenantsActionTests,
		},
		{
			name:   "a tenant",
			policy: []string{"p", "/collections/Foo/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
					Tenant:     authorization.All,
				},
			},
			tests: tenantsActionTests,
		},
		{
			name:   "backup all collections",
			policy: []string{"p", "/collections/*", "", "backups"},
			permission: &models.Permission{
				Backups: authorization.AllBackups,
			},
			tests: backupsTests,
		},
		{
			name:   "a collection ABC",
			policy: []string{"p", "/collections/ABC", "", "backups"},
			permission: &models.Permission{
				Backups: &models.PermissionBackups{
					Collection: authorization.String("ABC"),
				},
			},
			tests: backupsTests,
		},
		{
			name:   "all collections",
			policy: []string{"p", "/collections/*/shards/#", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collections: authorization.AllCollections,
			},
			tests: collectionsTests,
		},
		{
			name:   "a collection",
			policy: []string{"p", "/collections/Foo/shards/#", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collections: &models.PermissionCollections{
					Collection: foo,
				},
			},
			tests: collectionsTests,
		},
		{
			name:   "all tenants in all collections",
			policy: []string{"p", "/collections/*/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Tenants: authorization.AllTenants,
			},
			tests: tenantsActionTests,
		},
		{
			name:   "all tenants in a collection",
			policy: []string{"p", "/collections/Foo/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
					Tenant:     authorization.All,
				},
			},
			tests: tenantsActionTests,
		},
		{
			name:   "a tenant in all collections",
			policy: []string{"p", "/collections/*/shards/bar", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: authorization.All,
					Tenant:     bar,
				},
			},
			tests: tenantsActionTests,
		},
		{
			name:   "a tenant in a collection",
			policy: []string{"p", "/collections/Foo/shards/bar", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Tenants: &models.PermissionTenants{
					Collection: foo,
					Tenant:     bar,
				},
			},
			tests: tenantsActionTests,
		},
		{
			name:   "all objects in all collections ST",
			policy: []string{"p", "/collections/*/shards/*/objects/*", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: authorization.All,
					Tenant:     authorization.All,
					Object:     authorization.All,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "all objects in a collection ST",
			policy: []string{"p", "/collections/Foo/shards/*/objects/*", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     authorization.All,
					Object:     authorization.All,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "an object in all collections ST",
			policy: []string{"p", "/collections/*/shards/*/objects/baz", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: authorization.All,
					Tenant:     authorization.All,
					Object:     baz,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "an object in a collection ST",
			policy: []string{"p", "/collections/Foo/shards/*/objects/baz", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     authorization.All,
					Object:     baz,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "all objects in all tenants in all collections MT",
			policy: []string{"p", "/collections/*/shards/*/objects/*", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: authorization.AllData,
			},
			tests: objectsDataTests,
		},
		{
			name:   "all objects in all tenants in a collection MT",
			policy: []string{"p", "/collections/Foo/shards/*/objects/*", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     authorization.All,
					Object:     authorization.All,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "all objects in a tenant in all collections MT",
			policy: []string{"p", "/collections/*/shards/bar/objects/*", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: authorization.All,
					Tenant:     bar,
					Object:     authorization.All,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "all objects in a tenant in a collection MT",
			policy: []string{"p", "/collections/Foo/shards/bar/objects/*", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     bar,
					Object:     authorization.All,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "an object in all tenants in all collections MT",
			policy: []string{"p", "/collections/*/shards/*/objects/baz", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: authorization.All,
					Tenant:     authorization.All,
					Object:     baz,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "an object in all tenants in a collection MT",
			policy: []string{"p", "/collections/Foo/shards/*/objects/baz", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     authorization.All,
					Object:     baz,
				},
			},
		},
		{
			name:   "an object in a tenant in all collections MT",
			policy: []string{"p", "/collections/*/shards/bar/objects/baz", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: authorization.All,
					Tenant:     bar,
					Object:     baz,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "an object in a tenant in a collection MT",
			policy: []string{"p", "/collections/Foo/shards/bar/objects/baz", "", authorization.DataDomain},
			permission: &models.Permission{
				Data: &models.PermissionData{
					Collection: foo,
					Tenant:     bar,
					Object:     baz,
				},
			},
			tests: objectsDataTests,
		},
		{
			name:   "a user",
			policy: []string{"p", "/baz", "", authorization.UsersDomain},
			permission: &models.Permission{
				Users: &models.PermissionUsers{
					Users: baz,
				},
			},
			tests: userTests,
		},
		{
			name:   "all users",
			policy: []string{"p", "/*", "", authorization.UsersDomain},
			permission: &models.Permission{
				Users: &models.PermissionUsers{
					Users: authorization.All,
				},
			},
			tests: userTests,
		},
	}
	for _, tt := range tests {
		tt.policy[1] = fmt.Sprintf("%s%s", tt.policy[3], tt.policy[1])
		for _, ttt := range tt.tests {
			t.Run(fmt.Sprintf("%s %s", ttt.testDescription, tt.name), func(t *testing.T) {
				policyForTest := make([]string, len(tt.policy))
				copy(policyForTest, tt.policy)
				tt.permission.Action = authorization.String(ttt.permissionAction)
				// TODO-RBAC : this test has to be rewritten and consider scopes
				if policyForTest[2] == authorization.ROLE_SCOPE_MATCH {
					policyForTest[2] = ttt.policyVerb + "_" + authorization.ROLE_SCOPE_MATCH
				} else {
					policyForTest[2] = ttt.policyVerb
				}
				permission, err := permission(policyForTest, true)
				require.Nil(t, err)
				require.Equal(t, tt.permission, permission)
			})
		}
	}
}

func Test_pUsers(t *testing.T) {
	tests := []struct {
		user     string
		expected string
	}{
		{user: "", expected: fmt.Sprintf("%s/.*", authorization.UsersDomain)},
		{user: "*", expected: fmt.Sprintf("%s/.*", authorization.UsersDomain)},
		{user: "foo", expected: fmt.Sprintf("%s/foo", authorization.UsersDomain)},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("user: %s", tt.user)
		t.Run(name, func(t *testing.T) {
			p := CasbinUsers(tt.user)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pRoles(t *testing.T) {
	tests := []struct {
		role     string
		expected string
	}{
		{role: "", expected: fmt.Sprintf("%s/.*", authorization.RolesDomain)},
		{role: "*", expected: fmt.Sprintf("%s/.*", authorization.RolesDomain)},
		{role: "foo", expected: fmt.Sprintf("%s/foo", authorization.RolesDomain)},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("role: %s", tt.role)
		t.Run(name, func(t *testing.T) {
			p := CasbinRoles(tt.role)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pCollections(t *testing.T) {
	tests := []struct {
		collection string
		expected   string
	}{
		{collection: "", expected: fmt.Sprintf("%s/collections/.*/shards/.*", authorization.SchemaDomain)},
		{collection: "*", expected: fmt.Sprintf("%s/collections/.*/shards/.*", authorization.SchemaDomain)},
		{collection: "foo", expected: fmt.Sprintf("%s/collections/Foo/shards/.*", authorization.SchemaDomain)},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s", tt.collection)
		t.Run(name, func(t *testing.T) {
			p := CasbinSchema(tt.collection, "")
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_CasbinShards(t *testing.T) {
	tests := []struct {
		collection string
		shard      string
		expected   string
	}{
		{collection: "", shard: "", expected: fmt.Sprintf("%s/collections/.*/shards/.*", authorization.SchemaDomain)},
		{collection: "*", shard: "*", expected: fmt.Sprintf("%s/collections/.*/shards/.*", authorization.SchemaDomain)},
		{collection: "foo", shard: "", expected: fmt.Sprintf("%s/collections/Foo/shards/.*", authorization.SchemaDomain)},
		{collection: "foo", shard: "*", expected: fmt.Sprintf("%s/collections/Foo/shards/.*", authorization.SchemaDomain)},
		{collection: "", shard: "bar", expected: fmt.Sprintf("%s/collections/.*/shards/bar", authorization.SchemaDomain)},
		{collection: "*", shard: "bar", expected: fmt.Sprintf("%s/collections/.*/shards/bar", authorization.SchemaDomain)},
		{collection: "foo", shard: "bar", expected: fmt.Sprintf("%s/collections/Foo/shards/bar", authorization.SchemaDomain)},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s", tt.collection, tt.shard)
		t.Run(name, func(t *testing.T) {
			p := CasbinSchema(tt.collection, tt.shard)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pObjects(t *testing.T) {
	tests := []struct {
		collection string
		shard      string
		object     string
		expected   string
	}{
		{collection: "", shard: "", object: "", expected: fmt.Sprintf("%s/collections/.*/shards/.*/objects/.*", authorization.DataDomain)},
		{collection: "*", shard: "*", object: "*", expected: fmt.Sprintf("%s/collections/.*/shards/.*/objects/.*", authorization.DataDomain)},
		{collection: "foo", shard: "", object: "", expected: fmt.Sprintf("%s/collections/Foo/shards/.*/objects/.*", authorization.DataDomain)},
		{collection: "foo", shard: "*", object: "*", expected: fmt.Sprintf("%s/collections/Foo/shards/.*/objects/.*", authorization.DataDomain)},
		{collection: "", shard: "bar", object: "", expected: fmt.Sprintf("%s/collections/.*/shards/bar/objects/.*", authorization.DataDomain)},
		{collection: "*", shard: "bar", object: "*", expected: fmt.Sprintf("%s/collections/.*/shards/bar/objects/.*", authorization.DataDomain)},
		{collection: "", shard: "", object: "baz", expected: fmt.Sprintf("%s/collections/.*/shards/.*/objects/baz", authorization.DataDomain)},
		{collection: "*", shard: "*", object: "baz", expected: fmt.Sprintf("%s/collections/.*/shards/.*/objects/baz", authorization.DataDomain)},
		{collection: "foo", shard: "bar", object: "", expected: fmt.Sprintf("%s/collections/Foo/shards/bar/objects/.*", authorization.DataDomain)},
		{collection: "foo", shard: "bar", object: "*", expected: fmt.Sprintf("%s/collections/Foo/shards/bar/objects/.*", authorization.DataDomain)},
		{collection: "foo", shard: "", object: "baz", expected: fmt.Sprintf("%s/collections/Foo/shards/.*/objects/baz", authorization.DataDomain)},
		{collection: "foo", shard: "*", object: "baz", expected: fmt.Sprintf("%s/collections/Foo/shards/.*/objects/baz", authorization.DataDomain)},
		{collection: "", shard: "bar", object: "baz", expected: fmt.Sprintf("%s/collections/.*/shards/bar/objects/baz", authorization.DataDomain)},
		{collection: "*", shard: "bar", object: "baz", expected: fmt.Sprintf("%s/collections/.*/shards/bar/objects/baz", authorization.DataDomain)},
		{collection: "foo", shard: "bar", object: "baz", expected: fmt.Sprintf("%s/collections/Foo/shards/bar/objects/baz", authorization.DataDomain)},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s; object: %s", tt.collection, tt.shard, tt.object)
		t.Run(name, func(t *testing.T) {
			p := CasbinData(tt.collection, tt.shard, tt.object)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_CasbinBackups(t *testing.T) {
	tests := []struct {
		backend  string
		expected string
	}{
		{backend: "", expected: fmt.Sprintf("%s/collections/.*", authorization.BackupsDomain)},
		{backend: "*", expected: fmt.Sprintf("%s/collections/.*", authorization.BackupsDomain)},
		{backend: "foo", expected: fmt.Sprintf("%s/collections/Foo", authorization.BackupsDomain)},
		{backend: "foo", expected: fmt.Sprintf("%s/collections/Foo", authorization.BackupsDomain)},
		{backend: "", expected: fmt.Sprintf("%s/collections/.*", authorization.BackupsDomain)},
		{backend: "*", expected: fmt.Sprintf("%s/collections/.*", authorization.BackupsDomain)},
		{backend: "foo", expected: fmt.Sprintf("%s/collections/Foo", authorization.BackupsDomain)},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("backend: %s", tt.backend)
		t.Run(name, func(t *testing.T) {
			p := CasbinBackups(tt.backend)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_fromCasbinResource(t *testing.T) {
	tests := []struct {
		resource string
		expected string
	}{
		{resource: "collections/.*/shards/.*/objects/.*", expected: "collections/*/shards/*/objects/*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("Resource: %s", tt.resource)
		t.Run(name, func(t *testing.T) {
			p := fromCasbinResource(tt.resource)
			require.Equal(t, tt.expected, p)
		})
	}
}

func TestValidResource(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid resource - users",
			input:    fmt.Sprintf("%s/testUser", authorization.UsersDomain),
			expected: true,
		},
		{
			name:     "valid resource - roles",
			input:    fmt.Sprintf("%s/testRole", authorization.RolesDomain),
			expected: true,
		},
		{
			name:     "valid resource - collections",
			input:    fmt.Sprintf("%s/collections/testCollection", authorization.SchemaDomain),
			expected: true,
		},
		{
			name:     "valid resource - objects",
			input:    fmt.Sprintf("%s/collections/testCollection/shards/testShard/objects/testObject", authorization.DataDomain),
			expected: true,
		},
		{
			name:     "invalid resource",
			input:    "invalid/resource",
			expected: false,
		},
		{
			name:     "invalid resource",
			input:    "some resource",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validResource(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestValidVerb(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid verb - create",
			input:    "C",
			expected: true,
		},
		{
			name:     "valid verb - read",
			input:    "R",
			expected: true,
		},
		{
			name:     "valid verb - update",
			input:    "U",
			expected: true,
		},
		{
			name:     "valid verb - delete",
			input:    "D",
			expected: true,
		},
		{
			name:     "All",
			input:    "CRUD",
			expected: true,
		},
		{
			name:     "invalid verb",
			input:    "X",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validVerb(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
