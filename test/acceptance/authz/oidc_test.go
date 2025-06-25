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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/weaviate/mockoidc"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/authz"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestRbacWithOIDC(t *testing.T) {
	customKey := "custom-key"
	customUser := "custom-user"
	tests := []struct {
		name          string
		image         *docker.Compose
		nameCollision bool // same username for DB and OIDC
		onlyOIDC      bool
	}{
		{
			name: "RBAC with OIDC",
			image: docker.New().
				WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots("admin-user"),
			nameCollision: false,
			onlyOIDC:      true,
		},
		{
			name: "RBAC with OIDC and API key",
			image: docker.New().
				WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots("admin-user").
				WithApiKey().WithUserApiKey("other", "random-key"),
			nameCollision: false,
		},
		{
			name: "RBAC with OIDC and API key overlapping user names",
			image: docker.New().
				WithWeaviate().WithMockOIDC().
				WithRBAC().WithRbacRoots("admin-user").
				WithApiKey().WithUserApiKey("other", "random-key").
				WithApiKey().WithUserApiKey("custom-user", customKey),
			nameCollision: true,
		},
		{
			name: "RBAC with OIDC with certificate",
			image: docker.New().
				WithWeaviate().WithMockOIDCWithCertificate().WithRBAC().WithRbacRoots("admin-user"),
			nameCollision: false,
			onlyOIDC:      true,
		},
		{
			name: "RBAC with OIDC with certificate and API key",
			image: docker.New().
				WithWeaviate().WithMockOIDCWithCertificate().WithRBAC().WithRbacRoots("admin-user").
				WithApiKey().WithUserApiKey("other", "random-key"),
			nameCollision: false,
		},
		{
			name: "RBAC with OIDC with certificate and API key overlapping user names",
			image: docker.New().
				WithWeaviate().WithMockOIDCWithCertificate().
				WithRBAC().WithRbacRoots("admin-user").
				WithApiKey().WithUserApiKey("other", "random-key").
				WithApiKey().WithUserApiKey("custom-user", customKey),
			nameCollision: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			compose, err := test.image.Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()

			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()

			// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
			// description for details
			tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())
			tokenCustom, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

			// prepare roles to assign later
			all := "*"
			readSchemaAction := authorization.ReadCollections
			createSchemaAction := authorization.CreateCollections
			createSchemaRoleName := "createSchema"
			createSchemaRole := &models.Role{
				Name: &createSchemaRoleName,
				Permissions: []*models.Permission{
					{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
					{Action: &createSchemaAction, Collections: &models.PermissionCollections{Collection: &all}},
				},
			}
			helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)
			helper.CreateRole(t, tokenAdmin, createSchemaRole)
			defer helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)

			// custom-user does not have any roles/permissions
			err = createClass(t, &models.Class{Class: "testingOidc"}, helper.CreateAuth(tokenCustom))
			require.Error(t, err)
			var forbidden *clschema.SchemaObjectsCreateForbidden
			require.True(t, errors.As(err, &forbidden))

			// assigning to OIDC user
			helper.AssignRoleToUserOIDC(t, tokenAdmin, createSchemaRoleName, customUser)
			err = createClass(t, &models.Class{Class: "testingOidc"}, helper.CreateAuth(tokenCustom))
			require.NoError(t, err)

			// only OIDC user has role assigned
			rolesOIDC := helper.GetRolesForUserOIDC(t, customUser, tokenAdmin)
			require.Len(t, rolesOIDC, 1)

			if test.onlyOIDC || !test.nameCollision {
				// validation check for existence will fail
				_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(customUser).WithUserType(string(models.UserTypeInputDb)), helper.CreateAuth(tokenAdmin))
				require.Error(t, err)
				var notFound *authz.GetRolesForUserNotFound
				require.True(t, errors.As(err, &notFound))
			} else {
				rolesDB := helper.GetRolesForUser(t, customUser, tokenAdmin, true)
				require.Len(t, rolesDB, 0)
			}

			usersOidc := helper.GetUserForRolesBoth(t, createSchemaRoleName, tokenAdmin)
			require.Len(t, usersOidc, 1)
			if test.onlyOIDC || !test.nameCollision {
				_, err := helper.Client(t).Authz.GetRolesForUser(authz.NewGetRolesForUserParams().WithID(customUser).WithUserType(string(models.UserTypeInputDb)), helper.CreateAuth(tokenAdmin))
				require.Error(t, err)
				var notFound *authz.GetRolesForUserNotFound
				require.True(t, errors.As(err, &notFound))
			} else {
				usersDB := helper.GetUserForRoles(t, createSchemaRoleName, tokenAdmin)
				require.Len(t, usersDB, 0)
			}

			// assign role to non-existing user => no error (if OIDC is enabled)
			helper.AssignRoleToUserOIDC(t, tokenAdmin, createSchemaRoleName, "i-dont-exist")

			// only oidc root user, as api-keys are either not enabled or do not have a root user
			users := helper.GetUserForRolesBoth(t, "root", tokenAdmin)
			for _, user := range users {
				require.Equal(t, *user.UserType, models.UserTypeOutputOidc)
			}

			if test.nameCollision {
				// api key user does NOT have the rights, even though it has the same name
				err = createClass(t, &models.Class{Class: "testingApiKey"}, helper.CreateAuth(customKey))
				require.Error(t, err)
				var forbidden *clschema.SchemaObjectsCreateForbidden
				require.True(t, errors.As(err, &forbidden))

				helper.AssignRoleToUser(t, tokenAdmin, createSchemaRoleName, "custom-user")
				err = createClass(t, &models.Class{Class: "testingApiKey"}, helper.CreateAuth(customKey))
				require.NoError(t, err)
			}

			if test.onlyOIDC {
				// cannot assign/revoke to/from db users
				resp, err := helper.Client(t).Authz.AssignRoleToUser(
					authz.NewAssignRoleToUserParams().WithID("random-user").WithBody(authz.AssignRoleToUserBody{Roles: []string{createSchemaRoleName}, UserType: models.UserTypeInputDb}),
					helper.CreateAuth(tokenAdmin),
				)
				require.Nil(t, resp)
				require.Error(t, err)

				resp2, err := helper.Client(t).Authz.RevokeRoleFromUser(
					authz.NewRevokeRoleFromUserParams().WithID("random-user").WithBody(authz.RevokeRoleFromUserBody{Roles: []string{createSchemaRoleName}, UserType: models.UserTypeInputDb}),
					helper.CreateAuth(tokenAdmin),
				)
				require.Nil(t, resp2)
				require.Error(t, err)

				// no validation for deprecated path when OIDC is enabled:
				_, err = helper.Client(t).Authz.AssignRoleToUser(
					authz.NewAssignRoleToUserParams().WithID("random-user").WithBody(authz.AssignRoleToUserBody{Roles: []string{createSchemaRoleName}}),
					helper.CreateAuth(tokenAdmin),
				)
				require.NoError(t, err)

				_, err = helper.Client(t).Authz.RevokeRoleFromUser(
					authz.NewRevokeRoleFromUserParams().WithID("random-user").WithBody(authz.RevokeRoleFromUserBody{Roles: []string{createSchemaRoleName}}),
					helper.CreateAuth(tokenAdmin),
				)
				require.NoError(t, err)
			}
		})
	}
}

func TestRbacWithOIDCGroups(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		image *docker.Compose
	}{
		{
			name:  "without certificate",
			image: docker.New().WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots("admin-user"),
		},
		{
			name:  "with certificate",
			image: docker.New().WithWeaviate().WithMockOIDCWithCertificate().WithRBAC().WithRbacRoots("admin-user"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compose, err := test.image.Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()
			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()

			// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
			// description for details
			tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())
			tokenCustom, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

			// prepare roles to assign later
			className := strings.Replace(t.Name(), "/", "", 1) + "Class"
			readSchemaAction := authorization.ReadCollections
			createSchemaAction := authorization.CreateCollections
			createSchemaRoleName := "createSchema"
			createSchemaRole := &models.Role{
				Name: &createSchemaRoleName,
				Permissions: []*models.Permission{
					{Action: &readSchemaAction, Collections: &models.PermissionCollections{Collection: &className}},
					{Action: &createSchemaAction, Collections: &models.PermissionCollections{Collection: &className}},
				},
			}
			helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)
			helper.CreateRole(t, tokenAdmin, createSchemaRole)
			defer helper.DeleteRole(t, tokenAdmin, createSchemaRoleName)
			helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(tokenAdmin))

			// custom-user does not have any roles/permissions
			err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
			require.Error(t, err)
			var forbidden *clschema.SchemaObjectsCreateForbidden
			require.True(t, errors.As(err, &forbidden))

			// assigning role to group and now user has permission
			helper.AssignRoleToGroup(t, tokenAdmin, createSchemaRoleName, "custom-group")
			err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
			require.NoError(t, err)

			// delete class to test again after revocation
			helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(tokenAdmin))
			helper.RevokeRoleFromGroup(t, tokenAdmin, createSchemaRoleName, "custom-group")
			err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
			require.Error(t, err)
		})
	}
}

func TestRbacWithOIDCRootGroups(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		image *docker.Compose
	}{
		{
			name:  "without certificate",
			image: docker.New().WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots("admin-user"),
		},
		{
			name:  "with certificate",
			image: docker.New().WithWeaviate().WithMockOIDCWithCertificate().WithRBAC().WithRbacRoots("admin-user"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compose, err := test.image.WithRbacRootGroups("custom-group").Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()
			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()

			// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
			// description for details
			tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())
			tokenCustom, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

			className := strings.Replace(t.Name(), "/", "", 1) + "Class"
			helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(tokenAdmin))

			// custom user can create collection without any extra roles, because of membership in root group
			err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
			require.NoError(t, err)
		})
	}
}

func TestRbacWithOIDCViewerGroups(t *testing.T) {
	ctx := context.Background()
	image := docker.New().WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots("admin-user")

	compose, err := image.WithRbacViewerGroups("custom-group").Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
	// description for details
	tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())
	tokenCustom, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

	className := strings.Replace(t.Name(), "/", "", 1) + "Class"
	helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(tokenAdmin))

	// only viewer rights => custom user can NOT create collection
	err = createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenCustom))
	require.Error(t, err)
	var forbidden *clschema.SchemaObjectsCreateForbidden
	require.True(t, errors.As(err, &forbidden))

	require.NoError(t, createClass(t, &models.Class{Class: className}, helper.CreateAuth(tokenAdmin)))

	// can list collection
	classes := helper.GetClassAuth(t, className, tokenCustom)
	require.Equal(t, classes.Class, className)
}

const AuthCode = "auth"

// This test starts an oidc mock server with the same settings as the containerized one. Helpful if you want to know
// why a OIDC request fails
func TestRbacWithOIDCManual(t *testing.T) {
	t.Skip("This is for testing/debugging only")
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	m, _ := mockoidc.NewServer(rsaKey)
	ln, _ := net.Listen("tcp", "127.0.0.1:48001")
	m.Start(ln, nil)
	defer m.Shutdown()
	m.ClientSecret = "Secret"
	m.ClientID = "mock-oidc-test"

	admin := &mockoidc.MockUser{Subject: "admin-user"}
	m.QueueUser(admin)
	m.QueueCode(AuthCode)

	custom := &mockoidc.MockUser{Subject: "custom-user", Groups: []string{"custom-group"}}
	m.QueueUser(custom)
	m.QueueCode(AuthCode)

	// this should just run until we are done with testing
	for {
		fmt.Println(m.Issuer())
		fmt.Println(m.TokenEndpoint())
		time.Sleep(time.Second)
	}
}

func TestRbacWithOIDCAssignRevokeGroups(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		image *docker.Compose
	}{
		{
			name:  "without certificate",
			image: docker.New().WithWeaviate().WithMockOIDC().WithRBAC().WithRbacRoots("admin-user"),
		},
		{
			name:  "with certificate",
			image: docker.New().WithWeaviate().WithMockOIDCWithCertificate().WithRBAC().WithRbacRoots("admin-user"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compose, err := test.image.Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()
			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()

			// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
			// description for details
			tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())
			tokenCustom, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

			role := models.Role{Name: String("test-role"), Permissions: []*models.Permission{helper.NewCollectionsPermission().WithAction(authorization.ReadCollections).Permission()}}
			helper.CreateRole(t, tokenAdmin, &role)

			assign := func(key string) error {
				_, err = helper.Client(t).Authz.AssignRoleToGroup(authz.NewAssignRoleToGroupParams().WithBody(authz.AssignRoleToGroupBody{Roles: []string{*role.Name}}).WithID("custom-group"), helper.CreateAuth(key))
				return err
			}
			revoke := func(key string) error {
				_, err = helper.Client(t).Authz.RevokeRoleFromGroup(authz.NewRevokeRoleFromGroupParams().WithBody(authz.RevokeRoleFromGroupBody{Roles: []string{*role.Name}}).WithID("custom-group"), helper.CreateAuth(key))
				return err
			}

			// non-root users cannot assign roles to groups
			err = assign(tokenCustom)
			require.Error(t, err)
			var forbiddenAssign *authz.AssignRoleToGroupForbidden
			require.True(t, errors.As(err, &forbiddenAssign))

			// root users can assign roles to groups
			err = assign(tokenAdmin)
			if errors.As(err, &forbiddenAssign) {
				t.Log(forbiddenAssign.Payload.Error[0].Message)
			}
			require.NoError(t, err)

			// non-root users cannot revoke roles from groups
			err = revoke(tokenCustom)
			require.Error(t, err)
			var forbiddenRevoke *authz.RevokeRoleFromGroupForbidden
			require.True(t, errors.As(err, &forbiddenRevoke))

			// root users can revoke roles from groups
			err = revoke(tokenAdmin)
			require.NoError(t, err)

			// check that revoking the final group is a no-op
			err = revoke(tokenAdmin)
			require.NoError(t, err)
		})
	}
}

func TestOidcRootAndDynamicUsers(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		image *docker.Compose
	}{
		{
			name:  "without certificate",
			image: docker.New().WithWeaviate().WithMockOIDC().WithDbUsers(),
		},
		{
			name:  "with certificate",
			image: docker.New().WithWeaviate().WithMockOIDCWithCertificate().WithDbUsers(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			compose, err := test.image.Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()

			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()

			// the oidc mock server returns first the token for the admin user and then for the custom-user. See its
			// description for details
			tokenAdmin, _ := docker.GetTokensFromMockOIDCWithHelper(t, compose.GetMockOIDCHelper().URI())

			helper.DeleteUser(t, "dynamic1", tokenAdmin)
			apiKey := helper.CreateUser(t, "dynamic1", tokenAdmin)

			info := helper.GetInfoForOwnUser(t, apiKey)
			require.Equal(t, *info.Username, "dynamic1")
		})
	}
}

func TestOidcWrongCertificate(t *testing.T) {
	ctx := context.Background()
	t.Run("wrong certificates", func(t *testing.T) {
		// MockOIDC server has been created with it's own certifcates but we pass here some other certifcate, this situation should
		// lead to Weaviate not being able to connect OIDC server thus not being able to start
		wrongCertificate, _, err := docker.GenerateCertificateAndKey(docker.MockOIDC)
		require.NoError(t, err)
		compose, err := docker.New().
			WithWeaviate().WithDbUsers().
			WithMockOIDCWithCertificate().
			// pass some other certificate which is not used by MockOIDC
			WithWeaviateEnv("AUTHENTICATION_OIDC_CERTIFICATE", wrongCertificate).
			Start(ctx)
		// Weaviate should not start in this configuration
		require.Error(t, err)
		require.NoError(t, compose.Terminate(ctx))
	})
	t.Run("proper certificates", func(t *testing.T) {
		compose, err := docker.New().
			WithWeaviate().WithDbUsers().
			WithMockOIDCWithCertificate().
			Start(ctx)
		require.NoError(t, err)
		require.NoError(t, compose.Terminate(ctx))
	})
}
