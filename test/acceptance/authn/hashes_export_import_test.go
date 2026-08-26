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

package authn

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/experimental"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	root, rootKey     = "admin-user", "admin-key"
	staticUser        = "static-user"
	staticUserKey     = "static-key"
	dummyArgon2idHash = "$argon2id$v=19$m=65536,t=3,p=2$c29tZXNhbHQ$aGFzaHZhbHVl"
)

func strptr(s string) *string { return &s }

func TestHashesExportImport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Source cluster with namespaces off, where the credentials are created.
	source, err := docker.New().WithWeaviate().
		WithApiKey().
		WithUserApiKey(root, rootKey).
		WithUserApiKey(staticUser, staticUserKey).
		WithRBAC().WithRbacRoots(root).
		WithDbUsers().
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, source.Terminate(ctx)) }()

	// Namespace-enabled target cluster, where credentials are imported.
	target, err := docker.New().WithWeaviate().
		WithApiKey().
		WithUserApiKey(root, rootKey).
		WithRBAC().WithRbacRoots(root).
		WithDbUsers().
		WithNamespaces().
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, target.Terminate(ctx)) }()

	sourceURI := source.GetWeaviate().URI()
	targetURI := target.GetWeaviate().URI()
	defer helper.ResetClient()

	exportRecord := func(t *testing.T, userID string) *models.DBUserCredential {
		t.Helper()
		resp, err := helper.Client(t).Experimental.ExportUsers(experimental.NewExportUsersParams(), helper.CreateAuth(rootKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		for _, u := range resp.Payload.Users {
			if u.UserID != nil && *u.UserID == userID {
				return u
			}
		}
		require.FailNowf(t, "user not found in export", "user %q", userID)
		return nil
	}

	t.Run("strong key authenticates on the namespaced target after import", func(t *testing.T) {
		const userID = "strongmigrator"

		helper.SetupClient(sourceURI)
		sourceAPIKey := helper.CreateUser(t, userID, rootKey)
		// The key authenticates on the source before migration.
		require.Equal(t, userID, *helper.GetInfoForOwnUser(t, sourceAPIKey).Username)

		rec := exportRecord(t, userID)
		require.Equal(t, models.DBUserCredentialStatusExported, rec.Status)
		require.NotEmpty(t, rec.SecureHash)

		helper.SetupClient(targetURI)
		const ns = "migns"
		helper.CreateNamespace(t, ns, rootKey)

		importResp, err := helper.Client(t).Experimental.ImportUsers(
			experimental.NewImportUsersParams().WithBody(&models.UserImportRequest{
				Namespace: ns,
				Users:     []*models.DBUserCredential{rec},
			}), helper.CreateAuth(rootKey))
		require.NoError(t, err)
		require.Len(t, importResp.Payload.Results, 1)
		require.Equal(t, models.UserImportResultStatusCreated, *importResp.Payload.Results[0].Status)

		// The original source key now authenticates against the target. Import
		// stored the identifier, the user id, and the hash under the target namespace.
		info := helper.GetInfoForOwnUser(t, sourceAPIKey)
		require.NotNil(t, info)

		// A second import is idempotent: same identifier, already present.
		reimport, err := helper.Client(t).Experimental.ImportUsers(
			experimental.NewImportUsersParams().WithBody(&models.UserImportRequest{
				Namespace: ns,
				Users:     []*models.DBUserCredential{rec},
			}), helper.CreateAuth(rootKey))
		require.NoError(t, err)
		require.Equal(t, models.UserImportResultStatusSkippedExists, *reimport.Payload.Results[0].Status)
	})

	t.Run("imported key is reported skipped", func(t *testing.T) {
		helper.SetupClient(sourceURI)
		// Turn the static key into an imported (weak-hash) db user via the legacy
		// import path, then confirm export refuses to carry it.
		imp := true
		_, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(staticUser).WithBody(users.CreateUserBody{Import: &imp}),
			helper.CreateAuth(rootKey))
		require.NoError(t, err)

		rec := exportRecord(t, staticUser)
		require.Equal(t, models.DBUserCredentialStatusImportedKey, rec.Status)
		require.Empty(t, rec.SecureHash)
	})

	t.Run("revoked key is reported skipped", func(t *testing.T) {
		const userID = "revokeduser"

		helper.SetupClient(sourceURI)
		helper.CreateUser(t, userID, rootKey)
		helper.DeactivateUser(t, rootKey, userID, true) // revokeKey=true

		rec := exportRecord(t, userID)
		require.Equal(t, models.DBUserCredentialStatusRevoked, rec.Status)
		require.Empty(t, rec.SecureHash)
	})

	t.Run("namespaced admin is denied foreign-namespace import and any export", func(t *testing.T) {
		helper.SetupClient(targetURI)
		const ownNS, foreignNS = "tenanta", "tenantb"
		helper.CreateNamespace(t, ownNS, rootKey)
		helper.CreateNamespace(t, foreignNS, rootKey)

		// An admin whose RBAC role is limited to ownNS.
		adminID := "nsadmin"
		adminAPIKey := helper.CreateUserWithNamespace(t, adminID, ownNS, rootKey)
		helper.AssignRoleToUser(t, rootKey, authorization.Admin, ownNS+":"+adminID)
		helper.WaitForOwnRole(t, adminAPIKey, authorization.Admin)

		_, err := helper.Client(t).Experimental.ImportUsers(
			experimental.NewImportUsersParams().WithBody(&models.UserImportRequest{
				Namespace: foreignNS,
				Users: []*models.DBUserCredential{{
					UserID:         strptr("intruder"),
					UserIdentifier: "intruder-ident",
					SecureHash:     dummyArgon2idHash,
					Active:         true,
				}},
			}), helper.CreateAuth(adminAPIKey))
		require.Error(t, err)
		var forbidden *experimental.ImportUsersForbidden
		require.True(t, errors.As(err, &forbidden), "expected ImportUsersForbidden, got %T: %v", err, err)

		_, err = helper.Client(t).Experimental.ExportUsers(experimental.NewExportUsersParams(), helper.CreateAuth(adminAPIKey))
		require.Error(t, err)
		var exportForbidden *experimental.ExportUsersForbidden
		require.True(t, errors.As(err, &exportForbidden), "expected ExportUsersForbidden, got %T: %v", err, err)
	})
}
