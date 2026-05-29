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

package namespace

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaceGraduationE2E pins the full Stage-1 graduation journey: back
// up one of two namespaces, reset the cluster, restore the slice bare. A
// single container suffices because the strip is gated by
// ShouldStripNamespaces, not the target's NAMESPACES_ENABLED — bare names
// survive ValidateClassName and resolve unchanged for an empty namespace.
//
// The negative assertions (no ns2 user, no qualified ids surviving) pin the
// canCommit user-propagation contract: drop it and the participant snapshots
// the whole cluster, leaking foreign-namespace users into the restore.
func TestNamespaceGraduationE2E(t *testing.T) {
	const (
		ns1      = "ns1"
		ns2      = "ns2"
		backend  = "s3"
		backupID = "graduation-1"
	)
	adminAuth := helper.CreateAuth(adminKey)

	// Phase 1 — seed two namespaces, each with one class (+ data), one
	// alias, and one dynamic user.
	helper.CreateNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)
	t.Cleanup(func() {
		// Best-effort: bare survivors first, namespaced state second.
		helper.DeleteClassWithoutAssert(t, "Movies", adminKey)
		helper.DeleteUser(t, "alice", adminKey)
		helper.DeleteClassWithoutAssert(t, ns1+":Movies", adminKey)
		helper.DeleteClassWithoutAssert(t, ns2+":Books", adminKey)
		helper.DeleteUser(t, ns1+":alice", adminKey)
		helper.DeleteUser(t, ns2+":bob", adminKey)
		helper.DeleteNamespace(t, ns1, adminKey)
		helper.DeleteNamespace(t, ns2, adminKey)
	})

	aliceKey := createNamespacedUser(t, "alice", ns1, adminKey)
	bobKey := createNamespacedUser(t, "bob", ns2, adminKey)

	// ns1: Movies (3 objects) + alias MoviesByCity.
	helper.CreateClassAuth(t, &models.Class{
		Class:      "Movies",
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, aliceKey)
	movieUUIDs := []strfmt.UUID{
		"11111111-1111-1111-1111-111111111111",
		"22222222-2222-2222-2222-222222222222",
		"33333333-3333-3333-3333-333333333333",
	}
	movieTitles := []string{"Inception", "Interstellar", "Tenet"}
	for i, title := range movieTitles {
		require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
			ID:         movieUUIDs[i],
			Class:      "Movies",
			Properties: map[string]any{"title": title},
		}, aliceKey))
	}
	helper.CreateAliasAuth(t, &models.Alias{Alias: "MoviesByCity", Class: "Movies"}, aliceKey)

	// ns2: Books (2 objects) + alias BooksByAuthor.
	helper.CreateClassAuth(t, &models.Class{
		Class:      "Books",
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, bobKey)
	for i, title := range []string{"Dune", "Foundation"} {
		require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
			ID:         strfmt.UUID(fmt.Sprintf("44444444-4444-4444-4444-44444444444%d", i)),
			Class:      "Books",
			Properties: map[string]any{"title": title},
		}, bobKey))
	}
	helper.CreateAliasAuth(t, &models.Alias{Alias: "BooksByAuthor", Class: "Books"}, bobKey)

	// Sanity: both qualified classes visible to admin pre-backup.
	require.Equal(t, ns1+":Movies", helper.GetClassAuth(t, ns1+":Movies", adminKey).Class)
	require.Equal(t, ns2+":Books", helper.GetClassAuth(t, ns2+":Books", adminKey).Class)

	// Phase 2 — backup ns1 only, via include + includeUsers selectors.
	createResp, err := helper.Client(t).Backups.BackupsCreate(
		backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:           backupID,
				Include:      []string{ns1 + ":Movies"},
				IncludeUsers: []string{ns1 + ":*"},
				Config:       helper.DefaultBackupConfig(),
			}),
		adminAuth,
	)
	require.NoError(t, err)
	require.NotNil(t, createResp.Payload)
	require.Equal(t, "", createResp.Payload.Error)
	// The response reflects what the scheduler resolved, not the artefact's
	// contents — those are exercised by the restore assertions below.
	assert.ElementsMatch(t, []string{ns1 + ":Movies"}, createResp.Payload.Classes)
	assert.ElementsMatch(t, []string{ns1 + ":alice"}, createResp.Payload.Users)

	helper.ExpectBackupEventuallyCreated(t, backupID, backend, adminAuth,
		helper.WithPollInterval(helper.MinPollInterval),
		helper.WithDeadline(helper.MaxDeadline))

	// Phase 3 — reset cluster. Aliases must be removed explicitly (no
	// cascade) before the class delete; pattern from
	// test/acceptance/aliases/aliases_api_backup_test.go cleanup.
	helper.DeleteAliasWithAuthz(t, ns1+":MoviesByCity", adminAuth)
	helper.DeleteAliasWithAuthz(t, ns2+":BooksByAuthor", adminAuth)
	helper.DeleteClassWithAuthz(t, ns1+":Movies", adminAuth)
	helper.DeleteClassWithAuthz(t, ns2+":Books", adminAuth)
	helper.DeleteUser(t, ns1+":alice", adminKey)
	helper.DeleteUser(t, ns2+":bob", adminKey)

	// Sanity: cluster is empty of the graduated state.
	_, err = helper.GetClassWithoutAssert(t, ns1+":Movies", adminKey)
	require.Error(t, err, "ns1:Movies should be deleted pre-restore")
	_, err = helper.GetClassWithoutAssert(t, ns2+":Books", adminKey)
	require.Error(t, err, "ns2:Books should be deleted pre-restore")

	// Phase 4 — restore with strip. usersOptions=all loads the user blob;
	// rolesOptions=noRestore keeps the (deferred) RBAC slice out of scope.
	all := "all"
	noRestore := "noRestore"
	restoreConf := helper.DefaultRestoreConfig()
	restoreConf.UsersOptions = &all
	restoreConf.RolesOptions = &noRestore

	restoreResp, err := helper.Client(t).Backups.BackupsRestore(
		backups.NewBackupsRestoreParams().
			WithBackend(backend).
			WithID(backupID).
			WithBody(&models.BackupRestoreRequest{
				Config:                restoreConf,
				ShouldStripNamespaces: true,
			}),
		adminAuth,
	)
	require.NoError(t, err)
	require.NotNil(t, restoreResp.Payload)
	require.Equal(t, "", restoreResp.Payload.Error)

	helper.ExpectBackupEventuallyRestored(t, backupID, backend, adminAuth,
		helper.WithPollInterval(helper.MinPollInterval),
		helper.WithDeadline(helper.MaxDeadline))

	// Phase 5 — assert.

	// Class names stripped: bare "Movies" exists; qualified form does not.
	gotMovies := helper.GetClassAuth(t, "Movies", adminKey)
	require.NotNil(t, gotMovies)
	assert.Equal(t, "Movies", gotMovies.Class)
	_, err = helper.GetClassWithoutAssert(t, ns1+":Movies", adminKey)
	require.Error(t, err, "ns1:Movies must not exist post-strip")

	// ns2 excluded: no Books, qualified or bare.
	_, err = helper.GetClassWithoutAssert(t, "Books", adminKey)
	require.Error(t, err, "Books (bare) must not exist — ns2 was not in include-list")
	_, err = helper.GetClassWithoutAssert(t, ns2+":Books", adminKey)
	require.Error(t, err, "ns2:Books must not exist post-restore")

	// Alias stripped + retargeted; foreign-form absent.
	gotAlias := helper.GetAliasWithAuthz(t, "MoviesByCity", adminAuth)
	require.NotNil(t, gotAlias)
	assert.Equal(t, "MoviesByCity", gotAlias.Alias)
	assert.Equal(t, "Movies", gotAlias.Class)
	helper.GetAliasWithAuthzNotFound(t, ns1+":MoviesByCity", adminAuth)
	helper.GetAliasWithAuthzNotFound(t, "BooksByAuthor", adminAuth)

	// User stripped + namespace cleared; qualified id no longer resolves.
	gotUser := helper.GetUser(t, "alice", adminKey)
	require.NotNil(t, gotUser)
	assert.Empty(t, gotUser.Namespace, "User.Namespace must be cleared post-strip")
	assertUserNotFound(t, ns1+":alice", adminKey)

	// Foreign-namespace user absent. Without Users propagated through
	// coordinator.canCommit the participant snapshots the whole cluster and
	// bob would land here too.
	assertUserNotFound(t, "bob", adminKey)
	assertUserNotFound(t, ns2+":bob", adminKey)

	// Data round-trip: shard files materialized at the stripped name and
	// re-attached to the bare "Movies" class.
	for i, expectedTitle := range movieTitles {
		obj, err := helper.GetObjectAuth(t, "Movies", movieUUIDs[i], adminKey)
		require.NoError(t, err, "object %d (%s) not retrievable after restore", i, movieUUIDs[i])
		props, ok := obj.Properties.(map[string]any)
		require.True(t, ok, "object %d properties not a map", i)
		assert.Equal(t, expectedTitle, props["title"])
	}
}

// assertUserNotFound is the negative counterpart of helper.GetUser, which
// asserts 200; the OK helper would fail the test on a legitimate 404.
func assertUserNotFound(t *testing.T, userID, key string) {
	t.Helper()
	_, err := helper.Client(t).Users.GetUserInfo(
		users.NewGetUserInfoParams().WithUserID(userID),
		helper.CreateAuth(key),
	)
	require.Error(t, err, "expected user %q to be absent", userID)
	var nf *users.GetUserInfoNotFound
	require.True(t, errors.As(err, &nf),
		"expected user %q to return 404, got %T: %v", userID, err, err)
}
