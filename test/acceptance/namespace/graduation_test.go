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

// TestNamespaceGraduationE2E pins the full graduation journey: back up one of
// two namespaces, reset the cluster, restore the slice bare. A single container
// suffices because the strip is gated by ShouldStripNamespaces, not the target's
// NAMESPACES_ENABLED — bare names survive ValidateClassName and resolve
// unchanged for an empty namespace.
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

	helper.CreateNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)
	t.Cleanup(func() {
		helper.DeleteClassWithoutAssert(t, "Movies", adminKey)
		helper.DeleteClassWithoutAssert(t, "Directors", adminKey)
		helper.DeleteUser(t, "alice", adminKey)
		helper.DeleteClassWithoutAssert(t, ns1+":Movies", adminKey)
		helper.DeleteClassWithoutAssert(t, ns1+":Directors", adminKey)
		helper.DeleteClassWithoutAssert(t, ns2+":Books", adminKey)
		helper.DeleteUser(t, ns1+":alice", adminKey)
		helper.DeleteUser(t, ns2+":bob", adminKey)
		helper.DeleteNamespace(t, ns1, adminKey)
		helper.DeleteNamespace(t, ns2, adminKey)
	})

	aliceKey := createNamespacedUser(t, "alice", ns1, adminKey)
	bobKey := createNamespacedUser(t, "bob", ns2, adminKey)

	// Directors before Movies: live-cluster cross-ref validation needs the
	// target to exist (the restore path relaxes this).
	const nolanID = strfmt.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
	helper.CreateClassAuth(t, &models.Class{
		Class:      "Directors",
		Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
	}, aliceKey)
	require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
		ID:         nolanID,
		Class:      "Directors",
		Properties: map[string]any{"name": "Christopher Nolan"},
	}, aliceKey))

	// alice's short "Directors" DataType is stored qualified ("ns1:Directors") —
	// the form graduation must strip.
	helper.CreateClassAuth(t, &models.Class{
		Class: "Movies",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
			{Name: "directedBy", DataType: []string{"Directors"}},
		},
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
	// The stored beacon is namespace-free at rest, so it must survive graduation
	// untouched.
	_, err := helper.AddReferenceReturn(t,
		&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Directors/" + nolanID.String())},
		movieUUIDs[0], "Movies", "directedBy", "", helper.CreateAuth(aliceKey))
	require.NoError(t, err)
	helper.CreateAliasAuth(t, &models.Alias{Alias: "MoviesByCity", Class: "Movies"}, aliceKey)

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

	require.Equal(t, ns1+":Movies", helper.GetClassAuth(t, ns1+":Movies", adminKey).Class)
	require.Equal(t, ns2+":Books", helper.GetClassAuth(t, ns2+":Books", adminKey).Class)

	createResp, err := helper.Client(t).Backups.BackupsCreate(
		backups.NewBackupsCreateParams().
			WithBackend(backend).
			WithBody(&models.BackupCreateRequest{
				ID:           backupID,
				Include:      []string{ns1 + ":Movies", ns1 + ":Directors"},
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
	assert.ElementsMatch(t, []string{ns1 + ":Movies", ns1 + ":Directors"}, createResp.Payload.Classes)
	assert.ElementsMatch(t, []string{ns1 + ":alice"}, createResp.Payload.Users)

	helper.ExpectBackupEventuallyCreated(t, backupID, backend, adminAuth,
		helper.WithPollInterval(helper.MinPollInterval),
		helper.WithDeadline(helper.MaxDeadline))

	// Aliases must be removed explicitly (no cascade) before deleting their class.
	helper.DeleteAliasWithAuthz(t, ns1+":MoviesByCity", adminAuth)
	helper.DeleteAliasWithAuthz(t, ns2+":BooksByAuthor", adminAuth)
	// Movies (the ref source) before Directors (the ref target).
	helper.DeleteClassWithAuthz(t, ns1+":Movies", adminAuth)
	helper.DeleteClassWithAuthz(t, ns1+":Directors", adminAuth)
	helper.DeleteClassWithAuthz(t, ns2+":Books", adminAuth)
	helper.DeleteUser(t, ns1+":alice", adminKey)
	helper.DeleteUser(t, ns2+":bob", adminKey)

	_, err = helper.GetClassWithoutAssert(t, ns1+":Movies", adminKey)
	require.Error(t, err, "ns1:Movies should be deleted pre-restore")
	_, err = helper.GetClassWithoutAssert(t, ns2+":Books", adminKey)
	require.Error(t, err, "ns2:Books should be deleted pre-restore")

	// usersOptions=all loads the user blob; rolesOptions=noRestore because the
	// strip leaves role names at their pre-strip form, so restoring them would dangle.
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

	gotMovies := helper.GetClassAuth(t, "Movies", adminKey)
	require.NotNil(t, gotMovies)
	assert.Equal(t, "Movies", gotMovies.Class)
	_, err = helper.GetClassWithoutAssert(t, ns1+":Movies", adminKey)
	require.Error(t, err, "ns1:Movies must not exist post-strip")

	// "ns1:Directors" would dangle once the target is restored bare, so the
	// cross-ref DataType must be stripped too.
	var directedBy *models.Property
	for _, p := range gotMovies.Properties {
		if p.Name == "directedBy" {
			directedBy = p
		}
	}
	require.NotNil(t, directedBy, "directedBy property must survive restore")
	assert.Equal(t, []string{"Directors"}, directedBy.DataType, "cross-ref DataType must be stripped")
	gotDirectors := helper.GetClassAuth(t, "Directors", adminKey)
	require.NotNil(t, gotDirectors)
	assert.Equal(t, "Directors", gotDirectors.Class)
	_, err = helper.GetClassWithoutAssert(t, ns1+":Directors", adminKey)
	require.Error(t, err, "ns1:Directors must not exist post-strip")

	_, err = helper.GetClassWithoutAssert(t, "Books", adminKey)
	require.Error(t, err, "Books (bare) must not exist — ns2 was not in include-list")
	_, err = helper.GetClassWithoutAssert(t, ns2+":Books", adminKey)
	require.Error(t, err, "ns2:Books must not exist post-restore")

	gotAlias := helper.GetAliasWithAuthz(t, "MoviesByCity", adminAuth)
	require.NotNil(t, gotAlias)
	assert.Equal(t, "MoviesByCity", gotAlias.Alias)
	assert.Equal(t, "Movies", gotAlias.Class)
	helper.GetAliasWithAuthzNotFound(t, ns1+":MoviesByCity", adminAuth)
	helper.GetAliasWithAuthzNotFound(t, "BooksByAuthor", adminAuth)

	gotUser := helper.GetUser(t, "alice", adminKey)
	require.NotNil(t, gotUser)
	assert.Empty(t, gotUser.Namespace, "User.Namespace must be cleared post-strip")
	assertUserNotFound(t, ns1+":alice", adminKey)

	// Without Users propagated through coordinator.canCommit the participant
	// snapshots the whole cluster and bob would land here too.
	assertUserNotFound(t, "bob", adminKey)
	assertUserNotFound(t, ns2+":bob", adminKey)

	// Proves the strip reached storage: shard files materialized at the stripped
	// name and re-attached to the bare "Movies" class.
	for i, expectedTitle := range movieTitles {
		obj, err := helper.GetObjectAuth(t, "Movies", movieUUIDs[i], adminKey)
		require.NoError(t, err, "object %d (%s) not retrievable after restore", i, movieUUIDs[i])
		props, ok := obj.Properties.(map[string]any)
		require.True(t, ok, "object %d properties not a map", i)
		assert.Equal(t, expectedTitle, props["title"])
	}

	// A plain GET returns the ref as a beacon (?include is for vectors, not ref
	// expansion); the beacon is namespace-free at rest, so it still targets the
	// bare Directors — schema strip suffices.
	inception, err := helper.GetObjectAuth(t, "Movies", movieUUIDs[0], adminKey)
	require.NoError(t, err)
	inceptionProps, ok := inception.Properties.(map[string]any)
	require.True(t, ok)
	require.Contains(t, inceptionProps, "directedBy", "directedBy ref must survive restore")
	assert.Contains(t, fmt.Sprintf("%v", inceptionProps["directedBy"]), nolanID.String(),
		"directedBy beacon must still target the Nolan director")
	nolan, err := helper.GetObjectAuth(t, "Directors", nolanID, adminKey)
	require.NoError(t, err, "ref target must be retrievable at the bare class name")
	nolanProps, ok := nolan.Properties.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Christopher Nolan", nolanProps["name"])
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
	require.ErrorIs(t, err, nf)
}
