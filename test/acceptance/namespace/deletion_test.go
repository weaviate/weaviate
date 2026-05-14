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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// rawDeleteNamespace issues the DELETE and returns the typed response.
// helper.DeleteNamespace asserts NoError, which is wrong for tests that
// expect a 404 (idempotent re-call after removal) or that drive the
// endpoint from a goroutine where require.* is unsafe.
func rawDeleteNamespace(t *testing.T, name, key string) (*namespaces.DeleteNamespaceAccepted, error) {
	t.Helper()
	return helper.Client(t).Namespaces.DeleteNamespace(
		namespaces.NewDeleteNamespaceParams().WithNamespaceID(name),
		helper.CreateAuth(key),
	)
}

// TestNamespaces_DeleteHappyPath creates a namespace with a class, an
// alias, and a DB user, deletes it, and verifies that the namespace,
// its class, its alias, and the user are all gone.
func TestNamespaces_DeleteHappyPath(t *testing.T) {
	const (
		ns        = "delhappy"
		userID    = "alice"
		className = "Movies"
		aliasName = "Films"
	)
	qualifiedClass := ns + ":" + className
	qualifiedAlias := ns + ":" + aliasName
	qualifiedUser := ns + ":" + userID

	helper.CreateNamespace(t, ns, adminKey)
	userKey := createNamespacedUser(t, userID, ns, adminKey)
	helper.CreateClassAuth(t, &models.Class{
		Class:      className,
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, userKey)
	helper.CreateAliasAuth(t, &models.Alias{Alias: aliasName, Class: className}, userKey)

	// Trigger the async delete and wait for full removal. The helper
	// polls Get until 404, so by the time it returns the namespace is
	// gone — making the per-resource checks below assertions about the
	// cascade cleanup, not the namespace itself.
	helper.DeleteNamespace(t, ns, adminKey)

	_, err := helper.GetClassWithoutAssert(t, qualifiedClass, adminKey)
	require.Error(t, err, "class %q should have been deleted", qualifiedClass)

	helper.GetAliasWithAuthzNotFound(t, qualifiedAlias, helper.CreateAuth(adminKey))

	_, err = schemaDumpAs(t, userKey)
	require.Error(t, err, "user %q should no longer authenticate", qualifiedUser)
}

// TestNamespaces_DeleteUserAuthBlockedClusterWide creates a namespace +
// DB user, deletes the namespace, and asserts the user's API key
// eventually fails with 401 against every replica. The leader applies
// the synchronous DeleteUsersInNamespace before returning 202; followers
// apply asynchronously, so each node is polled until auth is rejected.
func TestNamespaces_DeleteUserAuthBlockedClusterWide(t *testing.T) {
	const (
		ns     = "delauth"
		userID = "bob"
	)

	helper.CreateNamespace(t, ns, adminKey)
	userKey := createNamespacedUser(t, userID, ns, adminKey)

	// Sanity: the user can authenticate before the delete.
	_, err := schemaDumpAs(t, userKey)
	require.NoError(t, err, "fresh DB user should authenticate")

	// Issue the delete; do not wait for full cleanup — the auth-blocked
	// guarantee is established by the synchronous user-delete RAFT
	// command, which has committed by the time the 202 is received.
	helper.DeleteNamespace(t, ns, adminKey, helper.WithoutWaitForCleanup())

	// On every replica, the user's API key must eventually be rejected.
	originalURI := sharedCompose.GetWeaviate().URI()
	t.Cleanup(func() { helper.SetupClient(originalURI) })
	for i := 1; i <= 3; i++ {
		nodeURI := sharedCompose.GetWeaviateNode(i).URI()
		helper.SetupClient(nodeURI)
		assert.Eventually(t, func() bool {
			_, err := schemaDumpAs(t, userKey)
			if err == nil {
				return false
			}
			var unauth *schema.SchemaDumpUnauthorized
			return errors.As(err, &unauth)
		}, 10*time.Second, 50*time.Millisecond,
			"auth must fail on node %s after namespace delete", nodeURI)
	}
	// Restore for subsequent tests; t.Cleanup also covers it.
	helper.SetupClient(originalURI)
	helper.WaitForNamespaceGone(t, ns, adminKey, 30*time.Second)
}

// TestNamespaces_RecreateAfterDelete creates a namespace with a class so
// the cleanup tick has work to do, deletes the namespace, then polls
// CreateNamespace until cleanup finishes and recreation succeeds. Only
// 409 (still deleting) and success are acceptable; any other response
// fails the test.
func TestNamespaces_RecreateAfterDelete(t *testing.T) {
	const (
		ns        = "delrecreate"
		userID    = "creator"
		className = "Movies"
	)

	helper.CreateNamespace(t, ns, adminKey)
	// On NS-enabled clusters QualifyForCreate rejects principals without a
	// namespace claim, so class creation must run as a namespaced user.
	userKey := createNamespacedUser(t, userID, ns, adminKey)
	helper.CreateClassAuth(t, &models.Class{
		Class:      className,
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, userKey)
	// Best-effort cleanup in case the namespace delete fails partway
	// through; if it succeeds the cascade has already removed the class.
	defer helper.DeleteClassWithoutAssert(t, ns+":"+className, adminKey)

	helper.DeleteNamespace(t, ns, adminKey, helper.WithoutWaitForCleanup())

	// Poll until recreate succeeds. While cleanup is in progress the
	// namespace is in the deleting state and create returns 409 — keep
	// retrying. Any other response is a real failure.
	assert.Eventually(t, func() bool {
		_, err := helper.Client(t).Namespaces.CreateNamespace(
			namespaces.NewCreateNamespaceParams().WithNamespaceID(ns),
			helper.CreateAuth(adminKey),
		)
		if err == nil {
			return true
		}
		var conflict *namespaces.CreateNamespaceConflict
		if errors.As(err, &conflict) {
			return false
		}
		require.Failf(t, "unexpected response during recreate", "%T: %v", err, err)
		return true
	}, 5*time.Second, 50*time.Millisecond,
		"namespace did not become recreatable within 5s")

	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
}

// TestNamespaces_DeleteIsIdempotent calls DELETE twice in a row while
// the namespace is still in the deleting state and asserts both return
// 202. After cleanup completes, DELETE returns 404.
func TestNamespaces_DeleteIsIdempotent(t *testing.T) {
	const ns = "delidempotent"
	helper.CreateNamespace(t, ns, adminKey)

	// First DELETE: 202.
	helper.DeleteNamespace(t, ns, adminKey, helper.WithoutWaitForCleanup())

	// Second DELETE while still deleting: 202 (best-effort — if cleanup is
	// fast and removes the entity between the two calls, the second one
	// returns 404, which is also acceptable per the contract).
	_, err := rawDeleteNamespace(t, ns, adminKey)
	if err != nil {
		var nf *namespaces.DeleteNamespaceNotFound
		require.True(t, errors.As(err, &nf),
			"second DELETE should be 202 (still deleting) or 404 (already removed); got %T: %v", err, err)
	}

	helper.WaitForNamespaceGone(t, ns, adminKey, 30*time.Second)

	// After cleanup: 404.
	_, err = rawDeleteNamespace(t, ns, adminKey)
	require.Error(t, err)
	var nf *namespaces.DeleteNamespaceNotFound
	require.True(t, errors.As(err, &nf), "DELETE after removal should return 404, got %T: %v", err, err)
}

// TestNamespaces_ConcurrentDeleteAndAddClass launches a DELETE and an
// AddClass concurrently. The add-class apply gate may reject
// (ErrNamespaceDeleting/ErrNamespaceGone) or it may succeed and then be
// cleaned up — both outcomes are acceptable. The post-condition is that
// no orphan class survives once the namespace entity is gone.
func TestNamespaces_ConcurrentDeleteAndAddClass(t *testing.T) {
	const ns = "delrace"
	const className = "Films"
	qualifiedClass := ns + ":" + className

	helper.CreateNamespace(t, ns, adminKey)
	userKey := createNamespacedUser(t, "carol", ns, adminKey)
	// No explicit user cleanup: the namespace delete below removes the
	// user as part of the cascade.

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = rawDeleteNamespace(t, ns, adminKey)
	}()
	go func() {
		defer wg.Done()
		// Best-effort: may succeed, may be rejected.
		_, _ = helper.Client(t).Schema.SchemaObjectsCreate(
			schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{
				Class:      className,
				Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
			}),
			helper.CreateAuth(userKey),
		)
	}()
	wg.Wait()

	helper.WaitForNamespaceGone(t, ns, adminKey, 30*time.Second)

	// Post-condition: no class with the qualified name survives.
	_, err := helper.GetClassWithoutAssert(t, qualifiedClass, adminKey)
	require.Error(t, err, "class %q must not survive namespace removal", qualifiedClass)
}

// TestNamespaces_DeleteMissingReturns404FromEveryReplica drives DELETE
// on a non-existent namespace against each replica in turn and asserts a
// 404 response. The Apply path forwards from any non-leader replica to
// the leader, so at least two of the three iterations exercise the
// follower-forward path. The leader's apply returns ErrNotFound, which
// must round-trip through gRPC and re-chain on the client so the
// handler's errors.Is mapping returns 404 rather than 500.
func TestNamespaces_DeleteMissingReturns404FromEveryReplica(t *testing.T) {
	const ns = "neverexisted"

	originalURI := sharedCompose.GetWeaviate().URI()
	t.Cleanup(func() { helper.SetupClient(originalURI) })

	for i := 1; i <= 3; i++ {
		nodeURI := sharedCompose.GetWeaviateNode(i).URI()
		helper.SetupClient(nodeURI)
		_, err := rawDeleteNamespace(t, ns, adminKey)
		require.Error(t, err, "DELETE on missing namespace must return an error from %s", nodeURI)
		var nf *namespaces.DeleteNamespaceNotFound
		require.True(t, errors.As(err, &nf),
			"DELETE on %s should return 404, got %T: %v", nodeURI, err, err)
	}
}

// schemaDumpAs hits a generic authenticated endpoint with the given key.
// Used to probe whether the key still authenticates.
func schemaDumpAs(t *testing.T, key string) (any, error) {
	t.Helper()
	return helper.Client(t).Schema.SchemaDump(
		schema.NewSchemaDumpParams(),
		helper.CreateAuth(key),
	)
}
