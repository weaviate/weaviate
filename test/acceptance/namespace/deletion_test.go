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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/weaviate/weaviate/client/batch"
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
	t.Parallel()
	ns := uniqueNS()
	const (
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
// eventually fails with 401 against every replica. The auth guard rejects
// the key on any node that has applied the flip to deleting; followers apply
// asynchronously, so each node is polled until auth is rejected.
func TestNamespaces_DeleteUserAuthBlockedClusterWide(t *testing.T) {
	ns := uniqueNS()
	const userID = "bob"

	helper.CreateNamespace(t, ns, adminKey)
	userKey := createNamespacedUser(t, userID, ns, adminKey)

	// Sanity: the user can authenticate before the delete.
	_, err := schemaDumpAs(t, userKey)
	require.NoError(t, err, "fresh DB user should authenticate")

	// Issue the delete; do not wait for full cleanup — the auth-blocked
	// guarantee comes from the flip to deleting, which the auth guard
	// enforces on every node that has applied it, before the cleanup tick
	// reclaims the user rows.
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
	t.Parallel()
	ns := uniqueNS()
	const (
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
	}, 30*time.Second, 50*time.Millisecond,
		"namespace did not become recreatable within 30s")

	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
}

// TestNamespaces_DeleteIsIdempotent calls DELETE twice in a row while
// the namespace is still in the deleting state and asserts both return
// 202. After cleanup completes, DELETE returns 404.
func TestNamespaces_DeleteIsIdempotent(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
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
	t.Parallel()
	ns := uniqueNS()
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

// TestNamespaces_DeleteWhileBatchInsertInFlight runs a sustained batch
// insert against a namespaced class, issues DELETE on the namespace, and
// asserts: the race is actually exercised (writes succeed before DELETE
// and fail after), the namespace is fully gone, and a fresh namespace +
// class can be created cleanly — the latter is the proxy for "no torn
// state was left behind".
func TestNamespaces_DeleteWhileBatchInsertInFlight(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	const (
		userID    = "dave"
		className = "Tickets"
	)
	qualifiedClass := ns + ":" + className

	helper.CreateNamespace(t, ns, adminKey)
	userKey := createNamespacedUser(t, userID, ns, adminKey)
	helper.CreateClassAuth(t, &models.Class{
		Class:      className,
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, userKey)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		batchOK, batchFailed atomic.Int64
		lastErrMu            sync.Mutex
		lastErr              error
	)

	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				_, err := helper.Client(t).Batch.BatchObjectsCreate(
					batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
						Objects: []*models.Object{{
							Class:      className,
							Properties: map[string]any{"title": fmt.Sprintf("w%d-%d", workerID, i)},
						}},
					}),
					helper.CreateAuth(userKey),
				)
				if err != nil {
					batchFailed.Add(1)
					lastErrMu.Lock()
					lastErr = err
					lastErrMu.Unlock()
				} else {
					batchOK.Add(1)
				}
			}
		}(w)
	}

	// Pre-condition: let a few batches commit so the test actually
	// exercises the race instead of issuing DELETE before any data lands.
	require.Eventually(t, func() bool { return batchOK.Load() >= 4 },
		10*time.Second, 50*time.Millisecond,
		"batch loop should commit some objects before DELETE")

	helper.DeleteNamespace(t, ns, adminKey)
	cancel()
	wg.Wait()

	// (a) The batch loop must have seen failures once the class was deleted
	// — otherwise the test never actually overlapped DELETE with writes.
	require.Greater(t, batchFailed.Load(), int64(0),
		"expected at least one batch failure after class delete; ok=%d failed=%d",
		batchOK.Load(), batchFailed.Load())

	// The last failure must be a typed REST error (any 4xx is fine), not
	// a 500 from torn state.
	lastErrMu.Lock()
	le := lastErr
	lastErrMu.Unlock()
	require.Error(t, le)
	var internal *batch.BatchObjectsCreateInternalServerError
	require.False(t, errors.As(le, &internal),
		"post-delete batch failure must not be a 500; got %T: %v", le, le)

	// (b) Post-condition: class is gone.
	_, err := helper.GetClassWithoutAssert(t, qualifiedClass, adminKey)
	require.Error(t, err, "class %q must not survive namespace removal", qualifiedClass)

	// (c) Namespace can be recreated cleanly with the same class name —
	// a torn cleanup would surface here as a create failure or stale state.
	helper.CreateNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	userKey2 := createNamespacedUser(t, userID, ns, adminKey)
	helper.CreateClassAuth(t, &models.Class{
		Class:      className,
		Properties: []*models.Property{{Name: "title", DataType: []string{"text"}}},
	}, userKey2)
}

// TestNamespaces_DeleteMissingReturns404FromEveryReplica drives DELETE
// on a non-existent namespace against each replica in turn and asserts a
// 404 response. The Apply path forwards from any non-leader replica to
// the leader, so at least two of the three iterations exercise the
// follower-forward path. The leader's apply returns ErrNotFound, which
// must round-trip through gRPC and re-chain on the client so the
// handler's errors.Is mapping returns 404 rather than 500.
func TestNamespaces_DeleteMissingReturns404FromEveryReplica(t *testing.T) {
	ns := uniqueNS()

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

// dataDirEntries lists ./data in the given node's container. A failed exec
// errors rather than returning the empty listing any absence check would pass.
func dataDirEntries(ctx context.Context, node int) ([]string, error) {
	code, reader, err := sharedCompose.GetWeaviateNode(node).Container().
		Exec(ctx, []string{"ls", "-1", "./data"}, tcexec.Multiplexed())
	if err != nil {
		return nil, fmt.Errorf("exec ls on node %d: %w", node, err)
	}
	if code != 0 {
		return nil, fmt.Errorf("ls ./data on node %d exited %d", node, code)
	}

	out := new(strings.Builder)
	if _, err := io.Copy(out, reader); err != nil {
		return nil, fmt.Errorf("read ls output from node %d: %w", node, err)
	}

	var entries []string
	for line := range strings.SplitSeq(out.String(), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			entries = append(entries, line)
		}
	}
	return entries, nil
}

// requireClassDirsReclaimed waits until no ./data entry on any node contains a
// lowercased class name. A drop first appends ".deleteme" or prepends
// "__DELETE_ME_AFTER_BACKUP__", so equality and prefix tests both miss it.
func requireClassDirsReclaimed(t *testing.T, ctx context.Context, qualifiedClasses ...string) {
	t.Helper()
	require.NotEmpty(t, qualifiedClasses, "nothing to assert the absence of")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, class := range qualifiedClasses {
			dirName := strings.ToLower(class)
			for node := 1; node <= 3; node++ {
				entries, err := dataDirEntries(ctx, node)
				if !assert.NoError(c, err) {
					return
				}
				for _, entry := range entries {
					assert.NotContains(c, entry, dirName, "node %d still holds %q", node, entry)
				}
			}
		}
	}, 30*time.Second, 250*time.Millisecond, "class directories were never reclaimed")
}

// TestNamespaces_DeleteSuspendedNamespace asserts the delete cascade removes a
// suspended namespace's classes and aliases and reclaims their directories on
// disk. The cascade issues DeleteAlias and DeleteClass while the namespace is
// deleting, so a prior suspend must not stop them.
func TestNamespaces_DeleteSuspendedNamespace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	ns := uniqueNS()
	const (
		mtClassName = "SuspendedTenants"
		stClassName = "SuspendedDocs"
		mtAlias     = "TenantsAlias"
		stAlias     = "DocsAlias"
		tenant      = "warm"
		objTitle    = "written before the suspend"
	)
	qualifiedMT := ns + ":" + mtClassName
	qualifiedST := ns + ":" + stClassName

	homeNode := helper.CreateNamespace(t, ns, adminKey).HomeNode
	userKey := createNamespacedUser(t, "u1", ns, adminKey)

	// Two of each, so the cascade's alias and class loops run over more than one.
	setupMTClassInNs1(t, ns, mtClassName, userKey)
	setupClassInNs1(t, ns, stClassName, userKey)
	require.NoError(t, addTenantsAuth(t, mtClassName, []*models.Tenant{
		{Name: tenant, ActivityStatus: models.TenantActivityStatusHOT},
	}, userKey))
	created, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		Class: mtClassName, Tenant: tenant, Properties: map[string]any{"title": objTitle},
	}, userKey)
	require.NoError(t, err)
	helper.CreateAliasAuth(t, &models.Alias{Alias: mtAlias, Class: mtClassName}, userKey)
	helper.CreateAliasAuth(t, &models.Alias{Alias: stAlias, Class: stClassName}, userKey)

	requireShardsEventually(t, qualifiedMT, tenant)
	requireShardCountEventually(t, qualifiedST, 1)

	// The read-back proves the directory the reclaim check waits on held an object.
	obj, err := helper.GetObjectAuthWithTenant(t, qualifiedMT, created.ID, tenant, adminKey)
	require.NoError(t, err)
	props, ok := obj.Properties.(map[string]any)
	require.True(t, ok, "unexpected property shape %T", obj.Properties)
	require.Equal(t, objTitle, props["title"])

	// Without this, the reclaim check below would pass on an empty ./data.
	entriesBeforeDelete, err := dataDirEntries(ctx, nodeIndexFromName(t, homeNode))
	require.NoError(t, err)
	for _, class := range []string{qualifiedMT, qualifiedST} {
		require.Contains(t, entriesBeforeDelete, strings.ToLower(class),
			"class directory missing on home node %s before the delete", homeNode)
	}

	helper.SuspendNamespace(t, ns, adminKey)

	// The helper returns once GET reports 404, so the cascade removed the entry.
	helper.DeleteNamespace(t, ns, adminKey)

	for _, class := range []string{qualifiedMT, qualifiedST} {
		_, err := helper.GetClassAuthWithReturn(t, class, adminKey)
		var notFound *schema.SchemaObjectsGetNotFound
		require.ErrorAs(t, err, &notFound, "class %q must be gone, got %T: %v", class, err, err)
	}
	for _, alias := range []string{ns + ":" + mtAlias, ns + ":" + stAlias} {
		_, err := helper.GetAliasAuthWithReturn(t, alias, adminKey)
		var notFound *schema.AliasesGetAliasNotFound
		require.ErrorAs(t, err, &notFound, "alias %q must be gone, got %T: %v", alias, err, err)
	}

	requireClassDirsReclaimed(t, ctx, qualifiedMT, qualifiedST)
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
