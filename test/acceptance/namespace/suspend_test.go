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
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/namespaces"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// rawSchemaDump issues GET /v1/schema over plain HTTP and returns the status
// and body. The generated client's 401 responder has no payload — the spec
// declares 401 without a schema — so the rendered message is unreachable
// through it.
func rawSchemaDump(t *testing.T, key string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+sharedCompose.GetWeaviate().URI()+"/v1/schema", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

// TestNamespaces_SuspendResumeRoundTrip drives both endpoints through the
// generated client and reads the state back, since they return no body. An
// unregistered handler serves 501, so this also covers the wiring.
func TestNamespaces_SuspendResumeRoundTrip(t *testing.T) {
	t.Parallel()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	require.Equal(t, string(models.NamespaceStateActive), helper.GetNamespace(t, ns, adminKey).State)

	helper.SuspendNamespace(t, ns, adminKey)
	assert.Equal(t, string(models.NamespaceStateSuspended), helper.GetNamespace(t, ns, adminKey).State)

	// Re-suspending an already-suspended namespace is accepted, not a conflict.
	helper.SuspendNamespace(t, ns, adminKey)
	assert.Equal(t, string(models.NamespaceStateSuspended), helper.GetNamespace(t, ns, adminKey).State)

	helper.ResumeNamespace(t, ns, adminKey)
	assert.Equal(t, string(models.NamespaceStateActive), helper.GetNamespace(t, ns, adminKey).State)

	// Resuming an already-active namespace is likewise accepted.
	helper.ResumeNamespace(t, ns, adminKey)
	assert.Equal(t, string(models.NamespaceStateActive), helper.GetNamespace(t, ns, adminKey).State)
}

// TestNamespaces_SuspendRejectsTheNamespacesKeys pins that a suspended
// namespace's DB user stops authenticating on every replica, with copy that
// names no namespace, and that resuming restores access.
//
// Not parallel: the per-node check retargets the shared client.
func TestNamespaces_SuspendRejectsTheNamespacesKeys(t *testing.T) {
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	// The user must exist before the suspend: CreateUser routes through the
	// active-namespace gate and would be rejected afterwards.
	userKey := createNamespacedUser(t, "suspendee", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":suspendee", adminKey) })

	_, err := schemaDumpAs(t, userKey)
	require.NoError(t, err, "the key must work before the suspend")

	helper.SuspendNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

	// Each node's auth guard applies the flip independently, so a rejection
	// on the client's node says nothing about the others.
	originalURI := sharedCompose.GetWeaviate().URI()
	t.Cleanup(func() { helper.SetupClient(originalURI) })
	for i := 1; i <= 3; i++ {
		nodeURI := sharedCompose.GetWeaviateNode(i).URI()
		helper.SetupClient(nodeURI)
		require.Eventually(t, func() bool {
			_, err := schemaDumpAs(t, userKey)
			var unauth *schema.SchemaDumpUnauthorized
			return errors.As(err, &unauth)
		}, 10*time.Second, 50*time.Millisecond,
			"the key must stop authenticating on node %s", nodeURI)
	}
	helper.SetupClient(originalURI)

	// The generated 401 responder carries no payload, so read the body
	// directly: the rendered copy is the point of this assertion.
	status, body := rawSchemaDump(t, userKey)
	assert.Equal(t, http.StatusUnauthorized, status)
	assert.Contains(t, body, "instance suspended")
	assert.NotContains(t, body, ns, "the 401 must not disclose the namespace name")

	helper.ResumeNamespace(t, ns, adminKey)
	require.Eventually(t, func() bool {
		_, err := schemaDumpAs(t, userKey)
		return err == nil
	}, 10*time.Second, 50*time.Millisecond, "resume must restore access")
}

// TestNamespaces_SuspendIsScopedToOneNamespace pins that suspending one
// namespace leaves its neighbour untouched — the isolation the whole feature
// rests on.
func TestNamespaces_SuspendIsScopedToOneNamespace(t *testing.T) {
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	helper.SuspendNamespace(t, ns1, adminKey)
	// Resume before the fixture's cleanup so the shared compose is handed to
	// later tests with both namespaces active.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })

	require.Eventually(t, func() bool {
		_, err := schemaDumpAs(t, user1Key)
		var unauth *schema.SchemaDumpUnauthorized
		return errors.As(err, &unauth)
	}, 10*time.Second, 50*time.Millisecond, "the suspended namespace's key must be rejected")

	_, err := schemaDumpAs(t, user2Key)
	assert.NoError(t, err, "the other namespace's key must keep working")
	assert.Equal(t, string(models.NamespaceStateActive), helper.GetNamespace(t, ns2, adminKey).State)
}

// stateChangeEndpoints drives the error cases against both flip endpoints.
// They share one handler-side chokepoint, but each renders the outcome
// through its own generated responder, so the status a client actually sees
// has to be pinned per endpoint.
var stateChangeEndpoints = []struct {
	name          string
	call          func(t *testing.T, ns string) error
	notFound      func(error) bool
	unprocessable func(error) bool
}{
	{
		name: "suspend",
		call: func(t *testing.T, ns string) error {
			_, err := helper.Client(t).Namespaces.SuspendNamespace(
				namespaces.NewSuspendNamespaceParams().WithNamespaceID(ns),
				helper.CreateAuth(adminKey),
			)
			return err
		},
		notFound: func(err error) bool {
			var e *namespaces.SuspendNamespaceNotFound
			return errors.As(err, &e)
		},
		unprocessable: func(err error) bool {
			var e *namespaces.SuspendNamespaceUnprocessableEntity
			return errors.As(err, &e)
		},
	},
	{
		name: "resume",
		call: func(t *testing.T, ns string) error {
			_, err := helper.Client(t).Namespaces.ResumeNamespace(
				namespaces.NewResumeNamespaceParams().WithNamespaceID(ns),
				helper.CreateAuth(adminKey),
			)
			return err
		},
		notFound: func(err error) bool {
			var e *namespaces.ResumeNamespaceNotFound
			return errors.As(err, &e)
		},
		unprocessable: func(err error) bool {
			var e *namespaces.ResumeNamespaceUnprocessableEntity
			return errors.As(err, &e)
		},
	},
}

// TestNamespaces_StateChangeMissingNamespace pins the 404 an operator gets
// for a name that does not exist, rather than a 500 from the pre-read.
func TestNamespaces_StateChangeMissingNamespace(t *testing.T) {
	t.Parallel()
	for _, ep := range stateChangeEndpoints {
		t.Run(ep.name, func(t *testing.T) {
			t.Parallel()
			err := ep.call(t, uniqueNS())
			require.Error(t, err)
			require.True(t, ep.notFound(err), "expected 404, got %T: %v", err, err)
		})
	}
}

// TestNamespaces_StateChangeDeletingNamespace pins that a namespace already
// on its way out cannot be diverted into another state: deleting is terminal.
func TestNamespaces_StateChangeDeletingNamespace(t *testing.T) {
	t.Parallel()
	for _, ep := range stateChangeEndpoints {
		t.Run(ep.name, func(t *testing.T) {
			t.Parallel()
			// A namespace each: the deleting state is only observable until
			// cleanup finishes, so sharing one would race the second case
			// into a 404.
			err := ep.call(t, deletingNamespace(t))
			require.Error(t, err)
			// If cleanup removes the namespace between the DELETE and this
			// call, a 404 is equally acceptable; otherwise the flip against a
			// deleting namespace must be refused as unprocessable.
			require.True(t, ep.notFound(err) || ep.unprocessable(err),
				"deleting is terminal, so the flip must be refused as unprocessable (or 404 once cleanup wins); got %T: %v", err, err)
		})
	}
}

// deletingNamespace returns a namespace parked in the deleting state. A class
// gives the cleanup tick work to do, so the state stays observable long
// enough to flip against; returning on the 202 rather than waiting for
// removal is what keeps it reachable at all.
func deletingNamespace(t *testing.T) string {
	t.Helper()
	ns := uniqueNS()
	helper.CreateNamespace(t, ns, adminKey)

	// Classes are created by a namespaced user, never the global admin.
	userKey := createNamespacedUser(t, "classowner", ns, adminKey)
	helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, userKey)
	helper.DeleteNamespace(t, ns, adminKey, helper.WithoutWaitForCleanup())
	t.Cleanup(func() { helper.WaitForNamespaceGone(t, ns, adminKey, 30*time.Second) })
	return ns
}
