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

package namespace_limits

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// insertItem POSTs one object and returns the raw server error. The class
// is the short (unqualified) name; the server qualifies it via the
// namespaced principal's namespace.
func insertItem(t *testing.T, class, key string, i int) error {
	t.Helper()
	obj := &models.Object{
		ID:         strfmt.UUID(uuid.NewString()),
		Class:      class,
		Properties: map[string]any{"i": i},
	}
	return helper.CreateObjectAuth(t, obj, key)
}

// fillUntilQuota inserts until the chokepoint returns 429, then checks the
// USAGE_LIMIT_EXCEEDED payload. The loop tolerates a brief overshoot above
// objectCap: CountAsync excludes the memtable, so writes between two flush
// cycles are invisible to the cap check. maxInserts bounds the overshoot.
func fillUntilQuota(t *testing.T, class, key string) {
	t.Helper()
	const maxInserts = 200
	for i := 0; i < maxInserts; i++ {
		err := insertItem(t, class, key, i)
		if err == nil {
			continue
		}
		var tmr *objects.ObjectsCreateTooManyRequests
		require.ErrorAs(t, err, &tmr, "expected 429 USAGE_LIMIT_EXCEEDED, got %T: %v", err, err)
		require.NotNil(t, tmr.Payload)
		assert.Equal(t, "USAGE_LIMIT_EXCEEDED", tmr.Payload.ErrorCode)
		assert.Equal(t, "objects", tmr.Payload.Limit)
		assert.EqualValues(t, objectCap, tmr.Payload.Value)
		return
	}
	t.Fatalf("object cap %d did not fire within %d inserts", objectCap, maxInserts)
}

// TestObjectLimitEnforcedPerNamespace pins two namespaces to the same node
// so the cap is per namespace, not per node: after alpha hits its cap,
// beta can still write.
func TestObjectLimitEnforcedPerNamespace(t *testing.T) {
	const (
		nsA      = "alpha"
		nsB      = "beta"
		homeNode = "weaviate-0"
	)

	helper.CreateNamespaceWithHomeNode(t, nsA, homeNode, adminKey)
	helper.CreateNamespaceWithHomeNode(t, nsB, homeNode, adminKey)
	t.Cleanup(func() {
		helper.DeleteNamespace(t, nsA, adminKey)
		helper.DeleteNamespace(t, nsB, adminKey)
	})

	userA := createNamespacedUser(t, "u", nsA)
	userB := createNamespacedUser(t, "u", nsB)
	t.Cleanup(func() {
		helper.DeleteUser(t, nsA+":u", adminKey)
		helper.DeleteUser(t, nsB+":u", adminKey)
	})

	helper.CreateClassAuth(t, &models.Class{Class: "Items"}, userA)
	helper.CreateClassAuth(t, &models.Class{Class: "Items"}, userB)
	t.Cleanup(func() {
		helper.DeleteClassAuth(t, nsA+":Items", adminKey)
		helper.DeleteClassAuth(t, nsB+":Items", adminKey)
	})

	t.Run("alpha fills to its own cap", func(t *testing.T) {
		fillUntilQuota(t, "Items", userA)
	})

	t.Run("beta is unaffected by alpha's cap", func(t *testing.T) {
		require.NoError(t, insertItem(t, "Items", userB, 0),
			"namespace beta should still have room — cap is per namespace")
	})
}

// TestObjectLimitFromNonHomeNode pins the namespace to a node the test
// client is not bound to, so every insert is forwarded; the quota still
// fires at the home node.
func TestObjectLimitFromNonHomeNode(t *testing.T) {
	const (
		ns       = "forwarded"
		homeNode = "weaviate-1"
	)

	// Guard against a future helper change that would silently make the
	// test client bind to the home node and turn this into a local-write
	// test. setup_test.go calls helper.SetupClient(GetWeaviate().URI()),
	// which is weaviate-0; assert that name doesn't match the home_node.
	require.NotEqual(t, homeNode, sharedCompose.GetWeaviate().Name(),
		"test invariant: the client must enter on a node other than home_node so writes are forwarded")

	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u", ns)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u", adminKey) })

	helper.CreateClassAuth(t, &models.Class{Class: "Items"}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Items", adminKey) })

	fillUntilQuota(t, "Items", userKey)
}

// TestUpdatesAtQuotaRejected regression-guards the documented behaviour
// that PUTs also fail at full quota (the chokepoint covers updates).
func TestUpdatesAtQuotaRejected(t *testing.T) {
	const (
		ns       = "updates"
		homeNode = "weaviate-2"
	)

	helper.CreateNamespaceWithHomeNode(t, ns, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u", ns)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u", adminKey) })

	helper.CreateClassAuth(t, &models.Class{Class: "Items"}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Items", adminKey) })

	// Seed one object to target with the update, then fill the quota.
	target := strfmt.UUID(uuid.NewString())
	require.NoError(t, helper.CreateObjectAuth(t, &models.Object{
		ID:         target,
		Class:      "Items",
		Properties: map[string]any{"i": 0},
	}, userKey))

	fillUntilQuota(t, "Items", userKey)

	_, err := helper.Client(t).Objects.ObjectsClassPut(
		objects.NewObjectsClassPutParams().
			WithClassName("Items").
			WithID(target).
			WithBody(&models.Object{
				ID:         target,
				Class:      "Items",
				Properties: map[string]any{"i": 999},
			}),
		helper.CreateAuth(userKey),
	)
	require.Error(t, err, "update at quota must be rejected")
	require.True(t, isQuotaExceeded(err),
		"expected 429 USAGE_LIMIT_EXCEEDED on update at quota, got %T: %v", err, err)
}
