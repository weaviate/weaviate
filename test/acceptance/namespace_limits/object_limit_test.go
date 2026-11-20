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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// insertItem POSTs one object and returns the raw server error. The class
// is the short (unqualified) name; the server qualifies it via the
// namespaced principal's namespace.
func insertItem(t *testing.T, class, key string, i int) error {
	t.Helper()
	_, err := insertItemVia(t, helper.Client(t), class, key, i, "")
	return err
}

// insertItemVia inserts an item through c at consistency level cl and returns
// its ID. An empty cl sends no consistency level.
func insertItemVia(t *testing.T, c *client.Weaviate, class, key string, i int, cl types.ConsistencyLevel) (strfmt.UUID, error) {
	t.Helper()
	id := strfmt.UUID(uuid.NewString())
	obj := &models.Object{ID: id, Class: class, Properties: map[string]any{"i": i}}
	return id, helper.CreateObjectVia(t, c, obj, cl, key)
}

// requireObjectCapRefusal requires err to be the cap's 429 USAGE_LIMIT_EXCEEDED.
func requireObjectCapRefusal(t *testing.T, err error) {
	t.Helper()
	var tmr *objects.ObjectsCreateTooManyRequests
	require.ErrorAs(t, err, &tmr, "expected 429 USAGE_LIMIT_EXCEEDED, got %T: %v", err, err)
	require.NotNil(t, tmr.Payload)
	assert.Equal(t, "USAGE_LIMIT_EXCEEDED", tmr.Payload.ErrorCode)
	assert.Equal(t, "objects", tmr.Payload.Limit)
	assert.EqualValues(t, objectCap, tmr.Payload.Value)
}

// fillUntilQuota inserts at the default consistency level until the
// chokepoint returns 429.
func fillUntilQuota(t *testing.T, class, key string) {
	t.Helper()
	fillUntilQuotaAt(t, class, key, "")
}

// fillUntilQuotaAt inserts at consistency level cl until the chokepoint
// returns 429. Its deadline is wall time, not an insert count, because the cap
// fires only after a memtable flush, which a fixed count can outrun on fast CI.
func fillUntilQuotaAt(t *testing.T, class, key string, cl types.ConsistencyLevel) {
	t.Helper()
	const fillBudget = 30 * time.Second
	deadline := time.Now().Add(fillBudget)
	for i := 0; ; i++ {
		if time.Now().After(deadline) {
			t.Fatalf("object cap %d did not fire within %s (after %d inserts)", objectCap, fillBudget, i)
		}
		_, err := insertItemVia(t, helper.Client(t), class, key, i, cl)
		if err == nil {
			continue
		}
		requireObjectCapRefusal(t, err)
		return
	}
}

// TestObjectLimitEnforcedPerNamespace pins two namespaces to the same node
// so the cap is per namespace, not per node: after alpha hits its cap,
// beta can still write.
func TestObjectLimitEnforcedPerNamespace(t *testing.T) {
	t.Parallel()
	nsA, nsB := uniqueNS(), uniqueNS()
	const homeNode = "weaviate-0"

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
	t.Parallel()
	ns := uniqueNS()
	const homeNode = "weaviate-1"

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
	t.Parallel()
	ns := uniqueNS()
	const homeNode = "weaviate-2"

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
	var tmr *objects.ObjectsClassPutTooManyRequests
	require.ErrorAs(t, err, &tmr, "expected 429 USAGE_LIMIT_EXCEEDED, got %T: %v", err, err)
	require.NotNil(t, tmr.Payload)
	assert.Equal(t, "USAGE_LIMIT_EXCEEDED", tmr.Payload.ErrorCode)
	assert.Equal(t, "objects", tmr.Payload.Limit)
	assert.EqualValues(t, objectCap, tmr.Payload.Value)
}
