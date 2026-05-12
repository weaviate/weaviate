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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	objectsCli "github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func createNamespacedUser(t *testing.T, userID, ns, adminKey string) string {
	t.Helper()

	// AddNamespace no longer waits for local apply, so the follower the test
	// client talks to may not yet see the namespace in its local controller
	// when CreateUser arrives. The createUser handler's fast-path Exists()
	// check then 422s with "namespace does not exist"; the local check 422s
	// before any RAFT command is sent, so retries are safe.
	var apikey string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(userID).WithBody(users.CreateUserBody{Namespace: ns}),
			helper.CreateAuth(adminKey),
		)
		if !assert.NoError(c, err) {
			return
		}
		if !assert.NotNil(c, resp.Payload.Apikey) {
			return
		}
		apikey = *resp.Payload.Apikey
	}, 10*time.Second, 50*time.Millisecond, "user %q could not be created", userID)

	// On a multi-node cluster CreateUser is RAFT-forwarded to the leader and
	// returns once the leader applies the FSM entry; the follower the test
	// client talks to may still be replicating, so the very next request
	// authenticated with apikey can transiently 401. Poll a cheap auth-bearing
	// endpoint until the new key is recognized locally. Same pattern as
	// helper.CreateNamespace and retryOnAliasLag, just for dynamic users.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := helper.Client(t).Users.GetOwnInfo(
			users.NewGetOwnInfoParams(), helper.CreateAuth(apikey))
		assert.NoError(c, err)
	}, 10*time.Second, 50*time.Millisecond, "user %q apikey not recognized after create", userID)

	return apikey
}

// TestNamespaces_CollectionAndAliasCreate exercises the inline qualification
// added to AddClass / AddAlias, as well as the read path with namespace resolution.
// RBAC is off so namespaced DB users reach the handler unconditionally; the only
// gating in play is the handler-level IsGlobalOperator/Namespace check plus the
// entity-name validators.
func TestNamespaces_CollectionAndAlias(t *testing.T) {
	const (
		ns1 = "customer1"
		ns2 = "customer2"
	)

	helper.CreateNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)
	defer helper.DeleteNamespace(t, ns1, adminKey)
	defer helper.DeleteNamespace(t, ns2, adminKey)

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	user2Key := createNamespacedUser(t, "u2", ns2, adminKey)
	t.Cleanup(func() {
		helper.DeleteUser(t, ns1+":u1", adminKey)
		helper.DeleteUser(t, ns2+":u2", adminKey)
	})

	t.Run("namespace-local alias create succeeds and is independent across namespaces", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user1Key)
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user2Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:Movies", adminKey)

		// Admin (global) sees both qualified collections.
		gotClass1 := helper.GetClassAuth(t, "customer1:Movies", adminKey)
		require.Equal(t, "customer1:Movies", gotClass1.Class)
		gotClass2 := helper.GetClassAuth(t, "customer2:Movies", adminKey)
		require.Equal(t, "customer2:Movies", gotClass2.Class)

		// user1: Films -> Movies. The handler qualifies both to customer1:.
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Films", Class: "Movies"}, user1Key)
		// user2 can independently create the same short alias name.
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Films", Class: "Movies"}, user2Key)

		// Admin can see both qualified aliases exist.
		got1 := helper.GetAliasWithAuthz(t, "customer1:Films", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:Films", got1.Alias)
		require.Equal(t, "customer1:Movies", got1.Class)
		got2 := helper.GetAliasWithAuthz(t, "customer2:Films", helper.CreateAuth(adminKey))
		require.Equal(t, "customer2:Films", got2.Alias)
		require.Equal(t, "customer2:Movies", got2.Class)

		defer helper.DeleteAliasWithAuthz(t, "customer1:Films", helper.CreateAuth(adminKey))
		defer helper.DeleteAliasWithAuthz(t, "customer2:Films", helper.CreateAuth(adminKey))
	})

	t.Run("end-to-end: insert and get object via alias, with cross-namespace isolation", func(t *testing.T) {
		// Create the same class name in both namespaces so the only thing
		// keeping user2 from reading user1's object is the resolver.
		// The "title" property is declared up front so this test does not
		// exercise auto-schema.
		for _, key := range []string{user1Key, user2Key} {
			helper.CreateClassAuth(t, &models.Class{
				Class: "E2EFilmsTarget",
				Properties: []*models.Property{
					{Name: "title", DataType: []string{"text"}},
				},
			}, key)
		}
		defer helper.DeleteClassAuth(t, "customer1:E2EFilmsTarget", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:E2EFilmsTarget", adminKey)

		// Both namespaces get the same short alias name pointing at their own copy.
		for _, key := range []string{user1Key, user2Key} {
			helper.CreateAliasAuth(t, &models.Alias{Alias: "E2EFilmsAlias", Class: "E2EFilmsTarget"}, key)
		}
		defer helper.DeleteAliasWithAuthz(t, "customer1:E2EFilmsAlias", helper.CreateAuth(adminKey))
		defer helper.DeleteAliasWithAuthz(t, "customer2:E2EFilmsAlias", helper.CreateAuth(adminKey))

		// user1 inserts via the alias name; on disk it lands as customer1:E2EFilmsTarget.
		// retryOnAliasLag absorbs the brief window where the alias entry has
		// been applied on the leader but the follower we are talking to has
		// not yet replicated it, so local alias resolution would 500.
		var obj *models.Object
		retryOnAliasLag(t, func() error {
			var err error
			obj, err = helper.CreateObjectWithResponseAuth(t, &models.Object{
				Class:      "E2EFilmsAlias",
				Properties: map[string]any{"title": "Inception"},
			}, user1Key)
			return err
		})
		require.NotEmpty(t, obj.ID)

		// user1 reads it back via the alias — Resolve qualifies + maps alias → target.
		got, err := helper.GetObjectAuth(t, "E2EFilmsAlias", obj.ID, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "customer1:E2EFilmsTarget", got.Class)
		assert.Equal(t, obj.ID, got.ID)
		propsGot, ok := got.Properties.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Inception", propsGot["title"])

		// Cross-namespace isolation: user2 asks for the same id under the same
		// short class name and the same short alias name. Both qualify to
		// customer2:* — different shard, no such object → 404.
		_, err = helper.GetObjectAuth(t, "E2EFilmsTarget", obj.ID, user2Key)
		require.Error(t, err)
		var nfClass *objectsCli.ObjectsClassGetNotFound
		require.True(t, errors.As(err, &nfClass), "expected ObjectsClassGetNotFound, got %T: %v", err, err)
		_, err = helper.GetObjectAuth(t, "E2EFilmsAlias", obj.ID, user2Key)
		require.Error(t, err)
		var nfAlias *objectsCli.ObjectsClassGetNotFound
		require.True(t, errors.As(err, &nfAlias), "expected ObjectsClassGetNotFound, got %T: %v", err, err)
	})

	t.Run("global admin rejected with 403 on NS-enabled cluster", func(t *testing.T) {
		_, err := helper.CreateClassAuthWithReturn(t, &models.Class{Class: "Movies"}, adminKey)
		require.Error(t, err)
		var forbidden *schema.SchemaObjectsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected SchemaObjectsCreateForbidden, got %T: %v", err, err)
	})

	t.Run("namespaced caller submitting class name with ':' rejected", func(t *testing.T) {
		_, err := helper.CreateClassAuthWithReturn(t, &models.Class{Class: "Customer2:Movies"}, user1Key)
		require.Error(t, err)
		var unproc *schema.SchemaObjectsCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected SchemaObjectsCreateUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "is not a valid class name")
	})

	t.Run("namespaced caller submitting alias with ':' in target rejected", func(t *testing.T) {
		_, err := helper.CreateAliasAuthWithReturn(t, &models.Alias{Alias: "Films", Class: "Customer2:Movies"}, user1Key)
		require.Error(t, err)
		var unproc *schema.AliasesCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesCreateUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("namespaced caller submitting alias with ':' in alias name rejected", func(t *testing.T) {
		_, err := helper.CreateAliasAuthWithReturn(t, &models.Alias{Alias: "Customer2:Films", Class: "Movies"}, user1Key)
		require.Error(t, err)
		var unproc *schema.AliasesCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesCreateUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("global admin rejected on alias create with NS-enabled", func(t *testing.T) {
		_, err := helper.CreateAliasAuthWithReturn(t, &models.Alias{Alias: "Films", Class: "Movies"}, adminKey)
		require.Error(t, err)
		var forbidden *schema.AliasesCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected AliasesCreateForbidden, got %T: %v", err, err)
	})

	t.Run("namespaced caller deletes its class via short name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "DeleteMe"}, user1Key)

		// Confirm the qualified class exists before delete.
		got := helper.GetClassAuth(t, "customer1:DeleteMe", adminKey)
		require.Equal(t, "customer1:DeleteMe", got.Class)

		// Short-name delete from user1 must qualify to customer1:DeleteMe.
		helper.DeleteClassAuth(t, "DeleteMe", user1Key)

		_, err := helper.GetClassAuthWithReturn(t, "customer1:DeleteMe", adminKey)
		require.Error(t, err)
	})

	t.Run("delete is namespace-isolated when both namespaces share a short name", func(t *testing.T) {
		// Both namespaces independently create the same short class name.
		for _, key := range []string{user1Key, user2Key} {
			helper.CreateClassAuth(t, &models.Class{Class: "Shared"}, key)
		}
		defer helper.DeleteClassAuth(t, "customer2:Shared", adminKey)

		// user1 deletes via the short name — only customer1:Shared should go.
		helper.DeleteClassAuth(t, "Shared", user1Key)

		_, err := helper.GetClassAuthWithReturn(t, "customer1:Shared", adminKey)
		require.Error(t, err)

		got := helper.GetClassAuth(t, "customer2:Shared", adminKey)
		require.Equal(t, "customer2:Shared", got.Class)
	})

	t.Run("global admin deletes by qualified class name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AdminDelete"}, user1Key)

		helper.DeleteClassAuth(t, "customer1:AdminDelete", adminKey)

		_, err := helper.GetClassAuthWithReturn(t, "customer1:AdminDelete", adminKey)
		require.Error(t, err)
	})

	t.Run("namespaced caller updates its class via short path name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "UpdateMe", Description: "v1"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:UpdateMe", adminKey)

		// GET returns the qualified class; modify the description and PUT
		// with the short name in the path.
		got := helper.GetClassAuth(t, "customer1:UpdateMe", adminKey)
		require.Equal(t, "customer1:UpdateMe", got.Class)
		got.Description = "v2"

		helper.UpdateClassAuth(t, "UpdateMe", got, user1Key)

		after := helper.GetClassAuth(t, "customer1:UpdateMe", adminKey)
		assert.Equal(t, "v2", after.Description)
	})

	t.Run("update is namespace-isolated when both namespaces share a short name", func(t *testing.T) {
		for _, key := range []string{user1Key, user2Key} {
			helper.CreateClassAuth(t, &models.Class{Class: "SharedUpdate", Description: "v1"}, key)
		}
		defer helper.DeleteClassAuth(t, "customer1:SharedUpdate", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:SharedUpdate", adminKey)

		// user1's short-name update must only touch customer1:SharedUpdate.
		toUpdate := helper.GetClassAuth(t, "customer1:SharedUpdate", adminKey)
		toUpdate.Description = "v2"
		helper.UpdateClassAuth(t, "SharedUpdate", toUpdate, user1Key)

		after1 := helper.GetClassAuth(t, "customer1:SharedUpdate", adminKey)
		assert.Equal(t, "v2", after1.Description)
		after2 := helper.GetClassAuth(t, "customer2:SharedUpdate", adminKey)
		assert.Equal(t, "v1", after2.Description)
	})

	t.Run("global admin updates by qualified class name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AdminUpdate", Description: "v1"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AdminUpdate", adminKey)

		cls := helper.GetClassAuth(t, "customer1:AdminUpdate", adminKey)
		cls.Description = "by-admin"
		helper.UpdateClassAuth(t, "customer1:AdminUpdate", cls, adminKey)

		after := helper.GetClassAuth(t, "customer1:AdminUpdate", adminKey)
		assert.Equal(t, "by-admin", after.Description)
	})

	// Schema-level operations (DELETE/UPDATE class) must never act on an
	// alias name. If they did, an alias would be a backdoor for dropping
	// or mutating its underlying class. These tests pin that contract on
	// a namespaced cluster.
	t.Run("namespaced caller cannot delete its class via an alias name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasDeleteTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasDeleteTarget", adminKey)

		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasDeleter", Class: "AliasDeleteTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasDeleter", helper.CreateAuth(adminKey))

		// DELETE by alias name must be a no-op on the underlying class.
		// The HTTP delete returns 200 even for non-existent classes, so we
		// verify the underlying class still exists afterwards.
		helper.DeleteClassAuth(t, "AliasDeleter", user1Key)

		got := helper.GetClassAuth(t, "customer1:AliasDeleteTarget", adminKey)
		require.Equal(t, "customer1:AliasDeleteTarget", got.Class)
	})

	t.Run("namespaced caller cannot update its class via an alias name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasUpdateTarget", Description: "v1"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasUpdateTarget", adminKey)

		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasUpdater", Class: "AliasUpdateTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasUpdater", helper.CreateAuth(adminKey))

		// Body carries the underlying class name (mimicking a caller that
		// fetched the qualified class and only swapped the URL path to the
		// alias). The handler must reject regardless because the URL path
		// resolves to "customer1:AliasUpdater", which is an alias, not a
		// class.
		body := helper.GetClassAuth(t, "customer1:AliasUpdateTarget", adminKey)
		body.Description = "v2"
		_, err := helper.UpdateClassAuthWithReturn(t, "AliasUpdater", body, user1Key)
		require.Error(t, err)

		after := helper.GetClassAuth(t, "customer1:AliasUpdateTarget", adminKey)
		assert.Equal(t, "v1", after.Description)
	})

	t.Run("global admin cannot delete a class via a qualified alias name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AdminAliasDeleteTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AdminAliasDeleteTarget", adminKey)

		helper.CreateAliasAuth(t, &models.Alias{Alias: "AdminAliasDeleter", Class: "AdminAliasDeleteTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AdminAliasDeleter", helper.CreateAuth(adminKey))

		helper.DeleteClassAuth(t, "customer1:AdminAliasDeleter", adminKey)

		got := helper.GetClassAuth(t, "customer1:AdminAliasDeleteTarget", adminKey)
		require.Equal(t, "customer1:AdminAliasDeleteTarget", got.Class)
	})

	// A delete with a wrong-case namespace prefix must be rejected at the
	// resolver boundary; the class must survive untouched. Without this
	// guard the qualify path accepted the casing variant, removed the data
	// directory, and left the schema entry behind — blocking recreation.
	t.Run("delete with wrong-case namespace prefix is rejected and class is untouched", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "CaseDelete"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:CaseDelete", adminKey)

		err := helper.DeleteClassAuthWithReturn(t, "Customer1:CaseDelete", adminKey)
		require.Error(t, err)

		got := helper.GetClassAuth(t, "customer1:CaseDelete", adminKey)
		require.Equal(t, "customer1:CaseDelete", got.Class)

		// Caller can still recreate after attempted bad-case delete fails,
		// confirming no partial deletion occurred.
		helper.DeleteClassAuth(t, "customer1:CaseDelete", adminKey)
		helper.CreateClassAuth(t, &models.Class{Class: "CaseDelete"}, user1Key)
	})

	// Read path: GET /schema/{className} with a wrong-case namespace prefix
	// must surface a 422 (not a 500), so the 422 contract has end-to-end
	// coverage on the read side too.
	t.Run("get with wrong-case namespace prefix returns 422", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "CaseGet"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:CaseGet", adminKey)

		_, err := helper.GetClassAuthWithReturn(t, "Customer1:CaseGet", adminKey)
		require.Error(t, err)
		var unproc *schema.SchemaObjectsGetUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected SchemaObjectsGetUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "invalid namespace prefix")

		// Correct casing still works — the class is reachable as registered.
		got := helper.GetClassAuth(t, "customer1:CaseGet", adminKey)
		require.Equal(t, "customer1:CaseGet", got.Class)
	})
}
