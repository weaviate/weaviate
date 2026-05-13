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

	// Alias update/delete inherit the same namespace-aware qualification as
	// AddAlias. The next four tests pin two contracts: (a) global operators
	// supplying a malformed namespace prefix get a clean 422, not a NotFound
	// or 500; (b) namespaced callers can address their own aliases by the
	// short name, exactly mirroring the create path.
	t.Run("global admin alias delete with wrong-case namespace prefix returns 422", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasCaseDeleteTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasCaseDeleteTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasCaseDelete", Class: "AliasCaseDeleteTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasCaseDelete", helper.CreateAuth(adminKey))

		_, err := helper.DeleteAliasAuthWithReturn(t, "Customer1:AliasCaseDelete", adminKey)
		require.Error(t, err)
		var unproc *schema.AliasesDeleteUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesDeleteUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "invalid namespace prefix")

		// Alias survives the malformed-prefix attempt.
		got := helper.GetAliasWithAuthz(t, "customer1:AliasCaseDelete", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:AliasCaseDelete", got.Alias)
	})

	t.Run("global admin alias update with wrong-case namespace prefix returns 422", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasCaseUpdateA"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasCaseUpdateA", adminKey)
		helper.CreateClassAuth(t, &models.Class{Class: "AliasCaseUpdateB"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasCaseUpdateB", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasCaseUpdate", Class: "AliasCaseUpdateA"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasCaseUpdate", helper.CreateAuth(adminKey))

		// Wrong case in the alias path component is rejected.
		_, err := helper.UpdateAliasAuthWithReturn(t, "Customer1:AliasCaseUpdate", "customer1:AliasCaseUpdateB", adminKey)
		require.Error(t, err)
		var unproc *schema.AliasesUpdateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesUpdateUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "invalid namespace prefix")

		// Wrong case in the target class is also rejected.
		_, err = helper.UpdateAliasAuthWithReturn(t, "customer1:AliasCaseUpdate", "Customer1:AliasCaseUpdateB", adminKey)
		require.Error(t, err)
		require.True(t, errors.As(err, &unproc), "expected AliasesUpdateUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "invalid namespace prefix")

		// Alias still points at its original target.
		got := helper.GetAliasWithAuthz(t, "customer1:AliasCaseUpdate", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:AliasCaseUpdateA", got.Class)
	})

	t.Run("namespaced caller deletes its own alias via short name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasShortDeleteTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasShortDeleteTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasShortDelete", Class: "AliasShortDeleteTarget"}, user1Key)

		// Short name path is qualified by the principal's namespace, so the
		// underlying "customer1:AliasShortDelete" alias is found and removed.
		_, err := helper.DeleteAliasAuthWithReturn(t, "AliasShortDelete", user1Key)
		require.NoError(t, err)

		// Lookup against the qualified name returns 404 after delete.
		_, err = helper.Client(t).Schema.AliasesGetAlias(
			schema.NewAliasesGetAliasParams().WithAliasName("customer1:AliasShortDelete"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
	})

	t.Run("namespaced caller updates its own alias via short name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasShortUpdateA"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasShortUpdateA", adminKey)
		helper.CreateClassAuth(t, &models.Class{Class: "AliasShortUpdateB"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasShortUpdateB", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasShortUpdate", Class: "AliasShortUpdateA"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasShortUpdate", helper.CreateAuth(adminKey))

		// Short names on both the alias and target are qualified with the
		// caller's namespace; the alias re-points to the qualified target.
		_, err := helper.UpdateAliasAuthWithReturn(t, "AliasShortUpdate", "AliasShortUpdateB", user1Key)
		require.NoError(t, err)

		got := helper.GetAliasWithAuthz(t, "customer1:AliasShortUpdate", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:AliasShortUpdateB", got.Class)
	})

	// Global operators do not have a namespace, so qualification is a no-op
	// for them — the qualified name in the URL is what reaches the schema
	// lookup. These tests confirm that path works end-to-end.
	t.Run("global admin deletes alias via qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AdminAliasQualifiedDeleteTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AdminAliasQualifiedDeleteTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AdminAliasQualifiedDelete", Class: "AdminAliasQualifiedDeleteTarget"}, user1Key)

		_, err := helper.DeleteAliasAuthWithReturn(t, "customer1:AdminAliasQualifiedDelete", adminKey)
		require.NoError(t, err)

		_, err = helper.Client(t).Schema.AliasesGetAlias(
			schema.NewAliasesGetAliasParams().WithAliasName("customer1:AdminAliasQualifiedDelete"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
	})

	t.Run("global admin updates alias via qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AdminAliasQualifiedUpdateA"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AdminAliasQualifiedUpdateA", adminKey)
		helper.CreateClassAuth(t, &models.Class{Class: "AdminAliasQualifiedUpdateB"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AdminAliasQualifiedUpdateB", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AdminAliasQualifiedUpdate", Class: "AdminAliasQualifiedUpdateA"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AdminAliasQualifiedUpdate", helper.CreateAuth(adminKey))

		_, err := helper.UpdateAliasAuthWithReturn(t, "customer1:AdminAliasQualifiedUpdate", "customer1:AdminAliasQualifiedUpdateB", adminKey)
		require.NoError(t, err)

		got := helper.GetAliasWithAuthz(t, "customer1:AdminAliasQualifiedUpdate", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:AdminAliasQualifiedUpdateB", got.Class)
	})

	// Read paths (GET /aliases/{name} and GET /aliases?class=) qualify in
	// the same way the write paths do: namespaced callers reach their own
	// aliases via short names, global operators by the qualified name, and
	// malformed prefixes surface as 422.
	t.Run("namespaced caller gets its own alias via short name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasShortGetTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasShortGetTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasShortGet", Class: "AliasShortGetTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasShortGet", helper.CreateAuth(adminKey))

		// Short name is qualified with the principal's namespace.
		resp, err := helper.GetAliasAuthWithReturn(t, "AliasShortGet", user1Key)
		require.NoError(t, err)
		require.Equal(t, "customer1:AliasShortGet", resp.Payload.Alias)
		require.Equal(t, "customer1:AliasShortGetTarget", resp.Payload.Class)
	})

	t.Run("global admin alias get with wrong-case namespace prefix returns 422", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasCaseGetTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasCaseGetTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasCaseGet", Class: "AliasCaseGetTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasCaseGet", helper.CreateAuth(adminKey))

		_, err := helper.GetAliasAuthWithReturn(t, "Customer1:AliasCaseGet", adminKey)
		require.Error(t, err)
		var unproc *schema.AliasesGetAliasUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesGetAliasUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "invalid namespace prefix")

		// Correct casing still resolves.
		got := helper.GetAliasWithAuthz(t, "customer1:AliasCaseGet", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:AliasCaseGet", got.Alias)
	})

	t.Run("namespaced caller submitting qualified alias name to GET is rejected", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasQualifiedGetTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasQualifiedGetTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasQualifiedGet", Class: "AliasQualifiedGetTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasQualifiedGet", helper.CreateAuth(adminKey))

		// Namespaced principals are not allowed to type a "<ns>:" prefix —
		// the resolver adds their namespace automatically. Generic 422.
		_, err := helper.GetAliasAuthWithReturn(t, "customer1:AliasQualifiedGet", user1Key)
		require.Error(t, err)
		var unproc *schema.AliasesGetAliasUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesGetAliasUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "is not a valid class name")
	})

	t.Run("namespaced caller lists aliases filtered by short class name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasListByClass"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasListByClass", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasListByClassA", Class: "AliasListByClass"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasListByClassA", helper.CreateAuth(adminKey))
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasListByClassB", Class: "AliasListByClass"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasListByClassB", helper.CreateAuth(adminKey))

		// Short class filter must be qualified to customer1:AliasListByClass.
		class := "AliasListByClass"
		resp, err := helper.GetAliasesAuthWithReturn(t, &class, user1Key)
		require.NoError(t, err)
		require.Len(t, resp.Payload.Aliases, 2)
		gotAliases := map[string]string{}
		for _, a := range resp.Payload.Aliases {
			gotAliases[a.Alias] = a.Class
		}
		assert.Equal(t, "customer1:AliasListByClass", gotAliases["customer1:AliasListByClassA"])
		assert.Equal(t, "customer1:AliasListByClass", gotAliases["customer1:AliasListByClassB"])
	})

	t.Run("global admin alias list with wrong-case class filter returns 422", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AliasListCaseTarget"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AliasListCaseTarget", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasListCase", Class: "AliasListCaseTarget"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasListCase", helper.CreateAuth(adminKey))

		class := "Customer1:AliasListCaseTarget"
		_, err := helper.GetAliasesAuthWithReturn(t, &class, adminKey)
		require.Error(t, err)
		var unproc *schema.AliasesGetUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesGetUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "invalid namespace prefix")
	})

	t.Run("alias list is namespace-isolated when both namespaces share a short alias name", func(t *testing.T) {
		for _, key := range []string{user1Key, user2Key} {
			helper.CreateClassAuth(t, &models.Class{Class: "AliasListIsoTarget"}, key)
			helper.CreateAliasAuth(t, &models.Alias{Alias: "AliasListIso", Class: "AliasListIsoTarget"}, key)
		}
		defer helper.DeleteClassAuth(t, "customer1:AliasListIsoTarget", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:AliasListIsoTarget", adminKey)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AliasListIso", helper.CreateAuth(adminKey))
		defer helper.DeleteAliasWithAuthz(t, "customer2:AliasListIso", helper.CreateAuth(adminKey))

		// user1 lists without a class filter and only sees their namespace's
		// alias. The schema layer is namespace-agnostic, so this exercises
		// the usecase-level namespace filter (RBAC is off in this suite).
		resp, err := helper.GetAliasesAuthWithReturn(t, nil, user1Key)
		require.NoError(t, err)
		seen := map[string]bool{}
		for _, a := range resp.Payload.Aliases {
			seen[a.Alias] = true
		}
		assert.True(t, seen["customer1:AliasListIso"], "user1 should see its own alias")
		assert.False(t, seen["customer2:AliasListIso"], "user1 should not see customer2's alias")
	})

	t.Run("global admin lists aliases across namespaces", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "AdminListA"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:AdminListA", adminKey)
		helper.CreateClassAuth(t, &models.Class{Class: "AdminListB"}, user2Key)
		defer helper.DeleteClassAuth(t, "customer2:AdminListB", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AdminListAlphaA", Class: "AdminListA"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:AdminListAlphaA", helper.CreateAuth(adminKey))
		helper.CreateAliasAuth(t, &models.Alias{Alias: "AdminListAlphaB", Class: "AdminListB"}, user2Key)
		defer helper.DeleteAliasWithAuthz(t, "customer2:AdminListAlphaB", helper.CreateAuth(adminKey))

		resp, err := helper.GetAliasesAuthWithReturn(t, nil, adminKey)
		require.NoError(t, err)
		seen := map[string]bool{}
		for _, a := range resp.Payload.Aliases {
			seen[a.Alias] = true
		}
		assert.True(t, seen["customer1:AdminListAlphaA"], "admin should see customer1's alias")
		assert.True(t, seen["customer2:AdminListAlphaB"], "admin should see customer2's alias")
	})
}
