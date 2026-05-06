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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	objectsCli "github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func createMovies(t *testing.T, key string) {
	t.Helper()
	class := &models.Class{Class: "Movies"}
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, helper.CreateAuth(key))
	require.NoError(t, err)
}

func createNamespacedUser(t *testing.T, userID, ns, adminKey string) string {
	t.Helper()
	resp, err := helper.Client(t).Users.CreateUser(
		users.NewCreateUserParams().WithUserID(userID).WithBody(users.CreateUserBody{Namespace: ns}),
		helper.CreateAuth(adminKey),
	)
	require.NoError(t, err)
	require.NotNil(t, resp.Payload.Apikey)
	return *resp.Payload.Apikey
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
		createMovies(t, user1Key)
		createMovies(t, user2Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:Movies", adminKey)

		// Admin (global) sees both qualified collections.
		gotClass1 := helper.GetClassAuth(t, "customer1:Movies", adminKey)
		require.Equal(t, "customer1:Movies", gotClass1.Class)
		gotClass2 := helper.GetClassAuth(t, "customer2:Movies", adminKey)
		require.Equal(t, "customer2:Movies", gotClass2.Class)

		// user1: Films -> Movies. The handler qualifies both to customer1:.
		alias1 := &models.Alias{Alias: "Films", Class: "Movies"}
		_, err := helper.Client(t).Schema.AliasesCreate(
			schema.NewAliasesCreateParams().WithBody(alias1),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		// user2 can independently create the same short alias name.
		alias2 := &models.Alias{Alias: "Films", Class: "Movies"}
		_, err = helper.Client(t).Schema.AliasesCreate(
			schema.NewAliasesCreateParams().WithBody(alias2),
			helper.CreateAuth(user2Key),
		)
		require.NoError(t, err)

		// Admin can see both qualified aliases exist.
		got1 := helper.GetAliasWithAuthz(t, "customer1:Films", helper.CreateAuth(adminKey))
		require.Equal(t, "customer1:Films", got1.Alias)
		require.Equal(t, "customer1:Movies", got1.Class)
		got2 := helper.GetAliasWithAuthz(t, "customer2:Films", helper.CreateAuth(adminKey))
		require.Equal(t, "customer2:Films", got2.Alias)
		require.Equal(t, "customer2:Movies", got2.Class)

		// Cleanup is best-effort; the compose teardown also wipes state.
		_, _ = helper.Client(t).Schema.AliasesDelete(
			schema.NewAliasesDeleteParams().WithAliasName("customer1:Films"),
			helper.CreateAuth(adminKey),
		)
		_, _ = helper.Client(t).Schema.AliasesDelete(
			schema.NewAliasesDeleteParams().WithAliasName("customer2:Films"),
			helper.CreateAuth(adminKey),
		)
	})

	t.Run("end-to-end: insert and get object via alias, with cross-namespace isolation", func(t *testing.T) {
		// Create the same class name in both namespaces so the only thing
		// keeping user2 from reading user1's object is the resolver.
		for _, key := range []string{user1Key, user2Key} {
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "E2EFilmsTarget"}),
				helper.CreateAuth(key),
			)
			require.NoError(t, err)
		}
		defer helper.DeleteClassAuth(t, "customer1:E2EFilmsTarget", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:E2EFilmsTarget", adminKey)

		// Both namespaces get the same short alias name pointing at their own copy.
		for _, key := range []string{user1Key, user2Key} {
			_, err := helper.Client(t).Schema.AliasesCreate(
				schema.NewAliasesCreateParams().WithBody(&models.Alias{Alias: "E2EFilmsAlias", Class: "E2EFilmsTarget"}),
				helper.CreateAuth(key),
			)
			require.NoError(t, err)
		}
		defer helper.Client(t).Schema.AliasesDelete(
			schema.NewAliasesDeleteParams().WithAliasName("customer1:E2EFilmsAlias"),
			helper.CreateAuth(adminKey),
		)
		defer helper.Client(t).Schema.AliasesDelete(
			schema.NewAliasesDeleteParams().WithAliasName("customer2:E2EFilmsAlias"),
			helper.CreateAuth(adminKey),
		)

		// user1 inserts via the alias name; on disk it lands as customer1:E2EFilmsTarget.
		obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class:      "E2EFilmsAlias",
			Properties: map[string]any{"title": "Inception"},
		}, user1Key)
		require.NoError(t, err)
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
		class := &models.Class{Class: "Movies"}
		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, helper.CreateAuth(adminKey))
		require.Error(t, err)
		var forbidden *schema.SchemaObjectsCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected SchemaObjectsCreateForbidden, got %T: %v", err, err)
	})

	t.Run("namespaced caller submitting class name with ':' rejected", func(t *testing.T) {
		class := &models.Class{Class: "Customer2:Movies"}
		params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, helper.CreateAuth(user1Key))
		require.Error(t, err)
		var unproc *schema.SchemaObjectsCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected SchemaObjectsCreateUnprocessableEntity, got %T: %v", err, err)
		assert.Contains(t, unproc.Payload.Error[0].Message, "is not a valid class name")
	})

	t.Run("namespaced caller submitting alias with ':' in target rejected", func(t *testing.T) {
		alias := &models.Alias{Alias: "Films", Class: "Customer2:Movies"}
		_, err := helper.Client(t).Schema.AliasesCreate(
			schema.NewAliasesCreateParams().WithBody(alias),
			helper.CreateAuth(user1Key),
		)
		require.Error(t, err)
		var unproc *schema.AliasesCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesCreateUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("namespaced caller submitting alias with ':' in alias name rejected", func(t *testing.T) {
		alias := &models.Alias{Alias: "Customer2:Films", Class: "Movies"}
		_, err := helper.Client(t).Schema.AliasesCreate(
			schema.NewAliasesCreateParams().WithBody(alias),
			helper.CreateAuth(user1Key),
		)
		require.Error(t, err)
		var unproc *schema.AliasesCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected AliasesCreateUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("global admin rejected on alias create with NS-enabled", func(t *testing.T) {
		alias := &models.Alias{Alias: "Films", Class: "Movies"}
		_, err := helper.Client(t).Schema.AliasesCreate(
			schema.NewAliasesCreateParams().WithBody(alias),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
		var forbidden *schema.AliasesCreateForbidden
		require.True(t, errors.As(err, &forbidden), "expected AliasesCreateForbidden, got %T: %v", err, err)
	})

	t.Run("namespaced caller deletes its class via short name", func(t *testing.T) {
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(
			schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "DeleteMe"}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		// Confirm the qualified class exists before delete.
		got := helper.GetClassAuth(t, "customer1:DeleteMe", adminKey)
		require.Equal(t, "customer1:DeleteMe", got.Class)

		// Short-name delete from user1 must qualify to customer1:DeleteMe.
		helper.DeleteClassAuth(t, "DeleteMe", user1Key)

		_, err = helper.Client(t).Schema.SchemaObjectsGet(
			schema.NewSchemaObjectsGetParams().WithClassName("customer1:DeleteMe"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
	})

	t.Run("delete is namespace-isolated when both namespaces share a short name", func(t *testing.T) {
		// Both namespaces independently create the same short class name.
		for _, key := range []string{user1Key, user2Key} {
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "Shared"}),
				helper.CreateAuth(key),
			)
			require.NoError(t, err)
		}
		defer helper.DeleteClassAuth(t, "customer2:Shared", adminKey)

		// user1 deletes via the short name — only customer1:Shared should go.
		helper.DeleteClassAuth(t, "Shared", user1Key)

		_, err := helper.Client(t).Schema.SchemaObjectsGet(
			schema.NewSchemaObjectsGetParams().WithClassName("customer1:Shared"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)

		got := helper.GetClassAuth(t, "customer2:Shared", adminKey)
		require.Equal(t, "customer2:Shared", got.Class)
	})

	t.Run("global admin deletes by qualified class name", func(t *testing.T) {
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(
			schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "AdminDelete"}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		helper.DeleteClassAuth(t, "customer1:AdminDelete", adminKey)

		_, err = helper.Client(t).Schema.SchemaObjectsGet(
			schema.NewSchemaObjectsGetParams().WithClassName("customer1:AdminDelete"),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)
	})

	t.Run("namespaced caller updates its class via short path name", func(t *testing.T) {
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(
			schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "UpdateMe", Description: "v1"}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		defer helper.DeleteClassAuth(t, "customer1:UpdateMe", adminKey)

		// GET returns the qualified class; modify the description and PUT
		// with the short name in the path.
		got := helper.GetClassAuth(t, "customer1:UpdateMe", adminKey)
		require.Equal(t, "customer1:UpdateMe", got.Class)
		got.Description = "v2"

		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(
			schema.NewSchemaObjectsUpdateParams().WithClassName("UpdateMe").WithObjectClass(got),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		after := helper.GetClassAuth(t, "customer1:UpdateMe", adminKey)
		assert.Equal(t, "v2", after.Description)
	})

	t.Run("update is namespace-isolated when both namespaces share a short name", func(t *testing.T) {
		for _, key := range []string{user1Key, user2Key} {
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{
					Class: "SharedUpdate", Description: "v1",
				}),
				helper.CreateAuth(key),
			)
			require.NoError(t, err)
		}
		defer helper.DeleteClassAuth(t, "customer1:SharedUpdate", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:SharedUpdate", adminKey)

		// user1's short-name update must only touch customer1:SharedUpdate.
		toUpdate := helper.GetClassAuth(t, "customer1:SharedUpdate", adminKey)
		toUpdate.Description = "v2"
		_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
			schema.NewSchemaObjectsUpdateParams().WithClassName("SharedUpdate").WithObjectClass(toUpdate),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		after1 := helper.GetClassAuth(t, "customer1:SharedUpdate", adminKey)
		assert.Equal(t, "v2", after1.Description)
		after2 := helper.GetClassAuth(t, "customer2:SharedUpdate", adminKey)
		assert.Equal(t, "v1", after2.Description)
	})

	t.Run("global admin updates by qualified class name", func(t *testing.T) {
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(
			schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "AdminUpdate", Description: "v1"}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		defer helper.DeleteClassAuth(t, "customer1:AdminUpdate", adminKey)

		cls := helper.GetClassAuth(t, "customer1:AdminUpdate", adminKey)
		cls.Description = "by-admin"
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(
			schema.NewSchemaObjectsUpdateParams().WithClassName("customer1:AdminUpdate").WithObjectClass(cls),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)

		after := helper.GetClassAuth(t, "customer1:AdminUpdate", adminKey)
		assert.Equal(t, "by-admin", after.Description)
	})
}
