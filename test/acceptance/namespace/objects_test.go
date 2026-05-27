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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// twoNamespaces brings up customer1+customer2 plus a single namespaced
// DB user per namespace and returns their API keys. Cleanup is registered.
func twoNamespaces(t *testing.T) (string, string) {
	t.Helper()
	const (
		ns1 = "customer1"
		ns2 = "customer2"
	)
	helper.CreateNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)
	t.Cleanup(func() {
		helper.DeleteNamespace(t, ns1, adminKey)
		helper.DeleteNamespace(t, ns2, adminKey)
	})
	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	user2Key := createNamespacedUser(t, "u2", ns2, adminKey)
	t.Cleanup(func() {
		helper.DeleteUser(t, ns1+":u1", adminKey)
		helper.DeleteUser(t, ns2+":u2", adminKey)
	})
	return user1Key, user2Key
}

// setupClassInBothNamespaces creates a class with a single text "title"
// property under each user's namespace and registers cleanup of the
// qualified names.
func setupClassInBothNamespaces(t *testing.T, name, k1, k2 string) {
	t.Helper()
	for _, key := range []string{k1, k2} {
		helper.CreateClassAuth(t, &models.Class{
			Class: name,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
			},
		}, key)
	}
	t.Cleanup(func() {
		helper.DeleteClassAuth(t, "customer1:"+name, adminKey)
		helper.DeleteClassAuth(t, "customer2:"+name, adminKey)
	})
}

// setupClassInNs1 creates a class with a single text "title" property under
// user1Key only and registers cleanup of the qualified name.
func setupClassInNs1(t *testing.T, name, key string) {
	t.Helper()
	helper.CreateClassAuth(t, &models.Class{
		Class: name,
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
	}, key)
	t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+name, adminKey) })
}

// seedTwo writes the same UUID under the same short class name in both
// namespaces with different "title" content.
func seedTwo(t *testing.T, class string, id strfmt.UUID, ns1Title, ns2Title, k1, k2 string) {
	t.Helper()
	_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: id, Class: class, Properties: map[string]any{"title": ns1Title},
	}, k1)
	require.NoError(t, err)
	_, err = helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: id, Class: class, Properties: map[string]any{"title": ns2Title},
	}, k2)
	require.NoError(t, err)
}

// TestNamespaces_ObjectLifecycle exercises namespacing.Resolve fan-out across
// the object REST endpoints (add/get/update/merge/delete/head/list/validate)
// plus the contract for double-prefix and global-principal access.
func TestNamespaces_ObjectLifecycle(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	t.Run("add and get with short, lowercase class name", func(t *testing.T) {
		// Submit lowercased on every hop. UppercaseClassName runs before the
		// namespace prefix is glued on, so the qualified name on disk is
		// "<namespace>:E2emovies".
		const (
			short      = "e2emovies"
			shortClass = "E2emovies"
			qualified1 = "customer1:E2emovies"
			qualified2 = "customer2:E2emovies"
		)
		setupClassInBothNamespaces(t, short, user1Key, user2Key)

		id := strfmt.UUID("11111111-2222-3333-4444-555555555555")
		seedTwo(t, short, id, "The Matrix", "Inception", user1Key, user2Key)

		// Namespaced caller sees the short class name (stripped).
		got1, err := helper.GetObjectAuth(t, short, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, shortClass, got1.Class)
		assert.Equal(t, "The Matrix", got1.Properties.(map[string]any)["title"])

		got2, err := helper.GetObjectAuth(t, short, id, user2Key)
		require.NoError(t, err)
		assert.Equal(t, shortClass, got2.Class)
		assert.Equal(t, "Inception", got2.Properties.(map[string]any)["title"])

		// Admin's raw schema view shows both qualified names exist.
		assert.Equal(t, qualified1, helper.GetClassAuth(t, qualified1, adminKey).Class)
		assert.Equal(t, qualified2, helper.GetClassAuth(t, qualified2, adminKey).Class)
	})

	t.Run("update / merge / delete on ns1 leave ns2 untouched", func(t *testing.T) {
		const class = "MutationTarget"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		id := strfmt.UUID("aaaaaaaa-1111-1111-1111-111111111111")
		seedTwo(t, class, id, "v1-ns1", "v1-ns2", user1Key, user2Key)

		// PUT in ns1.
		_, err := helper.Client(t).Objects.ObjectsClassPut(
			objects.NewObjectsClassPutParams().WithClassName(class).WithID(id).
				WithBody(&models.Object{ID: id, Class: class, Properties: map[string]any{"title": "v2-ns1"}}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		got1, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "v2-ns1", got1.Properties.(map[string]any)["title"])

		// PATCH in ns1.
		_, err = helper.Client(t).Objects.ObjectsClassPatch(
			objects.NewObjectsClassPatchParams().WithClassName(class).WithID(id).
				WithBody(&models.Object{Class: class, Properties: map[string]any{"title": "v3-ns1"}}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		got1, err = helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "v3-ns1", got1.Properties.(map[string]any)["title"])

		// DELETE in ns1.
		_, err = helper.Client(t).Objects.ObjectsClassDelete(
			objects.NewObjectsClassDeleteParams().WithClassName(class).WithID(id),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		_, err = helper.GetObjectAuth(t, class, id, user1Key)
		require.Error(t, err)
		var nfGet *objects.ObjectsClassGetNotFound
		require.True(t, errors.As(err, &nfGet), "expected ObjectsClassGetNotFound, got %T: %v", err, err)

		// ns2 row never moved.
		got2, err := helper.GetObjectAuth(t, class, id, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "v1-ns2", got2.Properties.(map[string]any)["title"])
	})

	t.Run("head is namespace-scoped", func(t *testing.T) {
		const class = "HeadTarget"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		// Insert only in ns1; ns2 has the class but no row.
		id := strfmt.UUID("cccccccc-3333-3333-3333-333333333333")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "head-ns1"},
		}, user1Key)
		require.NoError(t, err)

		_, err = helper.Client(t).Objects.ObjectsClassHead(
			objects.NewObjectsClassHeadParams().WithClassName(class).WithID(id),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		_, err = helper.Client(t).Objects.ObjectsClassHead(
			objects.NewObjectsClassHeadParams().WithClassName(class).WithID(id),
			helper.CreateAuth(user2Key),
		)
		require.Error(t, err)
		var nf *objects.ObjectsClassHeadNotFound
		require.True(t, errors.As(err, &nf), "expected ObjectsClassHeadNotFound, got %T: %v", err, err)
	})

	t.Run("query (GET /objects?class=) is namespace-scoped", func(t *testing.T) {
		const class = "ListTarget"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		id := strfmt.UUID("eeeeeeee-5555-5555-5555-555555555555")
		seedTwo(t, class, id, "list-ns1", "list-ns2", user1Key, user2Key)

		shortClass := class
		listResp1, err := helper.Client(t).Objects.ObjectsList(
			objects.NewObjectsListParams().WithClass(&shortClass),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.Len(t, listResp1.Payload.Objects, 1)
		assert.Equal(t, class, listResp1.Payload.Objects[0].Class)
		assert.Equal(t, "list-ns1", listResp1.Payload.Objects[0].Properties.(map[string]any)["title"])

		listResp2, err := helper.Client(t).Objects.ObjectsList(
			objects.NewObjectsListParams().WithClass(&shortClass),
			helper.CreateAuth(user2Key),
		)
		require.NoError(t, err)
		require.Len(t, listResp2.Payload.Objects, 1)
		assert.Equal(t, class, listResp2.Payload.Objects[0].Class)
		assert.Equal(t, "list-ns2", listResp2.Payload.Objects[0].Properties.(map[string]any)["title"])
	})

	t.Run("validate resolves the class per namespace", func(t *testing.T) {
		// Class only in ns1. user1 validates → ok. user2 validates same short
		// class → unresolved → error.
		const class = "ValidateTarget"
		setupClassInNs1(t, class, user1Key)

		id := strfmt.UUID("ffffffff-6666-6666-6666-666666666666")
		_, err := helper.Client(t).Objects.ObjectsValidate(
			objects.NewObjectsValidateParams().WithBody(&models.Object{
				ID: id, Class: class, Properties: map[string]any{"title": "ok"},
			}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		_, err = helper.Client(t).Objects.ObjectsValidate(
			objects.NewObjectsValidateParams().WithBody(&models.Object{
				ID: id, Class: class, Properties: map[string]any{"title": "ok"},
			}),
			helper.CreateAuth(user2Key),
		)
		require.Error(t, err)
	})

	t.Run("namespaced caller submitting :-qualified class on read is rejected as 422", func(t *testing.T) {
		const class = "DoublePrefix"
		setupClassInNs1(t, class, user1Key)

		obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class:      class,
			Properties: map[string]any{"title": "Memento"},
		}, user1Key)
		require.NoError(t, err)
		require.NotEmpty(t, obj.ID)

		// user1 supplying the already-qualified name is rejected by namespace
		// prefix validation — namespaced callers must use the short name.
		_, err = helper.GetObjectAuth(t, "customer1:"+class, obj.ID, user1Key)
		require.Error(t, err)
		var unproc *objects.ObjectsClassGetUnprocessableEntity
		require.True(t, errors.As(err, &unproc), "expected ObjectsClassGetUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("global admin reads object via qualified class name", func(t *testing.T) {
		const class = "AdminQualified"
		setupClassInNs1(t, class, user1Key)

		obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class:      class,
			Properties: map[string]any{"title": "Tenet"},
		}, user1Key)
		require.NoError(t, err)
		require.NotEmpty(t, obj.ID)

		// Admin has no namespace, so Resolve is a no-op and the qualified
		// name flows through untouched.
		got, err := helper.GetObjectAuth(t, "customer1:"+class, obj.ID, adminKey)
		require.NoError(t, err)
		assert.Equal(t, "customer1:"+class, got.Class)
		assert.Equal(t, "Tenet", got.Properties.(map[string]any)["title"])
	})

	t.Run("global admin reading object via short name returns 404", func(t *testing.T) {
		const class = "AdminShort"
		setupClassInNs1(t, class, user1Key)

		obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class:      class,
			Properties: map[string]any{"title": "Dunkirk"},
		}, user1Key)
		require.NoError(t, err)
		require.NotEmpty(t, obj.ID)

		// Admin → no namespace → Resolve leaves the short name as-is. Storage
		// only has "customer1:AdminShort" → 404.
		_, err = helper.GetObjectAuth(t, class, obj.ID, adminKey)
		require.Error(t, err)
		var nf *objects.ObjectsClassGetNotFound
		require.True(t, errors.As(err, &nf), "expected ObjectsClassGetNotFound, got %T: %v", err, err)
	})
}

// TestNamespaces_BatchOperations exercises BatchManager fan-out under
// namespace resolution.
func TestNamespaces_BatchOperations(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	t.Run("batch insert is namespace-scoped", func(t *testing.T) {
		const class = "BatchInsert"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		id1 := strfmt.UUID("11111111-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
		id2 := strfmt.UUID("22222222-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "ns1-a"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "ns1-b"}},
		}, user1Key)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "ns2-a"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "ns2-b"}},
		}, user2Key)

		got1a, err := helper.GetObjectAuth(t, class, id1, user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, got1a.Class)
		assert.Equal(t, "ns1-a", got1a.Properties.(map[string]any)["title"])

		got2b, err := helper.GetObjectAuth(t, class, id2, user2Key)
		require.NoError(t, err)
		assert.Equal(t, class, got2b.Class)
		assert.Equal(t, "ns2-b", got2b.Properties.(map[string]any)["title"])
	})

	t.Run("batch delete by filter is namespace-scoped", func(t *testing.T) {
		const class = "BatchDelete"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		id1 := strfmt.UUID("33333333-cccc-cccc-cccc-cccccccccccc")
		id2 := strfmt.UUID("44444444-dddd-dddd-dddd-dddddddddddd")
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "kill"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "keep"}},
		}, user1Key)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "kill"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "keep"}},
		}, user2Key)

		killText := "kill"
		body := &models.BatchDelete{
			Match: &models.BatchDeleteMatch{
				Class: class,
				Where: &models.WhereFilter{
					Operator:  "Equal",
					Path:      []string{"title"},
					ValueText: &killText,
				},
			},
		}
		resp, err := helper.Client(t).Batch.BatchObjectsDelete(
			batch.NewBatchObjectsDeleteParams().WithBody(body),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload.Results)
		assert.EqualValues(t, 1, resp.Payload.Results.Successful)

		// ns1's "kill" gone; "keep" survives.
		_, err = helper.GetObjectAuth(t, class, id1, user1Key)
		require.Error(t, err)
		var nfGet *objects.ObjectsClassGetNotFound
		require.True(t, errors.As(err, &nfGet), "expected ObjectsClassGetNotFound, got %T: %v", err, err)
		got, err := helper.GetObjectAuth(t, class, id2, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got.Properties.(map[string]any)["title"])

		// ns2 untouched.
		got21, err := helper.GetObjectAuth(t, class, id1, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "kill", got21.Properties.(map[string]any)["title"])
		got22, err := helper.GetObjectAuth(t, class, id2, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got22.Properties.(map[string]any)["title"])
	})

	t.Run("batch delete by filter via namespace-local alias", func(t *testing.T) {
		// Each namespace registers its own short alias name for its copy of
		// the class. b.resolveNS resolves "<ns>:BDAlias" to the underlying
		// "<ns>:BatchDeleteAlias" before authz and filter parsing, so the
		// delete must only touch user1's data.
		const (
			class = "BatchDeleteAlias"
			alias = "BDAlias"
		)
		setupClassInBothNamespaces(t, class, user1Key, user2Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: class}, user1Key)
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: class}, user2Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, "customer1:"+alias, helper.CreateAuth(adminKey))
			helper.DeleteAliasWithAuthz(t, "customer2:"+alias, helper.CreateAuth(adminKey))
		})

		id1 := strfmt.UUID("55555555-eeee-eeee-eeee-eeeeeeeeeeee")
		id2 := strfmt.UUID("66666666-ffff-ffff-ffff-ffffffffffff")
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "kill"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "keep"}},
		}, user1Key)
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "kill"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "keep"}},
		}, user2Key)

		killText := "kill"
		body := &models.BatchDelete{
			Match: &models.BatchDeleteMatch{
				Class: alias,
				Where: &models.WhereFilter{
					Operator:  "Equal",
					Path:      []string{"title"},
					ValueText: &killText,
				},
			},
		}
		var resp *batch.BatchObjectsDeleteOK
		retryOnAliasLag(t, func() error {
			var err error
			resp, err = helper.Client(t).Batch.BatchObjectsDelete(
				batch.NewBatchObjectsDeleteParams().WithBody(body),
				helper.CreateAuth(user1Key),
			)
			return err
		})
		require.NotNil(t, resp.Payload.Results)
		assert.EqualValues(t, 1, resp.Payload.Results.Successful)

		// ns1: kill gone, keep survives.
		_, err := helper.GetObjectAuth(t, class, id1, user1Key)
		require.Error(t, err)
		got1Keep, err := helper.GetObjectAuth(t, class, id2, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got1Keep.Properties.(map[string]any)["title"])

		// ns2 untouched.
		got2Kill, err := helper.GetObjectAuth(t, class, id1, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "kill", got2Kill.Properties.(map[string]any)["title"])
		got2Keep, err := helper.GetObjectAuth(t, class, id2, user2Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got2Keep.Properties.(map[string]any)["title"])
	})

	t.Run("batch delete by filter as global admin via qualified class name", func(t *testing.T) {
		// Admin has no namespace, so namespacing.Resolve is a no-op on the
		// class portion: the qualified name flows through to storage and
		// matches; the short name does not exist on disk and 422s.
		const class = "BatchDeleteAdmin"
		setupClassInNs1(t, class, user1Key)

		id1 := strfmt.UUID("99999999-1111-1111-1111-111111111111")
		id2 := strfmt.UUID("aaaaaaaa-2222-2222-2222-222222222222")
		helper.CreateObjectsBatchAuth(t, []*models.Object{
			{ID: id1, Class: class, Properties: map[string]any{"title": "kill"}},
			{ID: id2, Class: class, Properties: map[string]any{"title": "keep"}},
		}, user1Key)

		killText := "kill"
		mkBody := func(matchClass string) *models.BatchDelete {
			return &models.BatchDelete{
				Match: &models.BatchDeleteMatch{
					Class: matchClass,
					Where: &models.WhereFilter{
						Operator:  "Equal",
						Path:      []string{"title"},
						ValueText: &killText,
					},
				},
			}
		}

		// Admin with the short class name cannot reach the namespaced data.
		_, err := helper.Client(t).Batch.BatchObjectsDelete(
			batch.NewBatchObjectsDeleteParams().WithBody(mkBody(class)),
			helper.CreateAuth(adminKey),
		)
		require.Error(t, err)

		// Admin with the qualified class name resolves directly to storage.
		resp, err := helper.Client(t).Batch.BatchObjectsDelete(
			batch.NewBatchObjectsDeleteParams().WithBody(mkBody("customer1:"+class)),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload.Results)
		assert.EqualValues(t, 1, resp.Payload.Results.Successful)

		// kill is gone, keep survives — verify via the namespaced user.
		_, err = helper.GetObjectAuth(t, class, id1, user1Key)
		require.Error(t, err)
		got, err := helper.GetObjectAuth(t, class, id2, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "keep", got.Properties.(map[string]any)["title"])
	})

	t.Run("batch delete by reference-path filter is validated against the schema", func(t *testing.T) {
		// Reference-path filters are no longer rejected upfront on NS-enabled
		// clusters: filterext.Parse qualifies each inner class segment against
		// the source's namespace and then the filter is validated like any
		// other. This fixture has no "hasOther" ref property, so the request is
		// rejected by the downstream schema lookup ("no such prop") rather than
		// the removed path-len > 1 guard.
		const class = "BatchDeleteRefPath"
		setupClassInNs1(t, class, user1Key)

		x := "x"
		body := &models.BatchDelete{
			Match: &models.BatchDeleteMatch{
				Class: class,
				Where: &models.WhereFilter{
					Operator:  "Equal",
					Path:      []string{"hasOther", "Other", "name"},
					ValueText: &x,
				},
			},
		}
		_, err := helper.Client(t).Batch.BatchObjectsDelete(
			batch.NewBatchObjectsDeleteParams().WithBody(body),
			helper.CreateAuth(user1Key),
		)
		require.Error(t, err)
	})
}
