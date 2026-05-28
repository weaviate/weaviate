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
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_ResponseStripping_REST pins the response-stripping contract:
// every namespaced-caller success response surfaces short names while the
// global admin keeps the raw qualified form. Round-trip tests assert that a
// stripped GET response can be PUT/PATCHed back unchanged.
func TestNamespaces_ResponseStripping_REST(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	t.Run("AddClass response strips Class and cross-ref DataType", func(t *testing.T) {
		const name = "AddClassStripped"
		// Set up referenced classes first so the cross-ref existence
		// check passes when the new class is created.
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user1Key)
		helper.CreateClassAuth(t, &models.Class{Class: "Books"}, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:"+name, adminKey)
			helper.DeleteClassAuth(t, "customer1:Movies", adminKey)
			helper.DeleteClassAuth(t, "customer1:Books", adminKey)
		})

		resp, err := helper.CreateClassAuthWithReturn(t, &models.Class{
			Class: name,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "watched", DataType: []string{"Movies"}},
				{Name: "related", DataType: []string{"Movies", "Books"}},
			},
		}, user1Key)
		require.NoError(t, err)
		assert.Equal(t, name, resp.Payload.Class)
		require.Len(t, resp.Payload.Properties, 3)
		assert.Equal(t, []string{"text"}, findProp(resp.Payload, "title").DataType)
		assert.Equal(t, []string{"Movies"}, findProp(resp.Payload, "watched").DataType)
		assert.Equal(t, []string{"Movies", "Books"}, findProp(resp.Payload, "related").DataType)

		// Admin sees the raw qualified class + qualified cross-ref DataTypes
		// via direct GET.
		raw := helper.GetClassAuth(t, "customer1:"+name, adminKey)
		assert.Equal(t, "customer1:"+name, raw.Class)
		assert.Equal(t, []string{"customer1:Movies"}, findProp(raw, "watched").DataType)
		assert.Equal(t, []string{"customer1:Movies", "customer1:Books"}, findProp(raw, "related").DataType)
	})

	t.Run("addClassProperty response strips DataType cross-ref class names", func(t *testing.T) {
		const host = "Library"
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user1Key)
		helper.CreateClassAuth(t, &models.Class{Class: "Books"}, user1Key)
		setupClassInNs1(t, host, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:Movies", adminKey)
			helper.DeleteClassAuth(t, "customer1:Books", adminKey)
		})

		single, err := addPropertyAuthWithReturn(t, host,
			&models.Property{Name: "watched", DataType: []string{"Movies"}}, user1Key)
		require.NoError(t, err)
		assert.Equal(t, []string{"Movies"}, single.DataType)

		multi, err := addPropertyAuthWithReturn(t, host,
			&models.Property{Name: "related", DataType: []string{"Movies", "Books"}}, user1Key)
		require.NoError(t, err)
		assert.Equal(t, []string{"Movies", "Books"}, multi.DataType)

		// Admin sees the raw qualified DataTypes via direct GET.
		raw := helper.GetClassAuth(t, "customer1:"+host, adminKey)
		assert.Equal(t, []string{"customer1:Movies"}, findProp(raw, "watched").DataType)
		assert.Equal(t, []string{"customer1:Movies", "customer1:Books"}, findProp(raw, "related").DataType)
	})

	t.Run("AddClass with already-qualified own-NS DataType rejected", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user1Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:Movies", adminKey) })

		_, err := helper.CreateClassAuthWithReturn(t, &models.Class{
			Class: "RejectQualifiedOwn",
			Properties: []*models.Property{
				{Name: "watched", DataType: []string{"customer1:Movies"}},
			},
		}, user1Key)
		require.Error(t, err)
		assert.Contains(t, schemaCreateErrMessage(t, err), "not a valid class name")
	})

	t.Run("AddClass with cross-NS DataType rejected", func(t *testing.T) {
		// customer2:Movies exists, but customer1's caller may not reference it.
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user2Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer2:Movies", adminKey) })

		_, err := helper.CreateClassAuthWithReturn(t, &models.Class{
			Class: "RejectCrossNS",
			Properties: []*models.Property{
				{Name: "watched", DataType: []string{"customer2:Movies"}},
			},
		}, user1Key)
		require.Error(t, err)
		assert.Contains(t, schemaCreateErrMessage(t, err), "not a valid class name")
	})

	t.Run("schema GET → PUT round-trip on real class path succeeds with stripped body", func(t *testing.T) {
		// Round-trip safety: the namespaced GET returns Class="RoundTripMe";
		// the PUT body therefore carries the short class name. The handler
		// must realign Class with the qualified path before UpdateClass so
		// the immutable-field validator sees the qualified form on both
		// sides.
		const name = "RoundTripMe"
		helper.CreateClassAuth(t, &models.Class{Class: name, Description: "v1"}, user1Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+name, adminKey) })

		// GET by short path — response is stripped.
		got, err := helper.GetClassAuthWithReturn(t, name, user1Key)
		require.NoError(t, err)
		require.Equal(t, name, got.Payload.Class)

		// Modify a non-immutable field and PUT the stripped body back.
		got.Payload.Description = "v2"
		putResp, err := helper.UpdateClassAuthWithReturn(t, name, got.Payload, user1Key)
		require.NoError(t, err)
		// PUT response is itself stripped.
		assert.Equal(t, name, putResp.Payload.Class)

		// Confirm via admin: underlying class updated.
		after := helper.GetClassAuth(t, "customer1:"+name, adminKey)
		assert.Equal(t, "v2", after.Description)
	})

	t.Run("schema GET → PUT round-trip with cross-ref properties succeeds", func(t *testing.T) {
		// Stripped cross-ref DataTypes in the PUT body must be re-qualified;
		// otherwise the property-immutability check rejects the round-trip.
		const host = "RoundTripWithRefs"
		const target = "RoundTripRefTarget"
		helper.CreateClassAuth(t, &models.Class{Class: target}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class:       host,
			Description: "v1",
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "watched", DataType: []string{target}},
				{Name: "related", DataType: []string{target}},
			},
		}, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:"+host, adminKey)
			helper.DeleteClassAuth(t, "customer1:"+target, adminKey)
		})

		got, err := helper.GetClassAuthWithReturn(t, host, user1Key)
		require.NoError(t, err)
		require.Equal(t, host, got.Payload.Class)
		// GET response is stripped: cross-ref DataTypes are short.
		assert.Equal(t, []string{target}, findProp(got.Payload, "watched").DataType)
		assert.Equal(t, []string{target}, findProp(got.Payload, "related").DataType)

		// PUT the stripped body back verbatim with a non-immutable change.
		got.Payload.Description = "v2"
		putResp, err := helper.UpdateClassAuthWithReturn(t, host, got.Payload, user1Key)
		require.NoError(t, err)
		assert.Equal(t, host, putResp.Payload.Class)
		assert.Equal(t, []string{target}, findProp(putResp.Payload, "watched").DataType)

		// Admin GET confirms the stored cross-refs are still qualified and
		// the Description update landed.
		after := helper.GetClassAuth(t, "customer1:"+host, adminKey)
		assert.Equal(t, "v2", after.Description)
		assert.Equal(t, []string{"customer1:" + target}, findProp(after, "watched").DataType)
		assert.Equal(t, []string{"customer1:" + target}, findProp(after, "related").DataType)
	})

	t.Run("AddAlias response strips Alias and Class", func(t *testing.T) {
		const class = "AliasCreateTarget"
		const alias = "AliasCreateName"
		setupClassInNs1(t, class, user1Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, "customer1:"+alias, helper.CreateAuth(adminKey))
		})

		resp, err := helper.CreateAliasAuthWithReturn(t, &models.Alias{Alias: alias, Class: class}, user1Key)
		require.NoError(t, err)
		assert.Equal(t, alias, resp.Payload.Alias)
		assert.Equal(t, class, resp.Payload.Class)
	})

	t.Run("UpdateAlias response strips Alias and Class", func(t *testing.T) {
		const classA = "AliasUpdateA"
		const classB = "AliasUpdateB"
		const alias = "AliasUpdateName"
		setupClassInNs1(t, classA, user1Key)
		helper.CreateClassAuth(t, &models.Class{Class: classB}, user1Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+classB, adminKey) })
		helper.CreateAliasAuth(t, &models.Alias{Alias: alias, Class: classA}, user1Key)
		t.Cleanup(func() {
			helper.DeleteAliasWithAuthz(t, "customer1:"+alias, helper.CreateAuth(adminKey))
		})

		resp, err := helper.UpdateAliasAuthWithReturn(t, alias, classB, user1Key)
		require.NoError(t, err)
		assert.Equal(t, alias, resp.Payload.Alias)
		assert.Equal(t, classB, resp.Payload.Class)
	})

	t.Run("AddObject response is stripped", func(t *testing.T) {
		const class = "AddObjStripped"
		setupClassInNs1(t, class, user1Key)
		obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class: class, Properties: map[string]any{"title": "first"},
		}, user1Key)
		require.NoError(t, err)
		assert.Equal(t, class, obj.Class)
	})

	t.Run("object GET → PUT round-trip succeeds with stripped body", func(t *testing.T) {
		const class = "ObjPutRoundTrip"
		setupClassInNs1(t, class, user1Key)

		id := strfmt.UUID("aaaa1111-2222-3333-4444-aaaa11112222")
		obj, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "v1"},
		}, user1Key)
		require.NoError(t, err)
		require.Equal(t, class, obj.Class)

		// Fetch via short name; mutate a property; PUT the stripped body back.
		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		require.Equal(t, class, got.Class)

		got.Properties = map[string]any{"title": "v2"}
		putResp, err := helper.Client(t).Objects.ObjectsClassPut(
			objects.NewObjectsClassPutParams().WithClassName(class).WithID(id).WithBody(got),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		// PUT response is itself stripped.
		assert.Equal(t, class, putResp.Payload.Class)

		after, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "v2", after.Properties.(map[string]any)["title"])
	})

	t.Run("object GET → PATCH round-trip succeeds with stripped body", func(t *testing.T) {
		const class = "ObjPatchRoundTrip"
		setupClassInNs1(t, class, user1Key)

		id := strfmt.UUID("bbbb2222-3333-4444-5555-bbbb22223333")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "v1"},
		}, user1Key)
		require.NoError(t, err)

		got, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		require.Equal(t, class, got.Class)

		// Derive the PATCH body from the stripped GET payload.
		got.Properties = map[string]any{"title": "v2", "extra": "added"}
		_, err = helper.Client(t).Objects.ObjectsClassPatch(
			objects.NewObjectsClassPatchParams().WithClassName(class).WithID(id).WithBody(got),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)

		after, err := helper.GetObjectAuth(t, class, id, user1Key)
		require.NoError(t, err)
		assert.Equal(t, "v2", after.Properties.(map[string]any)["title"])
	})

	t.Run("batch create response strips per-item Class", func(t *testing.T) {
		const class = "BatchCreateStrip"
		setupClassInNs1(t, class, user1Key)

		resp, err := helper.Client(t).Batch.BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{Class: class, Properties: map[string]any{"title": "a"}},
					{Class: class, Properties: map[string]any{"title": "b"}},
				},
			}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.Len(t, resp.Payload, 2)
		assert.Equal(t, class, resp.Payload[0].Class)
		assert.Equal(t, class, resp.Payload[1].Class)

		// Admin-side symmetric: posting against the qualified class name
		// returns the qualified Class verbatim (StripObjectResponseClass is
		// a no-op for global principals).
		respAdmin, err := helper.Client(t).Batch.BatchObjectsCreate(
			batch.NewBatchObjectsCreateParams().WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{
					{Class: "customer1:" + class, Properties: map[string]any{"title": "admin-a"}},
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		require.Len(t, respAdmin.Payload, 1)
		assert.Equal(t, "customer1:"+class, respAdmin.Payload[0].Class,
			"global admin must see the qualified Class in the batch-create response")
	})

	t.Run("batch delete response strips Match.Class", func(t *testing.T) {
		const class = "BatchDeleteStrip"
		setupClassInNs1(t, class, user1Key)
		id := strfmt.UUID("cccc3333-4444-5555-6666-cccc33334444")
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: map[string]any{"title": "kill"},
		}, user1Key)
		require.NoError(t, err)

		killText := "kill"
		resp, err := helper.Client(t).Batch.BatchObjectsDelete(
			batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{
				Match: &models.BatchDeleteMatch{
					Class: class,
					Where: &models.WhereFilter{
						Operator:  "Equal",
						Path:      []string{"title"},
						ValueText: &killText,
					},
				},
			}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload.Match)
		assert.Equal(t, class, resp.Payload.Match.Class)

		// Admin-side symmetric: seed a fresh row in customer1's class and
		// delete it via the qualified class name; the echoed Match.Class
		// must remain qualified (StripOwnNamespace is a no-op for globals).
		idAdmin := strfmt.UUID("cccc3333-4444-5555-6666-cccc33335555")
		_, err = helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: idAdmin, Class: class, Properties: map[string]any{"title": "admin-kill"},
		}, user1Key)
		require.NoError(t, err)
		killTextAdmin := "admin-kill"
		respAdmin, err := helper.Client(t).Batch.BatchObjectsDelete(
			batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{
				Match: &models.BatchDeleteMatch{
					Class: "customer1:" + class,
					Where: &models.WhereFilter{
						Operator:  "Equal",
						Path:      []string{"title"},
						ValueText: &killTextAdmin,
					},
				},
			}),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		require.NotNil(t, respAdmin.Payload.Match)
		assert.Equal(t, "customer1:"+class, respAdmin.Payload.Match.Class,
			"global admin must see the qualified Match.Class in the batch-delete response")
	})

	// Regression: BatchReferencesCreate used to echo the From beacon back
	// with the qualified source class because resolveNS rewrites
	// BatchReference.From.Class to "<ns>:Class" before the response writer
	// runs (usecases/objects/batch_references_add.go:52-57), and
	// RefSource.String() embeds Class directly into the URI path. Without
	// StripRefSourceBeacon, a namespaced caller would see
	// "weaviate://localhost/customer1:Zoo/<uuid>/hasAnimals" — leaking their
	// own namespace through the response. The unit-level coverage lives at
	// usecases/schema/namespacing/resolver_test.go (TestStripRefSourceBeacon,
	// TestStripRefBeacon); this guards the end-to-end wire shape.
	t.Run("BatchReferencesCreate response strips own-namespace prefix from From/To beacons", func(t *testing.T) {
		const zoo, animal = "BatchRefStripZoo", "BatchRefStripAnimal"
		helper.CreateClassAuth(t, &models.Class{
			Class:      animal,
			Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
		}, user1Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+animal, adminKey) })
		helper.CreateClassAuth(t, &models.Class{
			Class: zoo,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasAnimals", DataType: []string{animal}},
			},
		}, user1Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+zoo, adminKey) })

		zooID := strfmt.UUID("bbbb1111-2222-3333-4444-bbbb11112222")
		animalID := strfmt.UUID("bbbb3333-4444-5555-6666-bbbb33334444")
		_, err := helper.CreateObjectWithResponseAuth(t,
			&models.Object{ID: zooID, Class: zoo, Properties: map[string]any{"name": "z"}}, user1Key)
		require.NoError(t, err)
		_, err = helper.CreateObjectWithResponseAuth(t,
			&models.Object{ID: animalID, Class: animal, Properties: map[string]any{"name": "a"}}, user1Key)
		require.NoError(t, err)

		refs := []*models.BatchReference{{
			From: strfmt.URI("weaviate://localhost/" + zoo + "/" + string(zooID) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/" + animal + "/" + string(animalID)),
		}}
		resp, err := helper.Client(t).Batch.BatchReferencesCreate(
			batch.NewBatchReferencesCreateParams().WithBody(refs),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.Len(t, resp.Payload, 1)
		require.Nil(t, resp.Payload[0].Result.Errors,
			"expected no batch errors, got %+v", resp.Payload[0].Result.Errors)

		gotFrom := string(resp.Payload[0].From)
		gotTo := string(resp.Payload[0].To)
		assert.NotContains(t, gotFrom, "customer1:",
			"namespaced caller must see short From beacon: %s", gotFrom)
		assert.NotContains(t, gotTo, "customer1:",
			"namespaced caller must see short To beacon: %s", gotTo)
		assert.Equal(t, "weaviate://localhost/"+zoo+"/"+string(zooID)+"/hasAnimals", gotFrom)
		assert.Equal(t, "weaviate://localhost/"+animal+"/"+string(animalID), gotTo)
	})

	t.Run("cross-NS isolation: same short class name in both namespaces, each sees only own", func(t *testing.T) {
		const class = "CrossNSStrip"
		setupClassInBothNamespaces(t, class, user1Key, user2Key)

		// Each namespaced caller GETs by short name and sees only their own
		// short form; admin sees both qualified forms exist.
		c1 := helper.GetClassAuth(t, class, user1Key)
		assert.Equal(t, class, c1.Class)
		c2 := helper.GetClassAuth(t, class, user2Key)
		assert.Equal(t, class, c2.Class)

		assert.Equal(t, "customer1:"+class, helper.GetClassAuth(t, "customer1:"+class, adminKey).Class)
		assert.Equal(t, "customer2:"+class, helper.GetClassAuth(t, "customer2:"+class, adminKey).Class)
	})
}

// TestNamespaces_ResponseStripping_OwnInfoUsername pins the Username strip
// contract: namespaced DB users carry "customer1:apiuser" internally, and
// /users/own-info must echo that as the short form. Role/permission strip
// requires RBAC and is covered in the authz acceptance suite.
func TestNamespaces_ResponseStripping_OwnInfoUsername(t *testing.T) {
	user1Key, _ := twoNamespaces(t)

	t.Run("namespaced DB user sees short username", func(t *testing.T) {
		resp, err := helper.Client(t).Users.GetOwnInfo(
			users.NewGetOwnInfoParams(), helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload.Username)
		assert.Equal(t, "u1", *resp.Payload.Username,
			"namespaced caller's stored username is customer1:u1; response must strip")
	})

	t.Run("global admin sees raw username", func(t *testing.T) {
		resp, err := helper.Client(t).Users.GetOwnInfo(
			users.NewGetOwnInfoParams(), helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		require.NotNil(t, resp.Payload.Username)
		// Admin's username does not carry a namespace prefix, so it
		// passes through unchanged.
		assert.Equal(t, adminUser, *resp.Payload.Username)
	})
}

// schemaCreateErrMessage extracts the human-readable validation message from a
// SchemaObjectsCreate 422 response. The go-swagger Error() output only shows
// the status line and a pointer to the body, so callers asserting on the
// underlying validation text must unwrap through the typed payload.
func schemaCreateErrMessage(t *testing.T, err error) string {
	t.Helper()
	var parsed *clschema.SchemaObjectsCreateUnprocessableEntity
	require.True(t, errors.As(err, &parsed), "expected SchemaObjectsCreateUnprocessableEntity, got %T: %v", err, err)
	require.NotNil(t, parsed.Payload)
	require.NotEmpty(t, parsed.Payload.Error)
	return parsed.Payload.Error[0].Message
}
