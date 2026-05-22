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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/client/objects"
	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_References exercises the end-to-end reference path through
// namespacing.Resolve (WS9). Each subtest submits short class names from a
// namespaced user and asserts:
//  1. source/target resolve into the caller's namespace (single-ref + batch)
//  2. cross-namespace references are rejected
//  3. update / delete resolve the same way add does
//  4. global admin can address objects via qualified class names
func TestNamespaces_References(t *testing.T) {
	user1Key, user2Key := twoNamespaces(t)

	// One Zoo and one Animal class per namespace. hasAnimals.DataType stays
	// short because the schema validator rejects ":" in cross-ref data types
	// — namespace handling happens at the reference handler, not in schema.
	zooAnimal := func() (animal, zoo *models.Class) {
		animal = &models.Class{
			Class: "Animal",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		}
		zoo = &models.Class{
			Class: "Zoo",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasAnimals", DataType: []string{"Animal"}},
			},
		}
		return animal, zoo
	}
	setup := func(t *testing.T, key string) {
		t.Helper()
		animal, zoo := zooAnimal()
		helper.CreateClassAuth(t, animal, key)
		helper.CreateClassAuth(t, zoo, key)
	}
	setup(t, user1Key)
	setup(t, user2Key)
	t.Cleanup(func() {
		helper.DeleteClassAuth(t, "customer1:Zoo", adminKey)
		helper.DeleteClassAuth(t, "customer1:Animal", adminKey)
		helper.DeleteClassAuth(t, "customer2:Zoo", adminKey)
		helper.DeleteClassAuth(t, "customer2:Animal", adminKey)
	})

	newID := func() strfmt.UUID { return strfmt.UUID(uuid.New().String()) }

	createIn := func(t *testing.T, key, class string, id strfmt.UUID, props map[string]any) {
		t.Helper()
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID: id, Class: class, Properties: props,
		}, key)
		require.NoError(t, err)
	}

	t.Run("single-ref add resolves through caller namespace", func(t *testing.T) {
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z1"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "a1"})

		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		// The object should now contain the reference (admin reads the
		// qualified class to inspect raw storage).
		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		refs, ok := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
		require.True(t, ok, "hasAnimals should be a list, got %T", got.Properties.(map[string]any)["hasAnimals"])
		require.Len(t, refs, 1)

		// Stored-shape invariant: the beacon on disk carries the short target
		// class so the object stays namespace-portable on export/import.
		stored := refs[0].(map[string]any)
		beaconStr, _ := stored["beacon"].(string)
		assert.Equal(t,
			"weaviate://localhost/Animal/"+string(animalID), beaconStr,
			"stored beacon must carry the short class name, not the qualified form")
	})

	t.Run("global admin submits qualified beacon, stored short on disk", func(t *testing.T) {
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "a"})

		// Admin addresses everything by qualified storage class. The handler
		// must still write the beacon in short form so portability holds.
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/customer1:Animal/" + string(animalID))},
			zooID, "customer1:Zoo", "hasAnimals", "", helper.CreateAuth(adminKey))
		require.NoError(t, err)

		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		refs := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
		require.Len(t, refs, 1)
		beaconStr, _ := refs[0].(map[string]any)["beacon"].(string)
		assert.Equal(t,
			"weaviate://localhost/Animal/"+string(animalID), beaconStr,
			"admin-submitted qualified beacon must be normalized to short on disk")
	})

	t.Run("source object in foreign namespace is invisible", func(t *testing.T) {
		// Object lives only in customer2. user1 tries to add a reference on
		// the same UUID via "Zoo" — that resolves to customer1:Zoo where the
		// object does not exist, so the call fails (404 from the source-object
		// existence check, not a cross-namespace leak).
		zooID, animalID := newID(), newID()
		createIn(t, user2Key, "Zoo", zooID, map[string]any{"name": "z2"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "a1"})

		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.Error(t, err)
	})

	t.Run("namespaced user cannot reference cross-namespace target via qualified beacon", func(t *testing.T) {
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z"})
		createIn(t, user2Key, "Animal", animalID, map[string]any{"name": "a2"})

		// Qualified target class — namespaced caller must not type "<ns>:" in
		// the beacon. namespacing.Resolve rejects with 422.
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/customer2:Animal/" + string(animalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.Error(t, err)
		var unproc *objects.ObjectsClassReferencesCreateUnprocessableEntity
		require.True(t, errors.As(err, &unproc),
			"expected ObjectsClassReferencesCreateUnprocessableEntity, got %T: %v", err, err)
	})

	t.Run("batch references stay namespace-local", func(t *testing.T) {
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "a"})

		refs := []*models.BatchReference{{
			From: strfmt.URI("weaviate://localhost/Zoo/" + string(zooID) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/Animal/" + string(animalID)),
		}}
		resp, err := helper.Client(t).Batch.BatchReferencesCreate(
			batch.NewBatchReferencesCreateParams().WithBody(refs),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.Len(t, resp.Payload, 1)
		assert.Nil(t, resp.Payload[0].Result.Errors,
			"expected no batch errors, got %+v", resp.Payload[0].Result.Errors)
	})

	t.Run("batch references against cross-namespace target fail that ref", func(t *testing.T) {
		zooID, badAnimalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z"})
		createIn(t, user2Key, "Animal", badAnimalID, map[string]any{"name": "a2"})

		refs := []*models.BatchReference{{
			From: strfmt.URI("weaviate://localhost/Zoo/" + string(zooID) + "/hasAnimals"),
			To:   strfmt.URI("weaviate://localhost/customer2:Animal/" + string(badAnimalID)),
		}}
		resp, err := helper.Client(t).Batch.BatchReferencesCreate(
			batch.NewBatchReferencesCreateParams().WithBody(refs),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err)
		require.Len(t, resp.Payload, 1)
		require.NotNil(t, resp.Payload[0].Result.Errors,
			"expected a batch-level error for cross-namespace target, got none")
	})

	t.Run("update and delete reference resolve through caller namespace", func(t *testing.T) {
		zooID, animalAID, animalBID := newID(), newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z"})
		createIn(t, user1Key, "Animal", animalAID, map[string]any{"name": "aA"})
		createIn(t, user1Key, "Animal", animalBID, map[string]any{"name": "aB"})

		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalAID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		// PUT replaces the whole multi-ref list with a different animal.
		_, err = helper.ReplaceReferencesReturn(t,
			[]*models.SingleRef{{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalBID))}},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		// DELETE the only remaining ref (passed in short form; the handler
		// must resolve it to "customer1:Animal" so the stored qualified beacon
		// matches).
		_, err = helper.DeleteReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalBID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		refs, ok := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
		if ok {
			assert.Len(t, refs, 0, "expected hasAnimals to be empty after delete")
		}
	})

	t.Run("admin delete with qualified beacon actually removes the ref", func(t *testing.T) {
		// Regression guard for the delete-side counterpart of the
		// storage invariant. references_add normalises admin-submitted
		// qualified beacons to short before storage; without the
		// structural (Class, TargetID) compare in removeReference, an
		// admin submitting "weaviate://localhost/customer1:Animal/<id>"
		// would hit an exact-string compare against the stored short
		// "weaviate://localhost/Animal/<id>", miss, and silently return
		// 204 with the ref still present.
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z-admin-del"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "leo"})

		// Namespaced user adds with the short beacon — stored short.
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		// Admin DELETE with the QUALIFIED beacon must remove the ref.
		_, err = helper.DeleteReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/customer1:Animal/" + string(animalID))},
			zooID, "customer1:Zoo", "hasAnimals", "", helper.CreateAuth(adminKey))
		require.NoError(t, err)

		// Verify the ref is actually gone — admin reads via qualified class.
		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		refs, ok := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
		if ok {
			assert.Len(t, refs, 0,
				"admin DELETE with qualified beacon must remove the ref; still saw %v", refs)
		}
	})

	t.Run("namespaced user cannot delete with foreign-namespace qualified beacon", func(t *testing.T) {
		// Counterpart to "namespaced user cannot reference cross-namespace
		// target via qualified beacon" but for the DELETE path. The
		// ValidateNamespacePrefix gate in references_delete runs BEFORE
		// any normalisation, so a namespaced user submitting
		// "weaviate://localhost/customer2:Animal/<id>" is rejected with
		// 422 — and the local ref is untouched.
		zooID, animalID, foreignAnimalID := newID(), newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z-foreign-del"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "local"})
		createIn(t, user2Key, "Animal", foreignAnimalID, map[string]any{"name": "foreign"})

		// Seed a legitimate ref on the namespaced user's row.
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		// Namespaced user DELETE with a foreign-NS qualified beacon: 422.
		_, err = helper.DeleteReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/customer2:Animal/" + string(foreignAnimalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.Error(t, err)
		var unproc *objects.ObjectsClassReferencesDeleteUnprocessableEntity
		require.True(t, errors.As(err, &unproc),
			"expected ObjectsClassReferencesDeleteUnprocessableEntity, got %T: %v", err, err)

		// The legitimate stored ref must still be present — the 422 path
		// must NOT mutate state.
		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		refs := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
		require.Len(t, refs, 1, "cross-NS delete must not remove the local ref")
		beaconStr, _ := refs[0].(map[string]any)["beacon"].(string)
		assert.Equal(t,
			"weaviate://localhost/Animal/"+string(animalID), beaconStr,
			"stored beacon for the surviving local ref should still be short")
	})

	t.Run("global admin reads object via qualified class", func(t *testing.T) {
		zooID := newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "admin-view"})

		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		assert.Equal(t, "customer1:Zoo", got.Class)
	})

	t.Run("identical UUIDs in two namespaces stay isolated through refs", func(t *testing.T) {
		// Same UUIDs on both sides of the cross-ref in both namespaces. The
		// only thing keeping user1's Zoo from accidentally fetching user2's
		// Animal on a read-side join is namespace enrichment of the beacon
		// target using the source's namespace.
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "zoo-ns1"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "animal-ns1"})
		createIn(t, user2Key, "Zoo", zooID, map[string]any{"name": "zoo-ns2"})
		createIn(t, user2Key, "Animal", animalID, map[string]any{"name": "animal-ns2"})

		// Each user adds a reference to *their* animal.
		for _, key := range []string{user1Key, user2Key} {
			_, err := helper.AddReferenceReturn(t,
				&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalID))},
				zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(key))
			require.NoError(t, err)
		}

		// Both rows have the same short beacon on disk — portability check.
		for _, ns := range []string{"customer1", "customer2"} {
			got, err := helper.GetObjectAuth(t, ns+":Zoo", zooID, adminKey)
			require.NoError(t, err)
			refs := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
			require.Len(t, refs, 1)
			beaconStr, _ := refs[0].(map[string]any)["beacon"].(string)
			assert.Equal(t,
				"weaviate://localhost/Animal/"+string(animalID), beaconStr,
				"both namespaces must store the identical short beacon")
		}
	})

	t.Run("gRPC search return_references resolves target via source namespace", func(t *testing.T) {
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Zoo", zooID, map[string]any{"name": "z-grpc"})
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "habitat-lion"})

		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(animalID))},
			zooID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		grpcClient, conn := newGrpcClient(t)
		defer conn.Close()

		// user1 sends a gRPC search using the short class name and asks for
		// hasAnimals to be ref-resolved inline. The parser must qualify the
		// linked Animal class via customer1:, otherwise the multi-get misses
		// the qualified storage.
		req := searchReq("Zoo", 10)
		req.Properties = &pb.PropertiesRequest{
			NonRefProperties: []string{"name"},
			RefProperties: []*pb.RefPropertiesRequest{{
				ReferenceProperty: "hasAnimals",
				Properties:        &pb.PropertiesRequest{NonRefProperties: []string{"name"}},
			}},
		}
		// Search may return Zoos from earlier subtests too; the assertion is
		// that *the one we just created* has its hasAnimals ref resolved
		// inline to the customer1:Animal target with the expected name.
		req.Limit = 100
		resp, err := grpcClient.Search(authCtx(user1Key), req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)

		var foundResolved bool
		var resolvedName string
		for _, result := range resp.Results {
			// Find our Zoo by name.
			zooName := result.Properties.NonRefProps.Fields["name"]
			if zooName == nil || zooName.GetTextValue() != "z-grpc" {
				continue
			}
			for _, np := range result.Properties.RefProps {
				if np.PropName != "hasAnimals" {
					continue
				}
				require.NotEmpty(t, np.Properties)
				if v, ok := np.Properties[0].NonRefProps.Fields["name"]; ok {
					resolvedName = v.GetTextValue()
					if resolvedName == "habitat-lion" {
						foundResolved = true
					}
				}
			}
		}
		assert.True(t, foundResolved,
			"gRPC ref-resolve should inline the customer1:Animal target via the source namespace; got name=%q", resolvedName)
	})

	t.Run("gRPC filter-by-ref via SingleTarget returns the right row on NS cluster", func(t *testing.T) {
		// Regression guard: stored ref beacons are short
		// ("weaviate://localhost/Animal/<id>") because the references write
		// path normalizes via crossref.NewLocalhost. The by-ref filter must
		// strip the qualified prefix off the nested-search ClassName before
		// building its lookup beacon — otherwise the lookup value carries
		// "customer1:Animal" and never matches the stored short value.
		// Pre-fix this returned 0 rows; we now assert the actual matching
		// row is returned, not just that the call doesn't crash.
		zooTiger, zooLion := newID(), newID()
		tigerID, lionID := newID(), newID()
		createIn(t, user1Key, "Animal", tigerID, map[string]any{"name": "filter-tiger"})
		createIn(t, user1Key, "Animal", lionID, map[string]any{"name": "filter-lion"})
		createIn(t, user1Key, "Zoo", zooTiger, map[string]any{"name": "zoo-with-tiger"})
		createIn(t, user1Key, "Zoo", zooLion, map[string]any{"name": "zoo-with-lion"})
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(tigerID))},
			zooTiger, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)
		_, err = helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(lionID))},
			zooLion, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		grpcClient, conn := newGrpcClient(t)
		defer conn.Close()

		req := searchReq("Zoo", 100)
		req.Properties = &pb.PropertiesRequest{NonRefProperties: []string{"name"}}
		req.Filters = &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_SingleTarget{
					SingleTarget: &pb.FilterReferenceSingleTarget{
						On: "hasAnimals",
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "filter-tiger"},
		}
		resp, err := grpcClient.Search(authCtx(user1Key), req)
		require.NoError(t, err, "namespaced filter on a ref property should not fail with class-not-found")

		// Other subtests run against the same Zoo class and may leave
		// behind rows whose hasAnimals refs got modified — assert that the
		// zoo-with-tiger row IS in the result and the zoo-with-lion row is
		// NOT, rather than asserting an exact total count.
		var sawTiger, sawLion bool
		for _, r := range resp.Results {
			name := r.Properties.NonRefProps.Fields["name"].GetTextValue()
			if name == "zoo-with-tiger" {
				sawTiger = true
			}
			if name == "zoo-with-lion" {
				sawLion = true
			}
		}
		assert.True(t, sawTiger, "by-ref filter on hasAnimals.name=='filter-tiger' should return zoo-with-tiger")
		assert.False(t, sawLion, "by-ref filter must not return zoos whose ref points to a different animal")
	})

	t.Run("create object with ref property in Properties payload (NS happy path)", func(t *testing.T) {
		// Reproduction guard: object create that *embeds* the ref in the
		// Properties payload (instead of using the dedicated /references
		// endpoint). On NS clusters the validation path runs
		// ValidateExistence against the short class from the user-submitted
		// beacon, but storage is qualified — the existence check misses
		// and the request fails. Asserting NoError here will fail until
		// the validation path is namespace-aware.
		zooID, animalID := newID(), newID()
		createIn(t, user1Key, "Animal", animalID, map[string]any{"name": "a"})

		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			ID:    zooID,
			Class: "Zoo",
			Properties: map[string]any{
				"name": "z-inline-ref",
				"hasAnimals": []any{
					map[string]any{"beacon": "weaviate://localhost/Animal/" + string(animalID)},
				},
			},
		}, user1Key)
		require.NoError(t, err, "creating an object with a ref property in Properties must succeed on NS clusters")

		// Stored shape: still short.
		got, err := helper.GetObjectAuth(t, "customer1:Zoo", zooID, adminKey)
		require.NoError(t, err)
		refs := got.Properties.(map[string]any)["hasAnimals"].([]interface{})
		require.Len(t, refs, 1)
		beaconStr, _ := refs[0].(map[string]any)["beacon"].(string)
		assert.Equal(t,
			"weaviate://localhost/Animal/"+string(animalID), beaconStr,
			"stored beacon stays short even when the ref is submitted inline")
	})

	t.Run("AddProperty adds a cross-ref property to an existing namespaced class", func(t *testing.T) {
		// Pre-WS9: AddClassProperty would call ReadOnlyClass("Animal") (short)
		// while storage has "customer1:Animal" (qualified), hitting
		// ErrRefToNonexistentClass at the use-case validator.
		// WS9: classGetterWithAuth stitches parent's namespace onto the short
		// class name before lookup, so the cross-ref data type resolves.
		const class = "PostHocZoo"
		// Create Zoo without the hasAnimals property, and Animal alongside.
		helper.CreateClassAuth(t, &models.Class{
			Class: class,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class: "PostHocAnimal",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
			},
		}, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:"+class, adminKey)
			helper.DeleteClassAuth(t, "customer1:PostHocAnimal", adminKey)
		})

		// Now add the cross-ref property — DataType is short, parent class
		// gets qualified internally to customer1:PostHocZoo.
		_, err := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(
			schemaCli.NewSchemaObjectsPropertiesAddParams().
				WithClassName(class).
				WithBody(&models.Property{Name: "hasAnimals", DataType: []string{"PostHocAnimal"}}),
			helper.CreateAuth(user1Key),
		)
		require.NoError(t, err, "adding a cross-ref property to an existing class must work on NS-enabled clusters")

		// Verify schema reflects the new property.
		got := helper.GetClassAuth(t, "customer1:"+class, adminKey)
		var sawHasAnimals bool
		for _, p := range got.Properties {
			if p.Name == "hasAnimals" {
				sawHasAnimals = true
				assert.Equal(t, []string{"PostHocAnimal"}, p.DataType,
					"DataType is stored short for namespace portability")
			}
		}
		assert.True(t, sawHasAnimals, "hasAnimals property should be present after AddProperty")
	})

	t.Run("self-referencing class on NS cluster (Zoo.relatedTo -> Zoo)", func(t *testing.T) {
		// The RAFT cross-ref existence check has a self-ref special case:
		// `qualifiedDT == req.Class.Class` short-circuits the existence
		// lookup. On NS clusters req.Class.Class is qualified but
		// Property.DataType is short — without parent-namespace stitching
		// the comparison fails and the class create fails with
		// "reference property to nonexistent class".
		const class = "SelfRef"
		helper.CreateClassAuth(t, &models.Class{
			Class: class,
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "relatedTo", DataType: []string{class}},
			},
		}, user1Key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, "customer1:"+class, adminKey) })

		// And the self-ref works end-to-end: two instances, one referencing
		// the other.
		a, b := newID(), newID()
		createIn(t, user1Key, class, a, map[string]any{"name": "a"})
		createIn(t, user1Key, class, b, map[string]any{"name": "b"})
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + class + "/" + string(b))},
			a, class, "relatedTo", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)
	})

	t.Run("multi-target ref DataType on NS cluster", func(t *testing.T) {
		// hasOther points at TWO classes. Both must qualify against the
		// parent's namespace via the per-element loop in the schema
		// validators (use-case validateProperty + RAFT cluster/schema).
		helper.CreateClassAuth(t, &models.Class{
			Class:      "MultiRefAlpha",
			Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
		}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class:      "MultiRefBeta",
			Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
		}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class: "MultiRefSource",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasOther", DataType: []string{"MultiRefAlpha", "MultiRefBeta"}},
			},
		}, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:MultiRefSource", adminKey)
			helper.DeleteClassAuth(t, "customer1:MultiRefAlpha", adminKey)
			helper.DeleteClassAuth(t, "customer1:MultiRefBeta", adminKey)
		})

		// Reference each multi-target via an explicit class in the beacon.
		srcID, alphaID, betaID := newID(), newID(), newID()
		createIn(t, user1Key, "MultiRefSource", srcID, map[string]any{"name": "src"})
		createIn(t, user1Key, "MultiRefAlpha", alphaID, map[string]any{"name": "a"})
		createIn(t, user1Key, "MultiRefBeta", betaID, map[string]any{"name": "b"})

		for _, ref := range []struct{ cls, id string }{
			{"MultiRefAlpha", string(alphaID)},
			{"MultiRefBeta", string(betaID)},
		} {
			_, err := helper.AddReferenceReturn(t,
				&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + ref.cls + "/" + ref.id)},
				srcID, "MultiRefSource", "hasOther", "", helper.CreateAuth(user1Key))
			require.NoError(t, err, "multi-target ref to %s should succeed on NS cluster", ref.cls)
		}

		// Stored beacons stay short for both targets.
		got, err := helper.GetObjectAuth(t, "customer1:MultiRefSource", srcID, adminKey)
		require.NoError(t, err)
		refs := got.Properties.(map[string]any)["hasOther"].([]interface{})
		require.Len(t, refs, 2)
		for _, r := range refs {
			beaconStr, _ := r.(map[string]any)["beacon"].(string)
			// Beacon format: weaviate://localhost/<class>/<uuid>. The class
			// segment must not carry a namespace prefix.
			assert.NotContains(t, beaconStr, "customer1:",
				"stored multi-target beacon must be short (no namespace prefix in the class segment): %s", beaconStr)
		}
	})

	t.Run("gRPC filter-by-ref via MultiTarget returns the right row on NS cluster", func(t *testing.T) {
		// Same regression guard as SingleTarget above, but on the
		// MultiTarget branch of the filter parser. The MultiTarget path
		// receives an explicit TargetCollection from the caller — the
		// parser qualifies it via parentNS, the by-ref lookup must then
		// strip back to short to match the stored beacon.
		//
		// Self-contained schema (MultiRefAlpha/Beta/Source) because the
		// shared Zoo/Animal schema doesn't have a multi-target ref. Mirror
		// the "multi-target ref DataType on NS cluster" subtest above.
		helper.CreateClassAuth(t, &models.Class{
			Class:      "MTFilterAlpha",
			Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
		}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class:      "MTFilterBeta",
			Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
		}, user1Key)
		helper.CreateClassAuth(t, &models.Class{
			Class: "MTFilterSource",
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "linkedTo", DataType: []string{"MTFilterAlpha", "MTFilterBeta"}},
			},
		}, user1Key)
		t.Cleanup(func() {
			helper.DeleteClassAuth(t, "customer1:MTFilterSource", adminKey)
			helper.DeleteClassAuth(t, "customer1:MTFilterAlpha", adminKey)
			helper.DeleteClassAuth(t, "customer1:MTFilterBeta", adminKey)
		})

		alphaWanted, alphaOther := newID(), newID()
		srcMatch, srcMiss := newID(), newID()
		createIn(t, user1Key, "MTFilterAlpha", alphaWanted, map[string]any{"name": "wanted"})
		createIn(t, user1Key, "MTFilterAlpha", alphaOther, map[string]any{"name": "ignored"})
		createIn(t, user1Key, "MTFilterSource", srcMatch, map[string]any{"name": "src-match"})
		createIn(t, user1Key, "MTFilterSource", srcMiss, map[string]any{"name": "src-miss"})
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/MTFilterAlpha/" + string(alphaWanted))},
			srcMatch, "MTFilterSource", "linkedTo", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)
		_, err = helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/MTFilterAlpha/" + string(alphaOther))},
			srcMiss, "MTFilterSource", "linkedTo", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)

		grpcClient, conn := newGrpcClient(t)
		defer conn.Close()

		req := searchReq("MTFilterSource", 100)
		req.Properties = &pb.PropertiesRequest{NonRefProperties: []string{"name"}}
		req.Filters = &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_MultiTarget{
					MultiTarget: &pb.FilterReferenceMultiTarget{
						On:               "linkedTo",
						TargetCollection: "MTFilterAlpha", // short — qualifier stitches customer1
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "wanted"},
		}
		resp, err := grpcClient.Search(authCtx(user1Key), req)
		require.NoError(t, err)

		var names []string
		for _, r := range resp.Results {
			names = append(names, r.Properties.NonRefProps.Fields["name"].GetTextValue())
		}
		require.Len(t, names, 1, "MultiTarget by-ref filter should return exactly one matching source row, got %v", names)
		assert.Equal(t, "src-match", names[0])
	})

	t.Run("gRPC filter MultiTarget rejects cross-namespace TargetCollection with 422", func(t *testing.T) {
		grpcClient, conn := newGrpcClient(t)
		defer conn.Close()

		req := searchReq("Zoo", 10)
		req.Properties = &pb.PropertiesRequest{NonRefProperties: []string{"name"}}
		req.Filters = &pb.Filters{
			Operator: pb.Filters_OPERATOR_EQUAL,
			Target: &pb.FilterTarget{
				Target: &pb.FilterTarget_MultiTarget{
					MultiTarget: &pb.FilterReferenceMultiTarget{
						On:               "hasAnimals",
						TargetCollection: "customer2:Animal", // foreign namespace
						Target: &pb.FilterTarget{
							Target: &pb.FilterTarget_Property{Property: "name"},
						},
					},
				},
			},
			TestValue: &pb.Filters_ValueText{ValueText: "anything"},
		}
		_, err := grpcClient.Search(authCtx(user1Key), req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})
}
