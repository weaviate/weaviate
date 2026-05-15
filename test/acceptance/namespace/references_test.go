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
			zooName, _ := result.Properties.NonRefProps.Fields["name"]
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
}
