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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_Ref2VecCentroid writes a ref2vec-centroid Zoo through every
// path that vectorizes it and checks its vector against its ns1 Animals. ns2
// holds an Animal under the same id, so a read outside ns1 changes the vector.
func TestNamespaces_Ref2VecCentroid(t *testing.T) {
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	for _, key := range []string{user1Key, user2Key} {
		helper.CreateClassAuth(t, &models.Class{
			Class:      "Animal",
			Vectorizer: "none",
			Properties: []*models.Property{{Name: "name", DataType: []string{"text"}}},
		}, key)
		helper.CreateClassAuth(t, &models.Class{
			Class:      "Zoo",
			Vectorizer: "ref2vec-centroid",
			ModuleConfig: map[string]any{
				"ref2vec-centroid": map[string]any{"referenceProperties": []string{"hasAnimals"}},
			},
			Properties: []*models.Property{
				{Name: "name", DataType: []string{"text"}},
				{Name: "hasAnimals", DataType: []string{"Animal"}},
			},
		}, key)
	}
	t.Cleanup(func() {
		for _, ns := range []string{ns1, ns2} {
			helper.DeleteClassAuth(t, ns+":Zoo", adminKey)
			helper.DeleteClassAuth(t, ns+":Animal", adminKey)
		}
	})

	newID := func() strfmt.UUID { return strfmt.UUID(uuid.New().String()) }
	lion, tiger := newID(), newID()
	for _, a := range []struct {
		key    string
		id     strfmt.UUID
		vector []float32
	}{
		{user1Key, lion, []float32{1, 2, 3}},
		{user1Key, tiger, []float32{3, 4, 5}},
		{user2Key, lion, []float32{100, 100, 100}},
	} {
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class: "Animal", ID: a.id, Vector: a.vector,
			Properties: map[string]any{"name": "animal"},
		}, a.key)
		require.NoError(t, err)
	}

	ref := func(id strfmt.UUID) *models.SingleRef {
		return &models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/Animal/" + string(id))}
	}
	zooProps := func(animals ...strfmt.UUID) map[string]any {
		refs := make([]any, 0, len(animals))
		for _, id := range animals {
			refs = append(refs, map[string]any{"beacon": string(ref(id).Beacon)})
		}
		return map[string]any{"name": "zoo", "hasAnimals": refs}
	}
	createZoo := func(key string, animals ...strfmt.UUID) strfmt.UUID {
		t.Helper()
		id := newID()
		_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
			Class: "Zoo", ID: id, Properties: zooProps(animals...),
		}, key)
		require.NoError(t, err)
		return id
	}
	requireZooVector := func(path, ns string, id strfmt.UUID, want []float32) {
		t.Helper()
		got, err := helper.GetObjectAuth(t, ns+":Zoo", id, adminKey, "vector")
		require.NoError(t, err, path)
		assert.Equal(t, want, []float32(got.Vector), path)
	}

	createdID := createZoo(user1Key, lion)
	requireZooVector("create", ns1, createdID, []float32{1, 2, 3})

	batchID := newID()
	helper.CreateObjectsBatchAuth(t, []*models.Object{
		{Class: "Zoo", ID: batchID, Properties: zooProps(lion, tiger)},
	}, user1Key)
	requireZooVector("batch import", ns1, batchID, []float32{2, 3, 4})

	adminID1, adminID2 := newID(), newID()
	helper.CreateObjectsBatchAuth(t, []*models.Object{
		{Class: ns1 + ":Zoo", ID: adminID1, Properties: zooProps(lion)},
		{Class: ns2 + ":Zoo", ID: adminID2, Properties: zooProps(lion)},
	}, adminKey)
	requireZooVector("admin batch import into ns1", ns1, adminID1, []float32{1, 2, 3})
	requireZooVector("admin batch import into ns2", ns2, adminID2, []float32{100, 100, 100})

	replacedID := createZoo(user1Key)
	_, err := helper.Client(t).Objects.ObjectsClassPut(objects.NewObjectsClassPutParams().
		WithClassName("Zoo").WithID(replacedID).
		WithBody(&models.Object{Class: "Zoo", ID: replacedID, Properties: zooProps(tiger)}),
		helper.CreateAuth(user1Key))
	require.NoError(t, err)
	requireZooVector("replace", ns1, replacedID, []float32{3, 4, 5})

	mergedID := createZoo(user1Key, lion)
	_, err = helper.Client(t).Objects.ObjectsClassPatch(objects.NewObjectsClassPatchParams().
		WithClassName("Zoo").WithID(mergedID).
		WithBody(&models.Object{Class: "Zoo", ID: mergedID, Properties: map[string]any{"name": "renamed"}}),
		helper.CreateAuth(user1Key))
	require.NoError(t, err)
	requireZooVector("merge", ns1, mergedID, []float32{1, 2, 3})

	refID := createZoo(user1Key)
	for _, animal := range []strfmt.UUID{lion, tiger} {
		_, err := helper.AddReferenceReturn(t, ref(animal), refID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
		require.NoError(t, err)
	}
	requireZooVector("add references", ns1, refID, []float32{2, 3, 4})
	_, err = helper.DeleteReferenceReturn(t, ref(tiger), refID, "Zoo", "hasAnimals", "", helper.CreateAuth(user1Key))
	require.NoError(t, err)
	requireZooVector("delete a reference", ns1, refID, []float32{1, 2, 3})
}
