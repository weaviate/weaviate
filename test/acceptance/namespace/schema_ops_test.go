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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func addPropertyAuth(t *testing.T, className string, prop *models.Property, key string) error {
	t.Helper()
	params := schema.NewSchemaObjectsPropertiesAddParams().
		WithClassName(className).
		WithBody(prop)
	_, err := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(params, helper.CreateAuth(key))
	return err
}

func deletePropertyIndexAuth(t *testing.T, className, propName, indexName, key string) error {
	t.Helper()
	params := schema.NewSchemaObjectsPropertiesDeleteParams().
		WithClassName(className).
		WithPropertyName(propName).
		WithIndexName(indexName)
	_, err := helper.Client(t).Schema.SchemaObjectsPropertiesDelete(params, helper.CreateAuth(key))
	return err
}

func deleteVectorIndexAuth(t *testing.T, className, vectorIndexName, key string) error {
	t.Helper()
	params := schema.NewSchemaObjectsVectorsDeleteParams().
		WithClassName(className).
		WithVectorIndexName(vectorIndexName)
	_, err := helper.Client(t).Schema.SchemaObjectsVectorsDelete(params, helper.CreateAuth(key))
	return err
}

func tokenizePropertyAuth(t *testing.T, className, propName, text, key string) (*models.TokenizeResponse, error) {
	t.Helper()
	params := schema.NewSchemaObjectsPropertiesTokenizeParams().
		WithClassName(className).
		WithPropertyName(propName).
		WithBody(&models.PropertyTokenizeRequest{Text: &text})
	resp, err := helper.Client(t).Schema.SchemaObjectsPropertiesTokenize(params, helper.CreateAuth(key))
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// findProp returns the property with the given name (case-insensitive), or nil.
func findProp(class *models.Class, name string) *models.Property {
	for _, p := range class.Properties {
		if strings.EqualFold(p.Name, name) {
			return p
		}
	}
	return nil
}

func TestNamespaces_AddClassProperty(t *testing.T) {
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

	t.Run("namespaced user adds property by short name; lands on qualified class", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "Movies"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

		err := addPropertyAuth(t, "Movies", &models.Property{
			Name:     "genre",
			DataType: []string{"text"},
		}, user1Key)
		require.NoError(t, err)

		got := helper.GetClassAuth(t, "customer1:Movies", adminKey)
		require.Len(t, got.Properties, 1)
		assert.Equal(t, "genre", got.Properties[0].Name)
	})

	t.Run("each namespace mutates only its own class", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "Books"}, user1Key)
		helper.CreateClassAuth(t, &models.Class{Class: "Books"}, user2Key)
		defer helper.DeleteClassAuth(t, "customer1:Books", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:Books", adminKey)

		require.NoError(t, addPropertyAuth(t, "Books", &models.Property{
			Name: "author", DataType: []string{"text"},
		}, user2Key))

		c1 := helper.GetClassAuth(t, "customer1:Books", adminKey)
		c2 := helper.GetClassAuth(t, "customer2:Books", adminKey)
		assert.Empty(t, c1.Properties)
		require.Len(t, c2.Properties, 1)
		assert.Equal(t, "author", c2.Properties[0].Name)
	})

	t.Run("global admin adds property by qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "Shows"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Shows", adminKey)

		err := addPropertyAuth(t, "customer1:Shows", &models.Property{
			Name: "rating", DataType: []string{"text"},
		}, adminKey)
		require.NoError(t, err)

		got := helper.GetClassAuth(t, "customer1:Shows", adminKey)
		require.Len(t, got.Properties, 1)
		assert.Equal(t, "rating", got.Properties[0].Name)
	})

	t.Run("alias is not a backdoor: AddClassProperty by alias name fails and underlying class unchanged", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{Class: "Concerts"}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:Gigs", helper.CreateAuth(adminKey))

		err := addPropertyAuth(t, "Gigs", &models.Property{
			Name: "venue", DataType: []string{"text"},
		}, user1Key)
		require.Error(t, err, "AddClassProperty must not resolve aliases on namespaced clusters")

		got := helper.GetClassAuth(t, "customer1:Concerts", adminKey)
		assert.Empty(t, got.Properties, "underlying class must be unchanged")
	})
}

func TestNamespaces_DeleteClassPropertyIndex(t *testing.T) {
	const ns1 = "customer1"

	helper.CreateNamespace(t, ns1, adminKey)
	defer helper.DeleteNamespace(t, ns1, adminKey)

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":u1", adminKey) })

	// Helper: build a class with a `title` text property that has the
	// filterable index enabled (the index this test toggles off).
	indexed := true
	classWithFilterable := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{{
				Name:            "title",
				DataType:        []string{"text"},
				IndexFilterable: &indexed,
			}},
		}
	}

	// requireFilterable asserts that class `qualified` has property `title`
	// with IndexFilterable set to `want`.
	requireFilterable := func(t *testing.T, qualified string, want bool) {
		t.Helper()
		got := helper.GetClassAuth(t, qualified, adminKey)
		prop := findProp(got, "title")
		require.NotNil(t, prop)
		require.NotNil(t, prop.IndexFilterable)
		assert.Equal(t, want, *prop.IndexFilterable)
	}

	t.Run("namespaced user drops filterable index by short name", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithFilterable("Movies"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)
		requireFilterable(t, "customer1:Movies", true)

		require.NoError(t, deletePropertyIndexAuth(t, "Movies", "title", "filterable", user1Key))

		requireFilterable(t, "customer1:Movies", false)
	})

	t.Run("global admin drops filterable index by qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithFilterable("Shows"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Shows", adminKey)
		requireFilterable(t, "customer1:Shows", true)

		require.NoError(t, deletePropertyIndexAuth(t, "customer1:Shows", "title", "filterable", adminKey))

		requireFilterable(t, "customer1:Shows", false)
	})

	t.Run("alias is not a backdoor: DeleteClassPropertyIndex by alias name fails and underlying class unchanged", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithFilterable("Concerts"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:Gigs", helper.CreateAuth(adminKey))
		requireFilterable(t, "customer1:Concerts", true)

		err := deletePropertyIndexAuth(t, "Gigs", "title", "filterable", user1Key)
		require.Error(t, err, "DeleteClassPropertyIndex must not resolve aliases on namespaced clusters")

		requireFilterable(t, "customer1:Concerts", true)
	})
}

func TestNamespaces_DeleteClassVectorIndex(t *testing.T) {
	const ns1 = "customer1"

	helper.CreateNamespace(t, ns1, adminKey)
	defer helper.DeleteNamespace(t, ns1, adminKey)

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":u1", adminKey) })

	// classWithTwoVectors returns a class with two named vectors (vec1, vec2),
	// both backed by HNSW. Dropping one toggles its VectorIndexType to "none".
	classWithTwoVectors := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					Vectorizer:      map[string]any{"none": map[string]any{}},
					VectorIndexType: "hnsw",
				},
				"vec2": {
					Vectorizer:      map[string]any{"none": map[string]any{}},
					VectorIndexType: "hnsw",
				},
			},
		}
	}

	// vectorIndexType returns the VectorIndexType of `vec` on `qualified`.
	vectorIndexType := func(t *testing.T, qualified, vec string) string {
		t.Helper()
		got := helper.GetClassAuth(t, qualified, adminKey)
		cfg, ok := got.VectorConfig[vec]
		require.True(t, ok, "vector config %q missing on %q", vec, qualified)
		return cfg.VectorIndexType
	}

	t.Run("namespaced user drops named vector by short name", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithTwoVectors("Movies"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)
		require.Equal(t, "hnsw", vectorIndexType(t, "customer1:Movies", "vec1"))

		require.NoError(t, deleteVectorIndexAuth(t, "Movies", "vec1", user1Key))

		assert.Equal(t, "none", vectorIndexType(t, "customer1:Movies", "vec1"))
		assert.Equal(t, "hnsw", vectorIndexType(t, "customer1:Movies", "vec2"))
	})

	t.Run("global admin drops named vector by qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithTwoVectors("Shows"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Shows", adminKey)
		require.Equal(t, "hnsw", vectorIndexType(t, "customer1:Shows", "vec1"))

		require.NoError(t, deleteVectorIndexAuth(t, "customer1:Shows", "vec1", adminKey))

		assert.Equal(t, "none", vectorIndexType(t, "customer1:Shows", "vec1"))
	})

	t.Run("alias is not a backdoor: DeleteClassVectorIndex by alias name fails and underlying class unchanged", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithTwoVectors("Concerts"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:Gigs", helper.CreateAuth(adminKey))
		require.Equal(t, "hnsw", vectorIndexType(t, "customer1:Concerts", "vec1"))

		err := deleteVectorIndexAuth(t, "Gigs", "vec1", user1Key)
		require.Error(t, err, "DeleteClassVectorIndex must not resolve aliases on namespaced clusters")

		assert.Equal(t, "hnsw", vectorIndexType(t, "customer1:Concerts", "vec1"))
	})
}

func TestNamespaces_PropertyTokenize(t *testing.T) {
	const ns1 = "customer1"

	helper.CreateNamespace(t, ns1, adminKey)
	defer helper.DeleteNamespace(t, ns1, adminKey)

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":u1", adminKey) })

	// classWithTokenizedTitle returns a class with a `title` text property using
	// the "word" tokenization, so propertyTokenize has something to split on.
	classWithTokenizedTitle := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{{
				Name:         "title",
				DataType:     []string{"text"},
				Tokenization: "word",
			}},
		}
	}

	t.Run("namespaced user tokenizes property by short class name", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithTokenizedTitle("Movies"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

		got, err := tokenizePropertyAuth(t, "Movies", "title", "Hello World", user1Key)
		require.NoError(t, err)
		assert.Equal(t, []string{"hello", "world"}, got.Indexed)
	})

	t.Run("namespaced user tokenizes property via alias resolves to underlying class", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithTokenizedTitle("Concerts"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:Gigs", helper.CreateAuth(adminKey))

		got, err := tokenizePropertyAuth(t, "Gigs", "title", "Hello World", user1Key)
		require.NoError(t, err, "propertyTokenize should resolve alias to underlying class")
		assert.Equal(t, []string{"hello", "world"}, got.Indexed)
	})

	t.Run("global admin tokenizes property by qualified class name", func(t *testing.T) {
		helper.CreateClassAuth(t, classWithTokenizedTitle("Shows"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Shows", adminKey)

		got, err := tokenizePropertyAuth(t, "customer1:Shows", "title", "Hello World", adminKey)
		require.NoError(t, err)
		assert.Equal(t, []string{"hello", "world"}, got.Indexed)
	})
}
