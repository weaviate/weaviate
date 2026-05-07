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
