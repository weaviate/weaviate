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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
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

func addPropertyAuthWithReturn(t *testing.T, className string, prop *models.Property, key string) (*models.Property, error) {
	t.Helper()
	params := schema.NewSchemaObjectsPropertiesAddParams().
		WithClassName(className).
		WithBody(prop)
	resp, err := helper.Client(t).Schema.SchemaObjectsPropertiesAdd(params, helper.CreateAuth(key))
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
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

		// retryOnAliasLag absorbs the brief window where the alias entry has
		// been applied on the leader but the follower has not yet replicated
		// it, which would surface here as a 404 from local alias resolution.
		var got *models.TokenizeResponse
		retryOnAliasLag(t, func() error {
			var err error
			got, err = tokenizePropertyAuth(t, "Gigs", "title", "Hello World", user1Key)
			return err
		})
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

// TestNamespaces_ShardsStatus exercises GET /v1/schema/<class>/shards on a
// namespaced class. The namespace is pinned to a node that is provably not
// the test client's entry (helper.SetupClient binds to GetWeaviate(),
// i.e. weaviate-0), so the shard always lives remote and the request hops
// the /indices/<ns>:<class>/shards/<sh>/status route every run.
func TestNamespaces_ShardsStatus(t *testing.T) {
	const (
		ns1      = "shardsstatusns"
		homeNode = "weaviate-2"
	)

	helper.CreateNamespaceWithHomeNode(t, ns1, homeNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns1, adminKey) })

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":u1", adminKey) })

	makeClass := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
			},
		}
	}

	getShards := func(t *testing.T, className, key string) (models.ShardStatusList, error) {
		t.Helper()
		params := schema.NewSchemaObjectsShardsGetParams().WithClassName(className)
		resp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(params, helper.CreateAuth(key))
		if err != nil {
			return nil, err
		}
		return resp.Payload, nil
	}

	t.Run("namespaced user gets shard by short name", func(t *testing.T) {
		helper.CreateClassAuth(t, makeClass("Movies"), user1Key)
		defer helper.DeleteClassAuth(t, ns1+":Movies", adminKey)

		shards, err := getShards(t, "Movies", user1Key)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		for _, s := range shards {
			assert.NotEmpty(t, s.Status, "shard %q should have a populated status", s.Name)
		}
	})

	t.Run("global admin gets shard by qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, makeClass("Shows"), user1Key)
		defer helper.DeleteClassAuth(t, ns1+":Shows", adminKey)

		shards, err := getShards(t, ns1+":Shows", adminKey)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		for _, s := range shards {
			assert.NotEmpty(t, s.Status, "shard %q should have a populated status", s.Name)
		}
	})

	t.Run("namespaced user gets shard via alias", func(t *testing.T) {
		helper.CreateClassAuth(t, makeClass("Concerts"), user1Key)
		defer helper.DeleteClassAuth(t, ns1+":Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, ns1+":Gigs", helper.CreateAuth(adminKey))

		// retryOnAliasLag absorbs the brief window where the alias entry has
		// been applied on the leader but the follower has not yet replicated
		// it, which would surface here as a 404 from local alias resolution.
		var shards models.ShardStatusList
		retryOnAliasLag(t, func() error {
			var err error
			shards, err = getShards(t, "Gigs", user1Key)
			return err
		})
		require.Len(t, shards, 1)
		for _, s := range shards {
			assert.NotEmpty(t, s.Status, "shard %q should have a populated status", s.Name)
		}
	})
}

// TestNamespaces_UpdateShardStatus exercises PUT
// /v1/schema/<class>/shards/<shard>. The write path qualifies via
// QualifyClass — same pattern as the other schema mutations — so short
// names from a namespaced principal must hit "<ns>:<class>" and aliases
// must not be a backdoor.
func TestNamespaces_UpdateShardStatus(t *testing.T) {
	const (
		ns1 = "customer1"
		ns2 = "customer2"
	)

	helper.CreateNamespace(t, ns1, adminKey)
	defer helper.DeleteNamespace(t, ns1, adminKey)
	helper.CreateNamespace(t, ns2, adminKey)
	defer helper.DeleteNamespace(t, ns2, adminKey)

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	defer helper.DeleteUser(t, ns1+":u1", adminKey)
	user2Key := createNamespacedUser(t, "u2", ns2, adminKey)
	defer helper.DeleteUser(t, ns2+":u2", adminKey)

	newClass := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
			},
			ShardingConfig: map[string]any{"desiredCount": 1},
		}
	}

	getShards := func(t *testing.T, className, key string) (models.ShardStatusList, error) {
		t.Helper()
		params := schema.NewSchemaObjectsShardsGetParams().WithClassName(className)
		resp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(params, helper.CreateAuth(key))
		if err != nil {
			return nil, err
		}
		return resp.Payload, nil
	}

	updateShard := func(t *testing.T, className, shardName, status, key string) error {
		t.Helper()
		params := schema.NewSchemaObjectsShardsUpdateParams().
			WithClassName(className).
			WithShardName(shardName).
			WithBody(&models.ShardStatus{Status: status})
		_, err := helper.Client(t).Schema.SchemaObjectsShardsUpdate(params, helper.CreateAuth(key))
		return err
	}

	// eventuallyShardStatus polls getShards until the named shard reports
	// the expected status. UpdateShardStatus commits via RAFT on the
	// leader, but the actual shard state lives on the shard owner, which
	// applies the entry asynchronously — so a read on a follower can
	// briefly see the old status.
	eventuallyShardStatus := func(t *testing.T, className, shardName, want, key string) {
		t.Helper()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			after, err := getShards(t, className, key)
			if !assert.NoError(c, err) {
				return
			}
			for _, s := range after {
				if s.Name == shardName {
					assert.Equal(c, want, s.Status)
					return
				}
			}
			assert.Failf(c, "shard not found", "shard %q not in %s", shardName, className)
		}, 10*time.Second, 50*time.Millisecond)
	}

	t.Run("namespaced user updates shard status by short name; lands on qualified class", func(t *testing.T) {
		helper.CreateClassAuth(t, newClass("Movies"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

		shards, err := getShards(t, "Movies", user1Key)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		shardName := shards[0].Name

		require.NoError(t, updateShard(t, "Movies", shardName, "READONLY", user1Key))

		eventuallyShardStatus(t, "customer1:Movies", shardName, "READONLY", adminKey)
	})

	t.Run("global admin updates shard status by qualified name", func(t *testing.T) {
		helper.CreateClassAuth(t, newClass("Shows"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Shows", adminKey)

		shards, err := getShards(t, "customer1:Shows", adminKey)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		shardName := shards[0].Name

		require.NoError(t, updateShard(t, "customer1:Shows", shardName, "READONLY", adminKey))

		eventuallyShardStatus(t, "customer1:Shows", shardName, "READONLY", adminKey)
	})

	t.Run("alias is not a backdoor: UpdateShardStatus by alias name fails and underlying shard unchanged", func(t *testing.T) {
		helper.CreateClassAuth(t, newClass("Concerts"), user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:Gigs", helper.CreateAuth(adminKey))

		shards, err := getShards(t, "Concerts", user1Key)
		require.NoError(t, err)
		require.Len(t, shards, 1)
		shardName := shards[0].Name
		initialStatus := shards[0].Status

		// Wait for the alias entry to be visible on the node handling the
		// request before asserting that UpdateShardStatus rejects it. On a
		// multi-node cluster CreateAliasAuth returns once the leader has
		// applied, but the follower may still be replicating — without
		// this, a transient "alias not found" would make the test pass
		// even if UpdateShardStatus incorrectly resolved aliases.
		retryOnAliasLag(t, func() error {
			_, err := getShards(t, "Gigs", user1Key)
			return err
		})

		err = updateShard(t, "Gigs", shardName, "READONLY", user1Key)
		require.Error(t, err, "UpdateShardStatus must not resolve aliases on namespaced clusters")

		after, err := getShards(t, "customer1:Concerts", adminKey)
		require.NoError(t, err)
		require.Len(t, after, 1)
		assert.Equal(t, initialStatus, after[0].Status, "underlying shard status must be unchanged")
	})

	t.Run("each namespace updates only its own class", func(t *testing.T) {
		helper.CreateClassAuth(t, newClass("Books"), user1Key)
		helper.CreateClassAuth(t, newClass("Books"), user2Key)
		defer helper.DeleteClassAuth(t, "customer1:Books", adminKey)
		defer helper.DeleteClassAuth(t, "customer2:Books", adminKey)

		// Shard ids are per-class, so customer1:Books and customer2:Books
		// have disjoint shard name sets.
		shards2, err := getShards(t, "customer2:Books", adminKey)
		require.NoError(t, err)
		require.Len(t, shards2, 1)
		shardName := shards2[0].Name

		// user2 says "Books" + a shard id that exists only on customer2:Books.
		// If qualification were skipped the update would resolve to
		// customer1:Books and fail with "shard not found".
		require.NoError(t, updateShard(t, "Books", shardName, "READONLY", user2Key))

		eventuallyShardStatus(t, "customer2:Books", shardName, "READONLY", adminKey)

		after1, err := getShards(t, "customer1:Books", adminKey)
		require.NoError(t, err)
		require.Len(t, after1, 1)
		assert.NotEqual(t, "READONLY", after1[0].Status, "customer1:Books must be untouched by user2's update")
	})
}

// TestNamespaces_NodesGetClass exercises GET /v1/nodes/<class> on a
// namespaced class. /nodes is an operator-only surface: under the narrowed
// built-in admin a namespaced user is denied (403), while the env-var root
// reaches it by qualified name. The qualified-name path also covers the
// cluster-API URL routing through `regxNodesClass`, which only accepts
// namespace-qualified names once it is built from IndexNameRegexCore.
func TestNamespaces_NodesGetClass(t *testing.T) {
	const ns1 = "customer1"

	helper.CreateNamespace(t, ns1, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns1, adminKey) })

	user1Key := createNamespacedUser(t, "u1", ns1, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns1+":u1", adminKey) })

	helper.CreateClassAuth(t, &models.Class{
		Class: "Movies",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
	}, user1Key)
	defer helper.DeleteClassAuth(t, "customer1:Movies", adminKey)

	verbose := "verbose"

	t.Run("namespaced user denied node status by short class name", func(t *testing.T) {
		params := nodes.NewNodesGetClassParams().WithClassName("Movies").WithOutput(&verbose)
		_, err := helper.Client(t).Nodes.NodesGetClass(params, helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *nodes.NodesGetClassForbidden
		require.True(t, errors.As(err, &forbidden), "expected NodesGetClassForbidden, got %T: %v", err, err)
	})

	t.Run("global admin gets node status by qualified class name", func(t *testing.T) {
		params := nodes.NewNodesGetClassParams().WithClassName("customer1:Movies").WithOutput(&verbose)
		resp, err := helper.Client(t).Nodes.NodesGetClass(params, helper.CreateAuth(adminKey))
		require.NoError(t, err)
		require.NotNil(t, resp.Payload)
		assert.NotEmpty(t, resp.Payload.Nodes)
	})

	t.Run("namespaced user denied node status via alias", func(t *testing.T) {
		helper.CreateClassAuth(t, &models.Class{
			Class: "Concerts",
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
			},
		}, user1Key)
		defer helper.DeleteClassAuth(t, "customer1:Concerts", adminKey)
		helper.CreateAliasAuth(t, &models.Alias{Alias: "Gigs", Class: "Concerts"}, user1Key)
		defer helper.DeleteAliasWithAuthz(t, "customer1:Gigs", helper.CreateAuth(adminKey))

		// A 403 is immediate (the authz deny precedes alias resolution), so no
		// retryOnAliasLag wrapper is needed here.
		params := nodes.NewNodesGetClassParams().WithClassName("Gigs").WithOutput(&verbose)
		_, err := helper.Client(t).Nodes.NodesGetClass(params, helper.CreateAuth(user1Key))
		require.Error(t, err)
		var forbidden *nodes.NodesGetClassForbidden
		require.True(t, errors.As(err, &forbidden), "expected NodesGetClassForbidden, got %T: %v", err, err)
	})
}
