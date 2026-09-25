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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// A suspended namespace takes no class update and no index drop, including a
// re-issued drop that finds the index already off. Everything runs as the global
// operator, because the namespace's own key stops authenticating once suspended.
func TestNamespaces_SuspendRefusesSchemaUpdates(t *testing.T) {
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	const (
		indexesClassName = "SuspendIndexes"
		vectorsClassName = "SuspendVectors"
		droppedVector    = "vec1"
		liveVector       = "vec2"
		// refusalText is the sentinel's own text, which a global operator sees.
		refusalText = "namespace is suspended"
	)
	qualifiedIndexes := ns1 + ":" + indexesClassName
	qualifiedVectors := ns1 + ":" + vectorsClassName

	indexed := true
	hnsw := models.VectorConfig{
		Vectorizer:      map[string]any{"none": map[string]any{}},
		VectorIndexType: "hnsw",
	}

	requireTitleIndexes := func(t *testing.T, qualified string, filterable, searchable bool) {
		t.Helper()
		prop := findProp(helper.GetClassAuth(t, qualified, adminKey), "title")
		require.NotNil(t, prop)
		require.NotNil(t, prop.IndexFilterable)
		require.NotNil(t, prop.IndexSearchable)
		assert.Equal(t, filterable, *prop.IndexFilterable, "filterable")
		assert.Equal(t, searchable, *prop.IndexSearchable, "searchable")
	}

	vectorIndexType := func(t *testing.T, qualified, vector string) string {
		t.Helper()
		cfg, ok := helper.GetClassAuth(t, qualified, adminKey).VectorConfig[vector]
		require.True(t, ok, "vector config %q missing on %q", vector, qualified)
		return cfg.VectorIndexType
	}

	// seed gives a namespace a title whose filterable index is already dropped and
	// a vec1 already dropped. The vectors class's only tenant is COLD, so the drop
	// starts no cleanup and vec1 keeps its dropped marker, the state a re-drop meets.
	seed := func(t *testing.T, ns, key string) {
		t.Helper()
		helper.CreateClassAuth(t, &models.Class{
			Class: indexesClassName,
			Properties: []*models.Property{{
				Name:            "title",
				DataType:        []string{"text"},
				IndexFilterable: &indexed,
				IndexSearchable: &indexed,
			}},
		}, key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+indexesClassName, adminKey) })

		helper.CreateClassAuth(t, &models.Class{
			Class:              vectorsClassName,
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			VectorConfig:       map[string]models.VectorConfig{droppedVector: hnsw, liveVector: hnsw},
		}, key)
		t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":"+vectorsClassName, adminKey) })
		require.NoError(t, addTenantsAuth(t, vectorsClassName, []*models.Tenant{
			{Name: "cold", ActivityStatus: models.TenantActivityStatusCOLD},
		}, key))

		require.NoError(t, deletePropertyIndexAuth(t, indexesClassName, "title", "filterable", key))
		require.NoError(t, deleteVectorIndexAuth(t, vectorsClassName, droppedVector, key))

		requireTitleIndexes(t, ns+":"+indexesClassName, false, true)
		require.Equal(t, "none", vectorIndexType(t, ns+":"+vectorsClassName, droppedVector))
		require.Equal(t, "hnsw", vectorIndexType(t, ns+":"+vectorsClassName, liveVector))
	}
	seed(t, ns1, user1Key)
	seed(t, ns2, user2Key)

	helper.SuspendNamespace(t, ns1, adminKey)
	// The resume is registered after the class cleanups, so LIFO runs it first.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })
	// The index drops check the state on the node they reach, which can apply the
	// suspend after the leader that SuspendNamespace polls.
	awaitSuspendVisible(t, sharedCompose.GetWeaviate().GrpcURI(), qualifiedIndexes)

	t.Run("a class update is refused", func(t *testing.T) {
		class := helper.GetClassAuth(t, qualifiedIndexes, adminKey)
		class.Description = "updated while suspended"

		_, err := helper.UpdateClassAuthWithReturn(t, qualifiedIndexes, class, adminKey)
		var refused *schemaCli.SchemaObjectsUpdateUnprocessableEntity
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)

		assert.Empty(t, helper.GetClassAuth(t, qualifiedIndexes, adminKey).Description,
			"the update must not have applied")
	})

	t.Run("a drop of a property index already off is refused", func(t *testing.T) {
		err := deletePropertyIndexAuth(t, qualifiedIndexes, "title", "filterable", adminKey)
		var refused *schemaCli.SchemaObjectsPropertiesDeleteUnprocessableEntity
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)
	})

	t.Run("a drop of a property index still on is refused", func(t *testing.T) {
		err := deletePropertyIndexAuth(t, qualifiedIndexes, "title", "searchable", adminKey)
		var refused *schemaCli.SchemaObjectsPropertiesDeleteUnprocessableEntity
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)

		requireTitleIndexes(t, qualifiedIndexes, false, true)
	})

	t.Run("a re-issued vector index drop is refused", func(t *testing.T) {
		err := deleteVectorIndexAuth(t, qualifiedVectors, droppedVector, adminKey)
		var refused *schemaCli.SchemaObjectsVectorsDeleteUnprocessableEntity
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)

		// vec1 is still marked, so the drop met the marker rather than a vector already gone.
		assert.Equal(t, "none", vectorIndexType(t, qualifiedVectors, droppedVector))
	})

	t.Run("a vector index drop is refused", func(t *testing.T) {
		err := deleteVectorIndexAuth(t, qualifiedVectors, liveVector, adminKey)
		var refused *schemaCli.SchemaObjectsVectorsDeleteUnprocessableEntity
		require.ErrorAs(t, err, &refused, "got %T: %v", err, err)
		require.NotEmpty(t, refused.Payload.Error)
		assert.Contains(t, refused.Payload.Error[0].Message, refusalText)
		assert.NotContains(t, refused.Payload.Error[0].Message, ns1)

		assert.Equal(t, "hnsw", vectorIndexType(t, qualifiedVectors, liveVector))
	})

	t.Run("a shard status read is still served", func(t *testing.T) {
		resp, err := helper.Client(t).Schema.SchemaObjectsShardsGet(
			schemaCli.NewSchemaObjectsShardsGetParams().WithClassName(qualifiedIndexes),
			helper.CreateAuth(adminKey),
		)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Payload)
	})

	// This row fails if the gate refuses whenever any namespace is suspended,
	// rather than only the one the collection belongs to.
	t.Run("the other namespace still takes the same requests", func(t *testing.T) {
		qualified2Indexes := ns2 + ":" + indexesClassName
		class := helper.GetClassAuth(t, qualified2Indexes, adminKey)
		class.Description = "updated beside a suspended namespace"
		_, err := helper.UpdateClassAuthWithReturn(t, qualified2Indexes, class, adminKey)
		require.NoError(t, err)

		require.NoError(t, deletePropertyIndexAuth(t, qualified2Indexes, "title", "filterable", adminKey))
		require.NoError(t, deleteVectorIndexAuth(t, ns2+":"+vectorsClassName, droppedVector, adminKey))
	})

	// Resume flips straight to active, so this is the active state rather than
	// resuming. It is the non-regression half, which passes without the gate.
	t.Run("a resumed namespace takes them", func(t *testing.T) {
		helper.ResumeNamespace(t, ns1, adminKey)

		class := helper.GetClassAuth(t, qualifiedIndexes, adminKey)
		class.Description = "updated after the resume"
		_, err := helper.UpdateClassAuthWithReturn(t, qualifiedIndexes, class, adminKey)
		require.NoError(t, err)

		// ResumeNamespace polls the leader, so this node may still refuse. The
		// re-issued drop writes nothing, which makes retrying it harmless.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.NoError(c, deletePropertyIndexAuth(t, qualifiedIndexes, "title", "filterable", adminKey))
		}, 30*time.Second, 200*time.Millisecond, "the resume never reached the node the request went to")
		require.NoError(t, deletePropertyIndexAuth(t, qualifiedIndexes, "title", "searchable", adminKey))
		require.NoError(t, deleteVectorIndexAuth(t, qualifiedVectors, droppedVector, adminKey))
		require.NoError(t, deleteVectorIndexAuth(t, qualifiedVectors, liveVector, adminKey))

		requireTitleIndexes(t, qualifiedIndexes, false, false)
		assert.Equal(t, "none", vectorIndexType(t, qualifiedVectors, liveVector))
	})
}
