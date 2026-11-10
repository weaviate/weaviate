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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// A suspended namespace takes no class update and no index drop. The refused drops
// are re-issued, which a gate behind the no-op would miss. Everything runs as the
// global operator, because the namespace's own key is refused while suspended.
func TestNamespaces_SuspendRefusesSchemaUpdates(t *testing.T) {
	t.Parallel()
	ns1, _, user1Key, _ := twoNamespaces(t)

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

	helper.SuspendNamespace(t, ns1, adminKey)
	// The resume is registered after the class cleanups, so LIFO runs it first.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })
	// SuspendNamespace polls only the leader, so the node the requests reach may
	// not have applied the suspend yet. That node refuses the namespace's key only
	// after applying it.
	require.Eventually(t, func() bool {
		_, err := schemaDumpAs(t, user1Key)
		var unauth *schemaCli.SchemaDumpUnauthorized
		return errors.As(err, &unauth)
	}, 10*time.Second, 50*time.Millisecond, "the node the requests reach never applied the suspend")

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
}
