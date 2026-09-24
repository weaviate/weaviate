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

	schemaCli "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// A suspended namespace takes no class update. Everything runs as the global
// operator, because the namespace's own key stops authenticating once suspended.
func TestNamespaces_SuspendRefusesSchemaUpdates(t *testing.T) {
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)

	const (
		indexesClassName = "SuspendIndexes"
		// refusalText is the sentinel's own text, which a global operator sees.
		refusalText = "namespace is suspended"
	)
	qualifiedIndexes := ns1 + ":" + indexesClassName

	indexed := true

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
	}
	seed(t, ns1, user1Key)
	seed(t, ns2, user2Key)

	helper.SuspendNamespace(t, ns1, adminKey)
	// The resume is registered after the class cleanups, so LIFO runs it first.
	t.Cleanup(func() { helper.ResumeNamespace(t, ns1, adminKey) })

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

	// This row fails if the gate refuses whenever any namespace is suspended,
	// rather than only the one the collection belongs to.
	t.Run("the other namespace still takes the same requests", func(t *testing.T) {
		qualified2Indexes := ns2 + ":" + indexesClassName
		class := helper.GetClassAuth(t, qualified2Indexes, adminKey)
		class.Description = "updated beside a suspended namespace"
		_, err := helper.UpdateClassAuthWithReturn(t, qualified2Indexes, class, adminKey)
		require.NoError(t, err)
	})
}
