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

package drop_vector_index

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testLastVectorDropToVectorless pins dropping a collection's ONLY named
// vector: the drop completes and the collection becomes vector-less — the
// same shape as creating a collection without any vector config (empty
// VectorConfig, inert legacy fields with vectorizer "none"; never the
// server's default vectorizer module, which would silently start vectorizing
// new writes). Objects and their properties survive; only the vectors go.
func testLastVectorDropToVectorless() func(t *testing.T) {
	return func(t *testing.T) {
		const (
			className = "DropVectorIndexLastVector"
			only      = "onlyvec"
			dim       = 32
			count     = 20
		)

		deleteParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)
		defer helper.Client(t).Schema.SchemaObjectsDelete(deleteParams, nil)

		t.Run("create single-vector class and pump data", func(t *testing.T) {
			cls := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "name", DataType: []string{schema.DataTypeText.String()}},
				},
				VectorConfig: map[string]models.VectorConfig{only: noneVectorConfig()},
			}
			_, err := helper.Client(t).Schema.SchemaObjectsCreate(
				clschema.NewSchemaObjectsCreateParams().WithObjectClass(cls), nil)
			require.NoError(t, err)

			batch := make([]*models.Object, count)
			for i := range count {
				batch[i] = &models.Object{
					ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-0000005%05d", i)),
					Class:      className,
					Properties: map[string]any{"name": fmt.Sprintf("object-%d", i)},
					Vectors:    models.Vectors{only: randVec(dim, float32(i))},
				}
			}
			helper.CreateObjectsBatch(t, batch)
			require.Equal(t, 3, nearVectorResults(t, className, only, randVec(dim, 7), 3))
		})

		t.Run("dropping the only named vector is accepted and finalizes", func(t *testing.T) {
			dropTargetVector(t, className, only)
			eventuallyTargetVectorRemoved(t, className, only)
		})

		t.Run("the collection is now vector-less with the inert legacy shape", func(t *testing.T) {
			cls, err := getClassErr(className)
			require.NoError(t, err)
			require.Empty(t, cls.VectorConfig)
			require.Equal(t, "none", cls.Vectorizer,
				"the flip must land on the inert vectorizer, never the server default module")
		})

		t.Run("objects survive without vectors; new vector-less writes work", func(t *testing.T) {
			objs := listObjectsWithVectors(t, className, "", count)
			require.Len(t, objs, count)
			for _, obj := range objs {
				require.NotContains(t, obj.Vectors, only, "stripped vectors must not resurface")
				props, ok := obj.Properties.(map[string]interface{})
				require.True(t, ok)
				require.NotEmpty(t, props["name"], "properties must survive the drop")
			}

			extra := []*models.Object{{
				ID:         strfmt.UUID("00000000-0000-0000-0000-000000599999"),
				Class:      className,
				Properties: map[string]any{"name": "post-flip"},
			}}
			helper.CreateObjectsBatch(t, extra)
		})
	}
}
