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

package alterschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
)

// testUpdateNamedVectorIndexType checks that changing the vector index type of
// a named vector is rejected with 422. The server used to panic on this update
// and answer 500.
func testUpdateNamedVectorIndexType() func(t *testing.T) {
	return func(t *testing.T) {
		for _, tt := range []struct{ from, to string }{
			{from: "hnsw", to: "flat"},
			{from: "flat", to: "hnsw"},
		} {
			t.Run(tt.from+" to "+tt.to, func(t *testing.T) {
				className := "UpdateNamedVectorIndexType"
				helper.DeleteClass(t, className)
				defer helper.DeleteClass(t, className)

				helper.CreateClass(t, &models.Class{
					Class: className,
					Properties: []*models.Property{
						{Name: "name", DataType: []string{schema.DataTypeText.String()}},
					},
					VectorConfig: map[string]models.VectorConfig{
						"v": {
							Vectorizer:      map[string]any{"none": map[string]any{}},
							VectorIndexType: tt.from,
						},
					},
				})

				update := helper.GetClass(t, className)
				require.NotNil(t, update)
				update.VectorConfig["v"] = models.VectorConfig{
					Vectorizer:      update.VectorConfig["v"].Vectorizer,
					VectorIndexType: tt.to,
				}

				_, err := helper.Client(t).Schema.SchemaObjectsUpdate(
					clschema.NewSchemaObjectsUpdateParams().
						WithClassName(className).WithObjectClass(update), nil)
				require.Error(t, err, "changing the index type of a named vector must be rejected")
				var unprocessable *clschema.SchemaObjectsUpdateUnprocessableEntity
				require.ErrorAs(t, err, &unprocessable, "expected 422, got: %v", err)
				assert.Contains(t, unprocessable.Payload.Error[0].Message,
					`vector index type of vector "v" is immutable`)

				got := helper.GetClass(t, className)
				require.NotNil(t, got)
				assert.Equal(t, tt.from, got.VectorConfig["v"].VectorIndexType)
			})
		}
	}
}
