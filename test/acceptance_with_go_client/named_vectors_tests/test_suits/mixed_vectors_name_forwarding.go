//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test_suits

import (
	"context"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
)

func testMixedVectorsDefaultNameForwarding(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: endpoint})
		require.NoError(t, err)

		class := createMixedVectorsSchema(t, client)

		verifyObject := func(t *testing.T, id string, expectVector []float32) {
			objWrap, err := client.Data().ObjectsGetter().
				WithClassName(class.Class).
				WithID(id).
				WithVector().
				Do(ctx)
			require.NoError(t, err)

			require.Len(t, objWrap, 1)
			obj := objWrap[0]
			require.Equal(t, []float32(obj.Vector), expectVector)
			require.Len(t, obj.Vectors, 3)
		}

		t.Run("simple", func(t *testing.T) {
			_, err = client.Data().Creator().
				WithClassName(class.Class).
				WithID(UUID1).
				WithProperties(map[string]any{
					"text": "Lorem ipsum dolor sit amet",
				}).
				WithVectors(map[string]models.Vector{
					modelsext.DefaultNamedVectorName: []float32{1, 2, 3},
				}).
				Do(ctx)
			require.NoError(t, err)

			verifyObject(t, UUID1, []float32{1, 2, 3})

			err = client.Data().Updater().
				WithClassName(class.Class).
				WithID(UUID1).
				WithVectors(map[string]models.Vector{
					modelsext.DefaultNamedVectorName: []float32{4, 5, 6},
				}).
				Do(ctx)
			require.NoError(t, err)

			verifyObject(t, UUID1, []float32{4, 5, 6})
		})

		t.Run("batch", func(t *testing.T) {
			resp, err := client.Batch().ObjectsBatcher().
				WithObjects(&models.Object{
					Class: class.Class,
					ID:    UUID2,
					Properties: map[string]any{
						"text": "Lorem ipsum dolor sit amet",
					},
					Vectors: map[string]models.Vector{
						modelsext.DefaultNamedVectorName: []float32{1, 2, 3},
					},
				}).
				Do(ctx)
			require.NoError(t, err)
			for _, r := range resp {
				require.Empty(t, r.Result.Errors, spew.Sdump(r.Result))
			}

			verifyObject(t, UUID2, []float32{1, 2, 3})
		})
	}
}
