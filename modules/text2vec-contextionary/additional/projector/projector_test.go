//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package projector

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	txt2vecmodels "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
)

func TestProjector(t *testing.T) {
	p := New()

	t.Run("with multiple results", func(t *testing.T) {
		vectors := [][]float32{
			{1, 0, 0, 0, 0},
			{0, 0, 1, 0, 0},
			{1, 1, 1, 0, 0},
		}

		testData := []search.Result{
			{
				Schema: map[string]interface{}{"name": "item1"},
				Vector: vectors[0],
			},
			{
				Schema: map[string]interface{}{"name": "item2"},
				Vector: vectors[1],
			},
			{
				Schema: map[string]interface{}{"name": "item3"},
				Vector: vectors[2],
				AdditionalProperties: map[string]interface{}{
					"classification": &additional.Classification{ // verify it doesn't remove existing additional props
						ID: strfmt.UUID("123"),
					},
				},
			},
		}

		res, err := p.Reduce(testData, &Params{})
		require.Nil(t, err)
		assert.Len(t, res, len(testData))
		classification, classificationOK := res[2].AdditionalProperties["classification"]
		assert.True(t, classificationOK)
		classificationElement, classificationElementOK := classification.(*additional.Classification)
		assert.True(t, classificationElementOK)
		assert.Equal(t, classificationElement.ID, strfmt.UUID("123"),
			"existing additionals should not be removed")
		for i := 0; i < 3; i++ {
			featureProjection, featureProjectionOK := res[i].AdditionalProperties["featureProjection"]
			assert.True(t, featureProjectionOK)
			fpElement, fpElementOK := featureProjection.(*txt2vecmodels.FeatureProjection)
			assert.True(t, fpElementOK)
			assert.Len(t, fpElement.Vector, 2)
		}
	})
}
