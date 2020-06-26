//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package projector

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjector(t *testing.T) {

	p := New()

	t.Run("with multiple results", func(t *testing.T) {
		vectors := [][]float32{
			[]float32{1, 0, 0, 0, 0},
			[]float32{0, 0, 1, 0, 0},
			[]float32{1, 1, 1, 0, 0},
		}

		testData := []search.Result{
			search.Result{
				Schema: map[string]interface{}{"name": "item1"},
				Vector: vectors[0],
			},
			search.Result{
				Schema: map[string]interface{}{"name": "item2"},
				Vector: vectors[1],
			},
			search.Result{
				Schema: map[string]interface{}{"name": "item3"},
				Vector: vectors[2],
				UnderscoreProperties: &models.UnderscoreProperties{
					Classification: &models.UnderscorePropertiesClassification{ // verify it doesn't remove existing underscore props
						ID: strfmt.UUID("123"),
					},
				},
			},
		}

		res, err := p.Reduce(testData, &Params{})
		require.Nil(t, err)
		assert.Len(t, res, len(testData))
		assert.Equal(t, res[2].UnderscoreProperties.Classification.ID, strfmt.UUID("123"),
			"existing underscores should not be removed")

		assert.Len(t, res[0].UnderscoreProperties.FeatureProjection.Vector, 2)
		assert.Len(t, res[1].UnderscoreProperties.FeatureProjection.Vector, 2)
		assert.Len(t, res[2].UnderscoreProperties.FeatureProjection.Vector, 2)
	})
}
