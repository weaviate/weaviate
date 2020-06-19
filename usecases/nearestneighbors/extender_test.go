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

package nearestneighbor

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtender(t *testing.T) {
	f := &fakeContextionary{}
	e := NewExtender(f)

	vectors := [][]float32{
		[]float32{0.1, 0.2, 0.3},
		[]float32{0.11, 0.22, 0.33},
		[]float32{0.111, 0.222, 0.333},
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

	expectedResults := []search.Result{
		search.Result{
			Schema: map[string]interface{}{"name": "item1"},
			Vector: vectors[0],
			UnderscoreProperties: &models.UnderscoreProperties{
				NearestNeighbors: &models.NearestNeighbors{
					Neighbors: []*models.NearestNeighbor{
						&models.NearestNeighbor{
							Concept:  "word1",
							Distance: 1,
						},
						&models.NearestNeighbor{
							Concept:  "word2",
							Distance: 2,
						},
						&models.NearestNeighbor{
							Concept:  "word3",
							Distance: 3,
						},
					},
				},
			},
		},
		search.Result{
			Schema: map[string]interface{}{"name": "item2"},
			Vector: vectors[1],
			UnderscoreProperties: &models.UnderscoreProperties{
				NearestNeighbors: &models.NearestNeighbors{
					Neighbors: []*models.NearestNeighbor{
						&models.NearestNeighbor{
							Concept:  "word4",
							Distance: 0.1,
						},
						&models.NearestNeighbor{
							Concept:  "word5",
							Distance: 0.2,
						},
						&models.NearestNeighbor{
							Concept:  "word6",
							Distance: 0.3,
						},
					},
				},
			},
		},
		search.Result{
			Schema: map[string]interface{}{"name": "item3"},
			Vector: vectors[2],
			UnderscoreProperties: &models.UnderscoreProperties{
				Classification: &models.UnderscorePropertiesClassification{ // verify it doesn't remove existing underscore props
					ID: strfmt.UUID("123"),
				},
				NearestNeighbors: &models.NearestNeighbors{
					Neighbors: []*models.NearestNeighbor{
						&models.NearestNeighbor{
							Concept:  "word5",
							Distance: 1.1,
						},
						&models.NearestNeighbor{
							Concept:  "word6",
							Distance: 1.2,
						},
						&models.NearestNeighbor{
							Concept:  "word7",
							Distance: 1.3,
						},
					},
				},
			},
		},
	}

	res, err := e.Do(testData)
	require.Nil(t, err)
	assert.Equal(t, expectedResults, res)
	assert.Equal(t, f.calledWithVectors, vectors)
}

type fakeContextionary struct {
	calledWithVectors [][]float32
}

func (f *fakeContextionary) MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, k, n int) ([][]string, [][]float32, error) {

	f.calledWithVectors = vectors
	words := [][]string{
		[]string{"word1", "word2", "word3"},
		[]string{"word4", "word5", "word6"},
		[]string{"word7", "word8", "word9"},
	}

	distances := [][]float32{
		[]float32{1.0, 2.0, 3.0},
		[]float32{0.1, 0.2, 0.3},
		[]float32{1.1, 2.2, 3.3},
	}

	return words, distances, nil
}
