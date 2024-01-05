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

package vectorizer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func TestInspector(t *testing.T) {
	type test struct {
		name           string
		input          string
		expectedErr    error
		expectedOutput *models.C11yWordsResponse
	}

	tests := []test{
		{
			name:        "invalid input",
			input:       "i don't like pizza",
			expectedErr: fmt.Errorf("invalid word input: words must only contain unicode letters and digits"),
		},
		{
			name:  "single valid word",
			input: "pizza",
			expectedOutput: &models.C11yWordsResponse{
				IndividualWords: []*models.C11yWordsResponseIndividualWordsItems0{
					{
						Present: true,
						Word:    "pizza",
						Info: &models.C11yWordsResponseIndividualWordsItems0Info{
							Vector: []float32{3, 2, 1, 0},
							NearestNeighbors: []*models.C11yNearestNeighborsItems0{
								{
									Distance: 0.1,
									Word:     "word1",
								},
								{
									Distance: 0.2,
									Word:     "word2",
								},
							},
						},
					},
				},
			},
		},
		{
			name:  "single valid word containing numbers",
			input: "pi55a",
			expectedOutput: &models.C11yWordsResponse{
				IndividualWords: []*models.C11yWordsResponseIndividualWordsItems0{
					{
						Present: true,
						Word:    "pi55a",
						Info: &models.C11yWordsResponseIndividualWordsItems0Info{
							Vector: []float32{3, 2, 1, 0},
							NearestNeighbors: []*models.C11yNearestNeighborsItems0{
								{
									Distance: 0.1,
									Word:     "word1",
								},
								{
									Distance: 0.2,
									Word:     "word2",
								},
							},
						},
					},
				},
			},
		},
		{
			name:  "concatenated words",
			input: "pizzaBakerMakerShaker",
			expectedOutput: &models.C11yWordsResponse{
				ConcatenatedWord: &models.C11yWordsResponseConcatenatedWord{
					ConcatenatedWord:   "pizzaBakerMakerShaker",
					SingleWords:        []string{"pizza", "baker", "maker", "shaker"},
					ConcatenatedVector: []float32{0, 1, 2, 3},
					ConcatenatedNearestNeighbors: []*models.C11yNearestNeighborsItems0{
						{
							Distance: 0.1,
							Word:     "word1",
						},
						{
							Distance: 0.2,
							Word:     "word2",
						},
					},
				},
				IndividualWords: []*models.C11yWordsResponseIndividualWordsItems0{
					{
						Present: true,
						Word:    "pizza",
						Info: &models.C11yWordsResponseIndividualWordsItems0Info{
							Vector: []float32{3, 2, 1, 0},
							NearestNeighbors: []*models.C11yNearestNeighborsItems0{
								{
									Distance: 0.1,
									Word:     "word1",
								},
								{
									Distance: 0.2,
									Word:     "word2",
								},
							},
						},
					},
					{
						Present: true,
						Word:    "baker",
						Info: &models.C11yWordsResponseIndividualWordsItems0Info{
							Vector: []float32{3, 2, 1, 0},
							NearestNeighbors: []*models.C11yNearestNeighborsItems0{
								{
									Distance: 0.1,
									Word:     "word1",
								},
								{
									Distance: 0.2,
									Word:     "word2",
								},
							},
						},
					},
					{
						Present: true,
						Word:    "maker",
						Info: &models.C11yWordsResponseIndividualWordsItems0Info{
							Vector: []float32{3, 2, 1, 0},
							NearestNeighbors: []*models.C11yNearestNeighborsItems0{
								{
									Distance: 0.1,
									Word:     "word1",
								},
								{
									Distance: 0.2,
									Word:     "word2",
								},
							},
						},
					},
					{
						Present: true,
						Word:    "shaker",
						Info: &models.C11yWordsResponseIndividualWordsItems0Info{
							Vector: []float32{3, 2, 1, 0},
							NearestNeighbors: []*models.C11yNearestNeighborsItems0{
								{
									Distance: 0.1,
									Word:     "word1",
								},
								{
									Distance: 0.2,
									Word:     "word2",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeClient{}
			i := NewInspector(client)
			res, err := i.GetWords(context.Background(), test.input)
			require.Equal(t, err, test.expectedErr)
			assert.Equal(t, res, test.expectedOutput)
		})
	}
}
