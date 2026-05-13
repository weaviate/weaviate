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

package traverser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
)

func TestAttachQueryVector(t *testing.T) {
	optIn := dto.GetParams{AdditionalProperties: additional.Properties{QueryVector: true}}
	optOut := dto.GetParams{AdditionalProperties: additional.Properties{QueryVector: false}}

	tests := []struct {
		name           string
		params         dto.GetParams
		single         models.Vector
		named          models.Vectors
		wantAttached   bool
		wantNamedShape bool
	}{
		{
			name:         "opt-out: vector present but not attached",
			params:       optOut,
			single:       []float32{0.1, 0.2, 0.3},
			wantAttached: false,
		},
		{
			name:         "opt-in single vector: attached to row 0",
			params:       optIn,
			single:       []float32{0.1, 0.2, 0.3},
			wantAttached: true,
		},
		{
			name:           "opt-in named vectors",
			params:         optIn,
			named:          models.Vectors{"title": []float32{0.1, 0.2}, "body": []float32{0.3, 0.4}},
			wantAttached:   true,
			wantNamedShape: true,
		},
		{
			name:         "opt-in but no vector (BM25-only)",
			params:       optIn,
			wantAttached: false,
		},
		{
			name:           "opt-in multi-vec (ColBERT-style)",
			params:         optIn,
			named:          models.Vectors{"col": [][]float32{{0.1, 0.2}, {0.3, 0.4}}},
			wantAttached:   true,
			wantNamedShape: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := []search.Result{{ID: "a"}, {ID: "b"}, {ID: "c"}}
			out := attachQueryVector(res, tt.params, tt.single, tt.named)

			if tt.wantAttached {
				require := assert.New(t)
				require.NotNil(out[0].AdditionalProperties["queryVector"])
				require.NotNil(out[0].AdditionalProperties["queryVectorRaw"])

				raw, ok := out[0].AdditionalProperties["queryVectorRaw"].(models.Vectors)
				require.True(ok, "queryVectorRaw must be models.Vectors")

				if tt.wantNamedShape {
					require.Len(raw, len(tt.named))
					for name, vec := range tt.named {
						require.Equal(vec, raw[name])
					}
				} else {
					vec, ok := raw[""]
					require.True(ok, "single-vector classes carry one entry under empty key")
					require.Equal(tt.single, vec)
				}
			} else {
				assert.Nil(t, out[0].AdditionalProperties["queryVector"])
				assert.Nil(t, out[0].AdditionalProperties["queryVectorRaw"])
			}

			// queryVector is a singleton — never on subsequent rows.
			assert.Nil(t, out[1].AdditionalProperties["queryVector"])
			assert.Nil(t, out[2].AdditionalProperties["queryVector"])
		})
	}
}

func TestAttachQueryVector_EmptyResults(t *testing.T) {
	out := attachQueryVector(nil,
		dto.GetParams{AdditionalProperties: additional.Properties{QueryVector: true}},
		[]float32{0.1, 0.2}, nil)
	assert.Nil(t, out)
}

func TestAttachQueryVector_PreservesExistingAdditional(t *testing.T) {
	res := []search.Result{{
		ID:                   "a",
		AdditionalProperties: models.AdditionalProperties{"existing": "value"},
	}}
	out := attachQueryVector(res,
		dto.GetParams{AdditionalProperties: additional.Properties{QueryVector: true}},
		[]float32{0.1}, nil)
	assert.Equal(t, "value", out[0].AdditionalProperties["existing"])
	assert.NotNil(t, out[0].AdditionalProperties["queryVector"])
}
