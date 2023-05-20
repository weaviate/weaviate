//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
	class "github.com/weaviate/weaviate/modules/text2vec-huggingface/vectorizer"
)

func Test_huggingFaceUrlBuilder_url(t *testing.T) {
	config := ent.VectorizationConfig{
		Origin:   class.DefaultOrigin,
		PathMask: class.DefaultPathMask,
	}

	tests := []struct {
		name  string
		model string
		want  string
	}{
		{
			name:  "Facebook DPR model",
			model: "sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
			want:  "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
		},
		{
			name:  "BERT base model (uncased)",
			model: "bert-base-uncased",
			want:  "https://api-inference.huggingface.co/pipeline/feature-extraction/bert-base-uncased",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, newHuggingFaceUrlBuilder(config).url(tt.model))
		})
	}
}
