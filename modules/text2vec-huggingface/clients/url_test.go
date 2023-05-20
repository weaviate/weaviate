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
)

func Test_getHuggingFaceUrl(t *testing.T) {
	tests := []struct {
		name   string
		config ent.VectorizationConfig
		want   string
	}{
		{
			name: "Facebook DPR model",
			config: ent.VectorizationConfig{
				Model: "sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
			},
			want: "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
		},
		{
			name: "BERT base model (uncased)",
			config: ent.VectorizationConfig{
				Model: "bert-base-uncased",
			},
			want: "https://api-inference.huggingface.co/pipeline/feature-extraction/bert-base-uncased",
		},
		{
			name: "BERT base model (uncased)",
			config: ent.VectorizationConfig{
				EndpointURL: "https://self-hosted-instance.com/bert-base-uncased",
			},
			want: "https://self-hosted-instance.com/bert-base-uncased",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getHuggingFaceUrl(tt.config))
		})
	}
}
