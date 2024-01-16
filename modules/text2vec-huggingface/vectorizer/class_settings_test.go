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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_getPassageModel(t *testing.T) {
	tests := []struct {
		name             string
		cfg              moduletools.ClassConfig
		wantPassageModel string
		wantQueryModel   string
		wantWaitForModel bool
		wantUseGPU       bool
		wantUseCache     bool
		wantEndpointURL  string
		wantError        error
	}{
		{
			name: "CShorten/CORD-19-Title-Abstracts",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "CShorten/CORD-19-Title-Abstracts",
					"options": map[string]interface{}{
						"waitForModel": true,
						"useGPU":       false,
						"useCache":     false,
					},
				},
			},
			wantPassageModel: "CShorten/CORD-19-Title-Abstracts",
			wantQueryModel:   "CShorten/CORD-19-Title-Abstracts",
			wantWaitForModel: true,
			wantUseGPU:       false,
			wantUseCache:     false,
		},
		{
			name: "sentence-transformers/all-MiniLM-L6-v2",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "sentence-transformers/all-MiniLM-L6-v2",
				},
			},
			wantPassageModel: "sentence-transformers/all-MiniLM-L6-v2",
			wantQueryModel:   "sentence-transformers/all-MiniLM-L6-v2",
			wantWaitForModel: false,
			wantUseGPU:       false,
			wantUseCache:     true,
		},
		{
			name: "DPR models",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"passageModel": "sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base",
					"queryModel":   "sentence-transformers/facebook-dpr-question_encoder-single-nq-base",
				},
			},
			wantPassageModel: "sentence-transformers/facebook-dpr-ctx_encoder-single-nq-base",
			wantQueryModel:   "sentence-transformers/facebook-dpr-question_encoder-single-nq-base",
			wantWaitForModel: false,
			wantUseGPU:       false,
			wantUseCache:     true,
		},
		{
			name: "Hugging Face Inference API - endpointURL",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpointURL": "http://endpoint.cloud",
				},
			},
			wantPassageModel: "",
			wantQueryModel:   "",
			wantWaitForModel: false,
			wantUseGPU:       false,
			wantUseCache:     true,
			wantEndpointURL:  "http://endpoint.cloud",
		},
		{
			name: "Hugging Face Inference API - endpointUrl",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"endpointUrl": "http://endpoint.cloud",
				},
			},
			wantPassageModel: "",
			wantQueryModel:   "",
			wantWaitForModel: false,
			wantUseGPU:       false,
			wantUseCache:     true,
			wantEndpointURL:  "http://endpoint.cloud",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			assert.Equal(t, tt.wantPassageModel, ic.getPassageModel())
			assert.Equal(t, tt.wantQueryModel, ic.getQueryModel())
			assert.Equal(t, tt.wantWaitForModel, ic.OptionWaitForModel())
			assert.Equal(t, tt.wantUseGPU, ic.OptionUseGPU())
			assert.Equal(t, tt.wantUseCache, ic.OptionUseCache())
			assert.Equal(t, tt.wantEndpointURL, ic.EndpointURL())
			assert.Equal(t, tt.wantError, ic.validateClassSettings())
		})
	}
}
