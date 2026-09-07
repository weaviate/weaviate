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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/rerankertest"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         moduletools.ClassConfig
		wantModel   string
		wantBaseUrl string
		wantErr     error
	}{
		{
			name: "default settings",
			cfg: rerankertest.FakeClassConfig{
				ClassConfig: map[string]interface{}{},
			},
			wantModel:   "rerank-lite-1",
			wantBaseUrl: "https://api.voyageai.com/v1",
		},
		{
			name: "custom settings",
			cfg: rerankertest.FakeClassConfig{
				ClassConfig: map[string]interface{}{
					"model":   "rerank-lite-1",
					"baseURL": "http://base-url.com",
				},
			},
			wantModel:   "rerank-lite-1",
			wantBaseUrl: "http://base-url.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(nil), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantBaseUrl, ic.BaseURL())
			}
		})
	}
}

func Test_classSettings_ValidateBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	tests := append(rerankertest.SSRFTestCases(), rerankertest.SSRFTestCase{
		Name: "default URL is valid", BaseURL: "https://api.voyageai.com/v1", WantErr: false,
	})
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ic := NewClassSettings(rerankertest.FakeClassConfig{
				ClassConfig: map[string]interface{}{
					"model":   "rerank-lite-1",
					"baseURL": tt.BaseURL,
				},
			})
			err := ic.Validate(nil)
			if tt.WantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
