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

package config

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantApiEndpoint string
		wantProjectID   string
		wantModelID     string
		wantTitle       string
		wantErr         error
	}{
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
				},
			},
			wantApiEndpoint: "us-central1-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "textembedding-gecko@001",
			wantErr:         nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint":   "google.com",
					"projectId":     "projectId",
					"titleProperty": "title",
				},
			},
			wantApiEndpoint: "google.com",
			wantProjectID:   "projectId",
			wantModelID:     "textembedding-gecko@001",
			wantTitle:       "title",
			wantErr:         nil,
		},
		{
			name: "empty projectId",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "",
				},
			},
			wantErr: errors.Errorf("projectId cannot be empty"),
		},
		{
			name: "wrong modelId",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
					"modelId":   "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong modelId available model names are: " +
				"[textembedding-gecko@001 textembedding-gecko@latest " +
				"textembedding-gecko-multilingual@latest textembedding-gecko@003 " +
				"textembedding-gecko@002 textembedding-gecko-multilingual@001 textembedding-gecko@001]"),
		},
		{
			name: "all wrong",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "",
					"modelId":   "wrong-model",
				},
			},
			wantErr: errors.Errorf("projectId cannot be empty, " +
				"wrong modelId available model names are: " +
				"[textembedding-gecko@001 textembedding-gecko@latest " +
				"textembedding-gecko-multilingual@latest textembedding-gecko@003 " +
				"textembedding-gecko@002 textembedding-gecko-multilingual@001 textembedding-gecko@001]"),
		},
		{
			name: "Generative AI",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
				},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "embedding-gecko-001",
			wantErr:         nil,
		},
		{
			name: "Generative AI with model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
					"modelId":     "embedding-gecko-001",
				},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "embedding-gecko-001",
			wantErr:         nil,
		},
		{
			name: "Generative AI with wrong model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
					"modelId":     "textembedding-gecko@001",
				},
			},
			wantErr: errors.Errorf("wrong modelId available Generative AI model names are: [embedding-gecko-001]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(nil), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantApiEndpoint, ic.ApiEndpoint())
				assert.Equal(t, tt.wantProjectID, ic.ProjectID())
				assert.Equal(t, tt.wantModelID, ic.ModelID())
				assert.Equal(t, tt.wantTitle, ic.TitleProperty())
			}
		})
	}
}

type fakeClassConfig struct {
	classConfig map[string]interface{}
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}
