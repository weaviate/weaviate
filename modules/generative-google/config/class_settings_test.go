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
	"fmt"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantApiEndpoint string
		wantProjectID   string
		wantModelID     string
		wantModel       string
		wantTemperature float64
		wantTokenLimit  int
		wantTopK        int
		wantTopP        float64
		wantErr         error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "chat-bison-001",
			wantModel:       "",
			wantTemperature: 1.0,
			wantTokenLimit:  1024,
			wantTopK:        40,
			wantTopP:        0.95,
			wantErr:         nil,
		},
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "projectId",
				},
			},
			wantApiEndpoint: "us-central1-aiplatform.googleapis.com",
			wantProjectID:   "projectId",
			wantModelID:     "chat-bison",
			wantTemperature: 1.0,
			wantTokenLimit:  1024,
			wantTopK:        40,
			wantTopP:        0.95,
			wantErr:         nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "google.com",
					"projectId":   "cloud-project",
					"modelId":     "model-id",
					"temperature": 0.25,
					"tokenLimit":  254,
					"topK":        30,
					"topP":        0.97,
				},
			},
			wantApiEndpoint: "google.com",
			wantProjectID:   "cloud-project",
			wantModelID:     "model-id",
			wantTemperature: 0.25,
			wantTokenLimit:  254,
			wantTopK:        30,
			wantTopP:        0.97,
			wantErr:         nil,
		},
		{
			name: "wrong temperature",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId":   "cloud-project",
					"temperature": 2,
				},
			},
			wantErr: errors.Errorf("temperature has to be float value between 0 and 1"),
		},
		{
			name: "wrong tokenLimit",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId":  "cloud-project",
					"tokenLimit": 2000,
				},
			},
			wantErr: errors.Errorf("tokenLimit has to be an integer value between 1 and 1024"),
		},
		{
			name: "wrong topP",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"projectId": "cloud-project",
					"topP":      3,
				},
			},
			wantErr: errors.Errorf("topP has to be float value between 0 and 1"),
		},
		{
			name: "wrong all",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"temperature": 2,
					"tokenLimit":  2000,
					"topK":        2000,
					"topP":        3,
				},
			},
			wantErr: errors.Errorf("temperature has to be float value between 0 and 1, " +
				"tokenLimit has to be an integer value between 1 and 1024, " +
				"topP has to be float value between 0 and 1"),
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
			wantModelID:     "chat-bison-001",
			wantTemperature: 1.0,
			wantTokenLimit:  1024,
			wantTopK:        40,
			wantTopP:        0.95,
			wantErr:         nil,
		},
		{
			name: "Generative AI with model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
					"modelId":     "chat-bison-001",
				},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "chat-bison-001",
			wantTemperature: 1.0,
			wantTokenLimit:  1024,
			wantTopK:        40,
			wantTopP:        0.95,
			wantErr:         nil,
		},
		{
			name: "Generative AI with gemini-ultra model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
					"modelId":     "gemini-ultra",
				},
			},
			wantApiEndpoint: "generativelanguage.googleapis.com",
			wantProjectID:   "",
			wantModelID:     "gemini-ultra",
			wantTemperature: 1.0,
			wantTokenLimit:  1024,
			wantTopK:        40,
			wantTopP:        0.95,
			wantErr:         nil,
		},
		{
			name: "Generative AI with not supported model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "generativelanguage.googleapis.com",
					"modelId":     "unsupported-model",
				},
			},
			wantErr: fmt.Errorf("unsupported-model is not supported available models are: " +
				"[chat-bison-001 gemini-pro gemini-ultra gemini-1.5-flash-latest gemini-1.5-pro-latest chat-bison chat-bison-32k chat-bison@002 chat-bison-32k@002 chat-bison@001 gemini-1.5-pro-preview-0514 gemini-1.5-pro-preview-0409 gemini-1.5-flash-preview-0514 gemini-1.0-pro-002 gemini-1.0-pro-001 gemini-1.0-pro]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(nil), tt.wantErr.Error())
			} else {
				assert.NoError(t, ic.Validate(nil))
				assert.Equal(t, tt.wantApiEndpoint, ic.ApiEndpoint())
				assert.Equal(t, tt.wantProjectID, ic.ProjectID())
				assert.Equal(t, tt.wantModelID, ic.ModelID())
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantTokenLimit, ic.TokenLimit())
				assert.Equal(t, tt.wantTopK, ic.TopK())
				assert.Equal(t, tt.wantTopP, ic.TopP())
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

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
