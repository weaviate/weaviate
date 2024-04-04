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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name      string
		cfg       moduletools.ClassConfig
		wantModel string
		wantErr   error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel: "mistral",
			wantErr:   nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "llama2",
				},
			},
			wantErr: nil,
		},
		{
			name: "unsupported model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "MarcinLLM",
				},
			},
			wantErr: errors.New("wrong Ollama model name, please see available models on https://weaviate.io/developers/weaviate/modules/reader-generator-modules/generative-ollama"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr.Error(), ic.Validate(nil).Error())
			} else {
				assert.Equal(t, tt.wantModel, ic.Model())
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
