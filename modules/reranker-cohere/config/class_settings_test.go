//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
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
			cfg: fakeClassConfig{
				classConfig: map[string]any{},
			},
			wantModel:   "rerank-v3.5",
			wantBaseUrl: "https://api.cohere.ai",
		},
		{
			name: "custom settings",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":   "rerank-english-v2.0",
					"baseURL": "http://base-url.com",
				},
			},
			wantModel:   "rerank-english-v2.0",
			wantBaseUrl: "http://base-url.com",
		},
		{
			name: "empty model",
			cfg: fakeClassConfig{
				classConfig: map[string]any{
					"model":   "",
					"baseURL": "http://base-url.com",
				},
			},
			wantErr: errors.New("no model provided"),
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

type fakeClassConfig struct {
	classConfig map[string]any
}

func (f fakeClassConfig) Class() map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	return nil
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f fakeClassConfig) Config() *config.Config {
	return nil
}
