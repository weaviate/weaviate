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

package ent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_classSettings_ValidateBaseURL(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	class := &models.Class{
		Class: "test",
		Properties: []*models.Property{
			{
				DataType: []string{schema.DataTypeText.String()},
				Name:     "test",
			},
		},
	}
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{
			name:    "valid HTTPS URL",
			baseURL: "https://api.openai.com",
			wantErr: false,
		},
		{
			name:    "HTTP URL is rejected",
			baseURL: "http://api.example.com",
			wantErr: true,
		},
		{
			name:    "loopback address is rejected",
			baseURL: "https://127.0.0.1",
			wantErr: true,
		},
		{
			name:    "private network address is rejected",
			baseURL: "https://192.168.1.1",
			wantErr: true,
		},
		{
			name:    "empty host is rejected",
			baseURL: "https://",
			wantErr: true,
		},
		{
			name:    "localhost is rejected",
			baseURL: "https://localhost",
			wantErr: true,
		},
		{
			name:    "local domain is rejected",
			baseURL: "https://myhost.local",
			wantErr: true,
		},
		{
			name:    "default URL is valid",
			baseURL: DefaultBaseURL,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewClassSettings(&fakeClassConfig{
				classConfig: map[string]any{
					"baseURL": tt.baseURL,
				},
			})
			err := cs.Validate(class)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

type fakeClassConfig struct {
	classConfig           map[string]any
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f fakeClassConfig) Class() map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]any {
	if propName == f.skippedProperty {
		return map[string]any{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]any{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]any{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
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
