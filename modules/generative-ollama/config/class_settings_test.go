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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_classSettings_Validate(t *testing.T) {
	tests := []struct {
		name            string
		cfg             moduletools.ClassConfig
		wantApiEndpoint string
		wantModel       string
		wantErr         error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantApiEndpoint: "http://localhost:11434",
			wantModel:       "llama3",
			wantErr:         nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "mistral",
				},
			},
			wantApiEndpoint: "http://localhost:11434",
			wantModel:       "mistral",
			wantErr:         nil,
		},
		{
			name: "empty model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "",
				},
			},
			wantErr: errors.New("model cannot be empty"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				err := ic.Validate(nil)
				require.Error(t, err)
				assert.Equal(t, tt.wantErr.Error(), err.Error())
			} else {
				assert.NoError(t, ic.Validate(nil))
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

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}
