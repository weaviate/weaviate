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
		wantModel       string
		wantErr         error
	}{
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "apiEndpoint",
					"projectId":   "projectId",
				},
			},
			wantApiEndpoint: "apiEndpoint",
			wantProjectID:   "projectId",
			wantModel:       "textembedding-gecko-001",
			wantErr:         nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "apiEndpoint",
					"projectId":   "projectId",
					"model":       "textembedding-gecko-001",
				},
			},
			wantApiEndpoint: "apiEndpoint",
			wantProjectID:   "projectId",
			wantModel:       "textembedding-gecko-001",
			wantErr:         nil,
		},
		{
			name: "empty apiEndpoint",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "",
					"projectId":   "projectId",
					"model":       "textembedding-gecko-001",
				},
			},
			wantErr: errors.Errorf("apiEndpoint cannot be empty"),
		},
		{
			name: "empty projectId",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "apiEndpoint",
					"projectId":   "",
					"model":       "textembedding-gecko-001",
				},
			},
			wantErr: errors.Errorf("projectId cannot be empty"),
		},
		{
			name: "wrong model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "apiEndpoint",
					"projectId":   "projectId",
					"model":       "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong model available model names are: [textembedding-gecko-001]"),
		},
		{
			name: "all wrong",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"apiEndpoint": "",
					"projectId":   "",
					"model":       "wrong-model",
				},
			},
			wantErr: errors.Errorf("apiEndpoint cannot be empty, " +
				"projectId cannot be empty, " +
				"wrong model available model names are: [textembedding-gecko-001]"),
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

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	return nil
}
