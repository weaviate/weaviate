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
		name        string
		cfg         moduletools.ClassConfig
		wantService string
		wantRegion  string
		wantModel   string
		wantErr     error
	}{
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-east-1",
					"model":   "amazon.titan-e1t-medium",
				},
			},
			wantService: "bedrock",
			wantRegion:  "us-east-1",
			wantModel:   "amazon.titan-e1t-medium",
			wantErr:     nil,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-west-1",
					"model":   "amazon.titan-e1t-medium",
				},
			},
			wantService: "bedrock",
			wantRegion:  "us-west-1",
			wantModel:   "amazon.titan-e1t-medium",
			wantErr:     nil,
		},
		{
			name: "empty service",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"region": "us-east-1",
					"model":  "amazon.titan-e1t-medium",
				},
			},
			wantErr: errors.Errorf("service cannot be empty"),
		},
		{
			name: "empty region",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"model":   "amazon.titan-e1t-medium",
				},
			},
			wantErr: errors.Errorf("region cannot be empty"),
		},
		{
			name: "wrong model",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-west-1",
					"model":   "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong model available model names are: [amazon.titan-e1t-medium]"),
		},
		{
			name: "all wrong",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "",
					"region":  "",
					"model":   "",
				},
			},
			wantErr: errors.Errorf("service cannot be empty, region cannot be empty, " +
				"wrong model available model names are: [amazon.titan-e1t-medium]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.EqualError(t, ic.Validate(nil), tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.wantService, ic.Service())
				assert.Equal(t, tt.wantRegion, ic.Region())
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
