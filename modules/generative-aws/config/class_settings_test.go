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
		name              string
		cfg               moduletools.ClassConfig
		wantService       string
		wantRegion        string
		wantModel         string
		wantMaxTokenCount int
		wantStopSequences []string
		wantTemperature   int
		wantTopP          int
		wantErr           error
	}{
		{
			name: "happy flow",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-east-1",
					"model":   "amazon.titan-tg1-large",
				},
			},
			wantService:       "bedrock",
			wantRegion:        "us-east-1",
			wantModel:         "amazon.titan-tg1-large",
			wantMaxTokenCount: 8192,
			wantStopSequences: []string{},
			wantTemperature:   0,
			wantTopP:          1,
		},
		{
			name: "custom values",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":       "bedrock",
					"region":        "us-east-1",
					"model":         "amazon.titan-tg1-large",
					"maxTokenCount": 4096,
					"stopSequences": []string{"test", "test2"},
					"temperature":   0.2,
					"topP":          0,
				},
			},
			wantService:       "bedrock",
			wantRegion:        "us-east-1",
			wantModel:         "amazon.titan-tg1-large",
			wantMaxTokenCount: 4096,
			wantStopSequences: []string{"test", "test2"},
			wantTemperature:   1,
			wantTopP:          0,
		},
		{
			name: "wrong temperature",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":     "bedrock",
					"region":      "us-east-1",
					"model":       "amazon.titan-tg1-large",
					"temperature": 2,
				},
			},
			wantErr: errors.Errorf("temperature has to be float value between 0 and 1"),
		},
		{
			name: "wrong maxTokenCount",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":       "bedrock",
					"region":        "us-east-1",
					"model":         "amazon.titan-tg1-large",
					"maxTokenCount": 9000,
				},
			},
			wantErr: errors.Errorf("maxTokenCount has to be an integer value between 1 and 8096"),
		},
		{
			name: "wrong topP",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service": "bedrock",
					"region":  "us-east-1",
					"model":   "amazon.titan-tg1-large",
					"topP":    2000,
				},
			},
			wantErr: errors.Errorf("topP has to be an integer value between 0 and 1"),
		},
		{
			name: "wrong all",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"maxTokenCount": 9000,
					"temperature":   2,
					"topP":          3,
				},
			},
			wantErr: errors.Errorf("service cannot be empty, " +
				"region cannot be empty, " +
				"wrong model available model names are: [amazon.titan-tg1-large], " +
				"maxTokenCount has to be an integer value between 1 and 8096, " +
				"temperature has to be float value between 0 and 1, " +
				"topP has to be an integer value between 0 and 1",
			),
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
