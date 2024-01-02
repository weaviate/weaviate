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
	t.Skip("Skipping this test for now")
	tests := []struct {
		name              string
		cfg               moduletools.ClassConfig
		wantService       string
		wantRegion        string
		wantModel         string
		wantEndpoint      string
		wantTargetModel   string
		wantTargetVariant string
		wantMaxTokenCount int
		wantStopSequences []string
		wantTemperature   float64
		wantTopP          int
		wantErr           error
	}{
		{
			name: "happy flow - Bedrock",
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
			name: "happy flow - Sagemaker",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":       "sagemaker",
					"region":        "us-east-1",
					"endpoint":      "my-endpoint-deployment",
					"targetModel":   "model",
					"targetVariant": "variant-1",
				},
			},
			wantService:       "sagemaker",
			wantRegion:        "us-east-1",
			wantEndpoint:      "my-endpoint-deployment",
			wantTargetModel:   "model",
			wantTargetVariant: "variant-1",
		},
		{
			name: "custom values - Bedrock",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":       "bedrock",
					"region":        "us-east-1",
					"model":         "amazon.titan-tg1-large",
					"maxTokenCount": 1,
					"stopSequences": []string{"test", "test2"},
					"temperature":   0.2,
					"topP":          0,
				},
			},
			wantService:       "bedrock",
			wantRegion:        "us-east-1",
			wantModel:         "amazon.titan-tg1-large",
			wantMaxTokenCount: 1,
			wantStopSequences: []string{"test", "test2"},
			wantTemperature:   0.2,
			wantTopP:          0,
		},
		{
			name: "custom values - Sagemaker",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"service":       "sagemaker",
					"region":        "us-east-1",
					"endpoint":      "this-is-my-endpoint",
					"targetModel":   "my-target-model",
					"targetVariant": "my-target¬variant",
				},
			},
			wantService:       "sagemaker",
			wantRegion:        "us-east-1",
			wantEndpoint:      "this-is-my-endpoint",
			wantTargetModel:   "my-target-model",
			wantTargetVariant: "my-target¬variant",
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
			wantErr: errors.Errorf("wrong service, " +
				"available services are: [bedrock sagemaker], " +
				"region cannot be empty",
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
				assert.Equal(t, tt.wantEndpoint, ic.Endpoint())
				assert.Equal(t, tt.wantTargetModel, ic.TargetModel())
				assert.Equal(t, tt.wantTargetVariant, ic.TargetVariant())
				if ic.Temperature() != nil {
					assert.Equal(t, tt.wantTemperature, *ic.Temperature())
				}
				assert.Equal(t, tt.wantStopSequences, ic.StopSequences())
				if ic.TopP() != nil {
					assert.Equal(t, tt.wantTopP, *ic.TopP())
				}
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
