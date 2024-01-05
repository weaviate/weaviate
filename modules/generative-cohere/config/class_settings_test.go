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
		name                  string
		cfg                   moduletools.ClassConfig
		wantModel             string
		wantMaxTokens         int
		wantTemperature       int
		wantK                 int
		wantStopSequences     []string
		wantReturnLikelihoods string
		wantBaseURL           string
		wantErr               error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:             "command-nightly",
			wantMaxTokens:         2048,
			wantTemperature:       0,
			wantK:                 0,
			wantStopSequences:     []string{},
			wantReturnLikelihoods: "NONE",
			wantBaseURL:           "https://api.cohere.ai",
			wantErr:               nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":             "command-xlarge",
					"maxTokens":         2048,
					"temperature":       1,
					"k":                 2,
					"stopSequences":     []string{"stop1", "stop2"},
					"returnLikelihoods": "NONE",
				},
			},
			wantModel:             "command-xlarge",
			wantMaxTokens:         2048,
			wantTemperature:       1,
			wantK:                 2,
			wantStopSequences:     []string{"stop1", "stop2"},
			wantReturnLikelihoods: "NONE",
			wantBaseURL:           "https://api.cohere.ai",
			wantErr:               nil,
		},
		{
			name: "wrong model configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong Cohere model name, available model names are: " +
				"[command-xlarge-beta command-xlarge command-medium command-xlarge-nightly " +
				"command-medium-nightly xlarge medium command command-light command-nightly command-light-nightly base base-light]"),
		},
		{
			name: "default settings with command-light-nightly",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "command-light-nightly",
				},
			},
			wantModel:             "command-light-nightly",
			wantMaxTokens:         2048,
			wantTemperature:       0,
			wantK:                 0,
			wantStopSequences:     []string{},
			wantReturnLikelihoods: "NONE",
			wantBaseURL:           "https://api.cohere.ai",
			wantErr:               nil,
		},
		{
			name: "default settings with command-light-nightly and baseURL",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":   "command-light-nightly",
					"baseURL": "http://custom-url.com",
				},
			},
			wantModel:             "command-light-nightly",
			wantMaxTokens:         2048,
			wantTemperature:       0,
			wantK:                 0,
			wantStopSequences:     []string{},
			wantReturnLikelihoods: "NONE",
			wantBaseURL:           "http://custom-url.com",
			wantErr:               nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr.Error(), ic.Validate(nil).Error())
			} else {
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantK, ic.K())
				assert.Equal(t, tt.wantStopSequences, ic.StopSequences())
				assert.Equal(t, tt.wantReturnLikelihoods, ic.ReturnLikelihoods())
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
