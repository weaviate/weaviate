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
		wantModel         string
		wantMaxTokens     int
		wantTemperature   float64
		wantK             int
		wantP             float64
		wantStopSequences []string
		wantBaseURL       string
		wantErr           error
	}{
		{
			name: "default settings",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
			wantModel:         "claude-3-5-sonnet-20240620",
			wantMaxTokens:     200000,
			wantTemperature:   1.0,
			wantK:             0,
			wantP:             0.0,
			wantStopSequences: []string{},
			wantBaseURL:       "https://api.anthropic.com/v1/messages",
			wantErr:           nil,
		},
		{
			name: "everything non default configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model":         "claude-3-opus-20240229",
					"maxTokens":     150000,
					"temperature":   0.7,
					"k":             5,
					"p":             0.9,
					"stopSequences": []string{"stop1", "stop2"},
					"baseURL":       "https://custom.anthropic.api",
				},
			},
			wantModel:         "claude-3-opus-20240229",
			wantMaxTokens:     150000,
			wantTemperature:   0.7,
			wantK:             5,
			wantP:             0.9,
			wantStopSequences: []string{"stop1", "stop2"},
			wantBaseURL:       "https://custom.anthropic.api",
			wantErr:           nil,
		},
		{
			name: "wrong model configured",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "wrong-model",
				},
			},
			wantErr: errors.Errorf("wrong Anthropic model name, available model names are: " +
				"[claude-3-5-sonnet-20240620 claude-3-opus-20240229 claude-3-sonnet-20240229 claude-3-haiku-20240307]"),
		},
		{
			name: "default settings with claude-3-haiku-20240307",
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "claude-3-haiku-20240307",
				},
			},
			wantModel:         "claude-3-haiku-20240307",
			wantMaxTokens:     200000,
			wantTemperature:   1.0,
			wantK:             0,
			wantP:             0.0,
			wantStopSequences: []string{},
			wantBaseURL:       "https://api.anthropic.com/v1/messages",
			wantErr:           nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.cfg)
			if tt.wantErr != nil {
				assert.Equal(t, tt.wantErr.Error(), ic.Validate(nil).Error())
			} else {
				assert.NoError(t, ic.Validate(nil))
				assert.Equal(t, tt.wantModel, ic.Model())
				assert.Equal(t, tt.wantMaxTokens, ic.MaxTokens())
				assert.Equal(t, tt.wantTemperature, ic.Temperature())
				assert.Equal(t, tt.wantK, ic.K())
				assert.Equal(t, tt.wantP, ic.P())
				assert.Equal(t, tt.wantStopSequences, ic.StopSequences())
				assert.Equal(t, tt.wantBaseURL, ic.BaseURL())
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
