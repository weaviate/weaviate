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

package settings

import (
	"encoding/json"
	"testing"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_BaseClassMultiModalSettings_ValidateMultiModal(t *testing.T) {
	type fields struct {
		cfg moduletools.ClassConfig
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "should not pass with empty config",
			wantErr: true,
		},
		{
			name: "should not pass with nil config",
			fields: fields{
				cfg: nil,
			},
			wantErr: true,
		},
		{
			name: "should not pass with nil imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", nil).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with fault imageFields value",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []string{}).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with empty imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []any{}).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with empty string in imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []any{""}).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with int value in imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []any{1.0}).build(),
			},
			wantErr: true,
		},
		{
			name: "should pass with proper value in imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []any{"field"}).build(),
			},
		},
		{
			name: "should pass with proper value in imageFields and textFields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("imageFields", []any{"imageField"}).
					addSetting("textFields", []any{"textField"}).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1", "imageField2"}).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1", "imageField2"}).
					addWeights([]any{1, 2}, []any{1, 2}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should pass with proper value in 1 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addWeights([]any{1, 2}, []any{1}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addWeights([]any{1, 2}, []any{1}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should not pass with proper value in 1 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addWeights([]any{1}, []any{1}, nil, nil, nil, nil, nil).
					build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with not proper weight value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addWeights([]any{1, "aaaa"}, []any{1}, nil, nil, nil, nil, nil).
					build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with not proper weight value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addWeights([]any{json.Number("1"), json.Number("2")}, []any{json.Number("3")}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should pass with proper values in all fields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addSetting("audioFields", []any{"audioField1"}).
					addSetting("videoFields", []any{"videoField1"}).
					addSetting("imuFields", []any{"imuField1"}).
					addSetting("thermalFields", []any{"thermalField1"}).
					addSetting("depthFields", []any{"depthField1", "depthField2"}).
					build(),
			},
		},
		{
			name: "should pass with proper values in all fields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []any{"textField1", "textField2"}).
					addSetting("imageFields", []any{"imageField1"}).
					addSetting("audioFields", []any{"audioField1"}).
					addSetting("videoFields", []any{"videoField1"}).
					addSetting("imuFields", []any{"imuField1"}).
					addSetting("thermalFields", []any{"thermalField1"}).
					addSetting("depthFields", []any{"depthField1", "depthField2"}).
					addWeights([]any{1, 2}, []any{1}, []any{1}, []any{1}, []any{1}, []any{1}, []any{1, 2}).
					build(),
			},
		},
		{
			name: "should pass with proper values audio, video, imu, thermal and depth fields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("audioFields", []any{"audioField1", "audioField2"}).
					addSetting("videoFields", []any{"videoField1"}).
					addSetting("imuFields", []any{"imuField1"}).
					addSetting("thermalFields", []any{"thermalField1"}).
					addSetting("depthFields", []any{"depthField1", "depthField2"}).
					addWeights(nil, nil, []any{1, 2}, []any{1}, []any{1}, []any{1}, []any{1, 2}).
					build(),
			},
		},
		{
			name: "should not pass with thermal and depth fields and not proper weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("thermalFields", []any{"thermalField1"}).
					addSetting("depthFields", []any{"depthField1", "depthField2"}).
					addWeights(nil, nil, nil, nil, nil, []any{1, 100}, []any{1, 2}).
					build(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := []string{ImageFieldsProperty, TextFieldsProperty, AudioFieldsProperty, VideoFieldsProperty, ImuFieldsProperty, ThermalFieldsProperty, DepthFieldsProperty}
			ic := NewBaseClassMultiModalSettingsWithAltNames(tt.fields.cfg, false, "moduleName", nil, nil)
			if err := ic.ValidateMultiModal(fields); (err != nil) != tt.wantErr {
				t.Errorf("classSettings.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type builder struct {
	fakeClassConfig *fakeClassConfig
}

func newConfigBuilder() *builder {
	return &builder{
		fakeClassConfig: &fakeClassConfig{config: map[string]any{}},
	}
}

func (b *builder) addSetting(name string, value any) *builder {
	b.fakeClassConfig.config[name] = value
	return b
}

func (b *builder) addWeights(textWeights, imageWeights, audioWeights,
	videoWeights, imuWeights, thermalWeights, depthWeights []any,
) *builder {
	weightSettings := map[string]any{}
	if textWeights != nil {
		weightSettings["textFields"] = textWeights
	}
	if imageWeights != nil {
		weightSettings["imageFields"] = imageWeights
	}
	if audioWeights != nil {
		weightSettings["audioFields"] = audioWeights
	}
	if videoWeights != nil {
		weightSettings["videoFields"] = videoWeights
	}
	if imuWeights != nil {
		weightSettings["imuFields"] = imuWeights
	}
	if thermalWeights != nil {
		weightSettings["thermalFields"] = thermalWeights
	}
	if depthWeights != nil {
		weightSettings["depthFields"] = depthWeights
	}
	if len(weightSettings) > 0 {
		b.fakeClassConfig.config["weights"] = weightSettings
	}
	return b
}

func (b *builder) build() *fakeClassConfig {
	return b.fakeClassConfig
}

type fakeClassConfig struct {
	config map[string]any
}

func (c fakeClassConfig) Class() map[string]any {
	return c.config
}

func (c fakeClassConfig) ClassByModuleName(moduleName string) map[string]any {
	return c.config
}

func (c fakeClassConfig) Property(propName string) map[string]any {
	return c.config
}

func (c fakeClassConfig) Tenant() string {
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
