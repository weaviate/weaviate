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

package ent

import (
	"encoding/json"
	"testing"

	"github.com/weaviate/weaviate/entities/moduletools"
)

func Test_classSettings_Validate(t *testing.T) {
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
				cfg: newConfigBuilder().
					addSetting("model", "model").addSetting("imageFields", nil).build(),
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
				cfg: newConfigBuilder().addSetting("imageFields", []interface{}{}).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with empty string in imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []interface{}{""}).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with int value in imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []interface{}{1.0}).build(),
			},
			wantErr: true,
		},
		{
			name: "should pass with proper value in imageFields",
			fields: fields{
				cfg: newConfigBuilder().addSetting("imageFields", []interface{}{"field"}).build(),
			},
		},
		{
			name: "should pass with proper value in imageFields and textFields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("imageFields", []interface{}{"imageField"}).
					addSetting("textFields", []interface{}{"textField"}).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1", "imageField2"}).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1", "imageField2"}).
					addWeights([]interface{}{1, 2}, []interface{}{1, 2}).
					build(),
			},
		},
		{
			name: "should pass with proper value in 1 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1, 2}, []interface{}{1}).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1, 2}, []interface{}{1}).
					build(),
			},
		},
		{
			name: "should not pass with proper value in 1 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1}, []interface{}{1}).
					build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with not proper weight value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1, "aaaa"}, []interface{}{1}).
					build(),
			},
			wantErr: true,
		},
		{
			name: "should pass with not proper weight value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{json.Number("1"), json.Number("2")}, []interface{}{json.Number("3")}).
					build(),
			},
		},
		{
			name: "should pass with proper dimensions setting in videoFields together with image fields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("videoFields", []interface{}{"video1"}).
					addSetting("imageFields", []interface{}{"image1"}).
					build(),
			},
			wantErr: false,
		},
		{
			name: "should not pass with wrong dimensions setting jina-clip-v2",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("model", "jina-clip-v2").
					addSetting("dimensions", int64(63)).
					addSetting("imageFields", []interface{}{"imageField"}).build(),
			},
			wantErr: true,
		},
		{
			name: "should not pass with wrong dimensions setting jina-clip-v2",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("model", "jina-clip-v1").
					addSetting("dimensions", int64(769)).
					addSetting("imageFields", []interface{}{"imageField"}).build(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := NewClassSettings(tt.fields.cfg)
			if err := ic.Validate(); (err != nil) != tt.wantErr {
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
		fakeClassConfig: &fakeClassConfig{config: map[string]interface{}{}},
	}
}

func (b *builder) addSetting(name string, value interface{}) *builder {
	b.fakeClassConfig.config[name] = value
	return b
}

func (b *builder) addWeights(textWeights, imageWeights []interface{}) *builder {
	if textWeights != nil || imageWeights != nil {
		weightSettings := map[string]interface{}{}
		if textWeights != nil {
			weightSettings["textFields"] = textWeights
		}
		if imageWeights != nil {
			weightSettings["imageFields"] = imageWeights
		}
		b.fakeClassConfig.config["weights"] = weightSettings
	}
	return b
}

func (b *builder) build() *fakeClassConfig {
	return b.fakeClassConfig
}

type fakeClassConfig struct {
	config map[string]interface{}
}

func (c fakeClassConfig) Class() map[string]interface{} {
	return c.config
}

func (c fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return c.config
}

func (c fakeClassConfig) Property(propName string) map[string]interface{} {
	return c.config
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}
