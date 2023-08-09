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

package vectorizer

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
					addWeights([]interface{}{1, 2}, []interface{}{1, 2}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should pass with proper value in 1 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1, 2}, []interface{}{1}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should pass with proper value in 2 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1, 2}, []interface{}{1}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should not pass with proper value in 1 imageFields and 2 textFields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addWeights([]interface{}{1}, []interface{}{1}, nil, nil, nil, nil, nil).
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
					addWeights([]interface{}{1, "aaaa"}, []interface{}{1}, nil, nil, nil, nil, nil).
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
					addWeights([]interface{}{json.Number("1"), json.Number("2")}, []interface{}{json.Number("3")}, nil, nil, nil, nil, nil).
					build(),
			},
		},
		{
			name: "should pass with proper values in all fields",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addSetting("audioFields", []interface{}{"audioField1"}).
					addSetting("videoFields", []interface{}{"videoField1"}).
					addSetting("imuFields", []interface{}{"imuField1"}).
					addSetting("thermalFields", []interface{}{"thermalField1"}).
					addSetting("depthFields", []interface{}{"depthField1", "depthField2"}).
					build(),
			},
		},
		{
			name: "should pass with proper values in all fields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("textFields", []interface{}{"textField1", "textField2"}).
					addSetting("imageFields", []interface{}{"imageField1"}).
					addSetting("audioFields", []interface{}{"audioField1"}).
					addSetting("videoFields", []interface{}{"videoField1"}).
					addSetting("imuFields", []interface{}{"imuField1"}).
					addSetting("thermalFields", []interface{}{"thermalField1"}).
					addSetting("depthFields", []interface{}{"depthField1", "depthField2"}).
					addWeights([]interface{}{1, 2}, []interface{}{1}, []interface{}{1}, []interface{}{1}, []interface{}{1}, []interface{}{1}, []interface{}{1, 2}).
					build(),
			},
		},
		{
			name: "should pass with proper values audio, video, imu, thermal and depth fields and weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("audioFields", []interface{}{"audioField1", "audioField2"}).
					addSetting("videoFields", []interface{}{"videoField1"}).
					addSetting("imuFields", []interface{}{"imuField1"}).
					addSetting("thermalFields", []interface{}{"thermalField1"}).
					addSetting("depthFields", []interface{}{"depthField1", "depthField2"}).
					addWeights(nil, nil, []interface{}{1, 2}, []interface{}{1}, []interface{}{1}, []interface{}{1}, []interface{}{1, 2}).
					build(),
			},
		},
		{
			name: "should not pass with thermal and depth fields and not proper weights",
			fields: fields{
				cfg: newConfigBuilder().
					addSetting("thermalFields", []interface{}{"thermalField1"}).
					addSetting("depthFields", []interface{}{"depthField1", "depthField2"}).
					addWeights(nil, nil, nil, nil, nil, []interface{}{1, 100}, []interface{}{1, 2}).
					build(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := &classSettings{
				cfg: tt.fields.cfg,
			}
			if err := ic.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("classSettings.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
