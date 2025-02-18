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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_classSettings_Validate(t *testing.T) {
	class := &models.Class{
		Class: "test",
		Properties: []*models.Property{
			{
				DataType: []string{schema.DataTypeText.String()},
				Name:     "test",
			},
		},
	}
	tests := []struct {
		name    string
		cfg     moduletools.ClassConfig
		wantErr error
	}{
		{
			name: "All defaults",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{},
			},
		},
		{
			name: "Explicit correct model",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "Snowflake/snowflake-arctic-embed-m-v1.5",
				},
			},
		},
		{
			name: "Explicit wrong model",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"model": "not-available-model",
				},
			},
			wantErr: errors.New("wrong model name, available model names are: [Snowflake/snowflake-arctic-embed-l-v2.0 Snowflake/snowflake-arctic-embed-m-v1.5]"),
		},
		{
			name: "Explicit correct dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"dimensions": 256,
				},
			},
		},
		{
			name: "Explicit wrong dimensions",
			cfg: &fakeClassConfig{
				classConfig: map[string]interface{}{
					"dimensions": 768,
				},
			},
			wantErr: errors.New("wrong dimensions setting for Snowflake/snowflake-arctic-embed-l-v2.0 model, available dimensions are: [256 1024]"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs := NewClassSettings(tt.cfg)
			err := cs.Validate(class)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	if propName == f.skippedProperty {
		return map[string]interface{}{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]interface{}{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]interface{}{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}
