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

package ent

import (
    "testing"

    "github.com/pkg/errors"
    "github.com/stretchr/testify/assert"
    "github.com/weaviate/weaviate/entities/models"
    "github.com/weaviate/weaviate/entities/moduletools"
    "github.com/weaviate/weaviate/entities/schema"
)

func Test_classSettings_Validate(t *testing.T) {
    class := &models.Class{
        Class: "test",
        Properties: []*models.Property{{
            DataType: []string{schema.DataTypeText.String()},
            Name:     "test",
        }},
    }

    tests := []struct {
        name    string
        cfg     moduletools.ClassConfig
        wantErr error
    }{
        {
            name: "morph-embedding-v3",
            cfg: &fakeClassConfig{classConfig: map[string]interface{}{"model": "morph-embedding-v3"}},
        },
        {
            name: "morph-embedding-v3 wrong dimensions",
            cfg:  &fakeClassConfig{classConfig: map[string]interface{}{"model": "morph-embedding-v3", "dimensions": 1536}},
            wantErr: errors.New("wrong dimensions setting for morph-embedding-v3 model, available dimensions are: [1024]"),
        },
        {
            name: "wrong model name",
            cfg:  &fakeClassConfig{classConfig: map[string]interface{}{"model": "unknown-model"}},
            wantErr: errors.New("wrong Morph model name, available model names are: [morph-embedding-v2 morph-embedding-v3]"),
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

// keep property config basic behavior separate; Morph does not change these semantics

func TestValidateModelVersion(t *testing.T) {
    // Morph models do not use versions; always return no error
    err := (&classSettings{}).validateModelVersion("any", "morph-embedding-v3", "text")
    assert.Nil(t, err)
}

func Test_getModelString(t *testing.T) {
    cs := NewClassSettings(&fakeClassConfig{classConfig: map[string]interface{}{"type": "text", "model": "morph-embedding-v3"}})
    if got := cs.ModelStringForAction("document"); got != "morph-embedding-v3" {
        t.Errorf("got %v, want %v", got, "morph-embedding-v3")
    }
    if got := cs.ModelStringForAction("query"); got != "morph-embedding-v3" {
        t.Errorf("got %v, want %v", got, "morph-embedding-v3")
    }
}

func TestPickDefaultModelVersion(t *testing.T) {
    version := PickDefaultModelVersion("morph-embedding-v3", "text")
    assert.Equal(t, "", version)
}

func TestClassSettings(t *testing.T) {
	type testCase struct {
		expectedBaseURL       string
		expectedDimensions    int64
		cfg                   moduletools.ClassConfig
	}
	tests := []testCase{
		{
			cfg: fakeClassConfig{
				classConfig: make(map[string]interface{}),
			},
            expectedBaseURL:    DefaultBaseURL,
            expectedDimensions: 1024,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL": "https://proxy.weaviate.dev",
				},
			},
			expectedBaseURL:    "https://proxy.weaviate.dev",
            expectedDimensions: 1024,
		},
		{
			cfg: fakeClassConfig{
				classConfig: map[string]interface{}{
					"baseURL":    "https://proxy.weaviate.dev",
                    "dimensions": 1024,
				},
			},
			expectedBaseURL:    "https://proxy.weaviate.dev",
            expectedDimensions: 1024,
		},
	}

	for _, tt := range tests {
		ic := NewClassSettings(tt.cfg)
		assert.Equal(t, tt.expectedBaseURL, ic.BaseURL())
        assert.Equal(t, tt.expectedDimensions, *ic.Dimensions())
	}
}
