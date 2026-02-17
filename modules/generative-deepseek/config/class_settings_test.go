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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestDeepSeekSettingsValidate(t *testing.T) {
	cases := map[string]struct {
		cfg     moduletools.ClassConfig
		model   string
		baseURL string
		err     string
	}{
		"Default Settings": {
			cfg:     dsTestCfg{p: map[string]any{}},
			model:   "deepseek-chat",
			baseURL: "https://api.deepseek.com",
		},
		"Custom Settings": {
			cfg: dsTestCfg{
				p: map[string]any{
					"model":       "deepseek-reasoner",
					"temperature": 0.5,
				},
			},
			model:   "deepseek-reasoner",
			baseURL: "https://api.deepseek.com",
		},
		"Error: Bad Temperature": {
			cfg: dsTestCfg{p: map[string]any{"temperature": 5.0}},
			err: "Wrong temperature configuration",
		},
		"Error: Bad MaxTokens": {
			cfg: dsTestCfg{p: map[string]any{"maxTokens": -5}},
			err: "Wrong maxTokens configuration",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewClassSettings(tc.cfg)
			if tc.err != "" {
				err := s.Validate(nil)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.err)
			} else {
				assert.NoError(t, s.Validate(nil))
				assert.Equal(t, tc.model, s.Model())
				assert.Equal(t, tc.baseURL, s.BaseURL())
			}
		})
	}
}

type dsTestCfg struct {
	p map[string]any
}

func (m dsTestCfg) Class() map[string]any                           { return m.p }
func (m dsTestCfg) Tenant() string                                  { return "" }
func (m dsTestCfg) ClassByModuleName(n string) map[string]any       { return m.p }
func (m dsTestCfg) Property(n string) map[string]any                { return nil }
func (m dsTestCfg) TargetVector() string                            { return "" }
func (m dsTestCfg) PropertiesDataTypes() map[string]schema.DataType { return nil }
func (m dsTestCfg) Config() *config.Config                          { return nil }
