//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package ent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

// The module sends the Hugging Face API key and the text to vectorize to endpointURL, so it must be a Hugging Face
// host or listed by the operator.
func TestClassSettingsValidate_EndpointURL(t *testing.T) {
	class := &models.Class{Class: "Test", Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}}}
	tests := []struct {
		name        string
		classConfig map[string]any
		allowlist   string
		wantErr     bool
	}{
		{name: "unset, so the default URL", classConfig: map[string]any{}},
		{name: "inference endpoint", classConfig: map[string]any{"endpointURL": "https://abc123.us-east-1.aws.endpoints.huggingface.cloud"}},
		{name: "inference endpoint with a path", classConfig: map[string]any{"endpointURL": "https://abc123.us-east-1.aws.endpoints.huggingface.cloud/embed"}},
		{name: "router", classConfig: map[string]any{"endpointURL": "https://router.huggingface.co/hf-inference/models/m/pipeline/feature-extraction"}},
		{name: "huggingface.co", classConfig: map[string]any{"endpointURL": "https://huggingface.co/x"}},
		{name: "upper case host", classConfig: map[string]any{"endpointURL": "HTTPS://ROUTER.HUGGINGFACE.CO/x"}},
		{name: "legacy endpointUrl spelling", classConfig: map[string]any{"endpointUrl": "https://abc.endpoints.huggingface.cloud"}},

		{name: "foreign host", classConfig: map[string]any{"endpointURL": "https://attacker.example/e"}, wantErr: true},
		{name: "foreign host, legacy endpointUrl spelling", classConfig: map[string]any{"endpointUrl": "https://attacker.example/e"}, wantErr: true},
		{name: "plaintext inference endpoint", classConfig: map[string]any{"endpointURL": "http://abc123.endpoints.huggingface.cloud"}, wantErr: true},
		{name: "huggingface.co as a prefix of another domain", classConfig: map[string]any{"endpointURL": "https://huggingface.co.attacker.example"}, wantErr: true},
		{name: "huggingface.co as a suffix of another label", classConfig: map[string]any{"endpointURL": "https://attackerhuggingface.co"}, wantErr: true},
		{name: "huggingface.cloud outside endpoints", classConfig: map[string]any{"endpointURL": "https://x.huggingface.cloud"}, wantErr: true},
		{name: "huggingface host as userinfo", classConfig: map[string]any{"endpointURL": "https://router.huggingface.co@attacker.example/e"}, wantErr: true},
		{name: "backslash before a huggingface host", classConfig: map[string]any{"endpointURL": `https://attacker.example\@router.huggingface.co`}, wantErr: true},
		{name: "huggingface host in the path", classConfig: map[string]any{"endpointURL": "https://attacker.example/router.huggingface.co"}, wantErr: true},
		{name: "huggingface host in the fragment", classConfig: map[string]any{"endpointURL": "https://attacker.example#.huggingface.co"}, wantErr: true},
		{name: "other huggingface.co subdomain", classConfig: map[string]any{"endpointURL": "https://x.huggingface.co/e"}, wantErr: true},
		{name: "port on a huggingface host", classConfig: map[string]any{"endpointURL": "https://router.huggingface.co:8443/e"}, wantErr: true},
		{name: "trailing dot on a huggingface host", classConfig: map[string]any{"endpointURL": "https://router.huggingface.co./e"}, wantErr: true},
		{name: "sub-delimiter in an inference endpoint host", classConfig: map[string]any{"endpointURL": "https://attacker.example;.endpoints.huggingface.cloud/e"}, wantErr: true},
		{name: "bare endpoints.huggingface.cloud", classConfig: map[string]any{"endpointURL": "https://endpoints.huggingface.cloud/e"}, wantErr: true},
		{name: "no scheme", classConfig: map[string]any{"endpointURL": "router.huggingface.co"}, wantErr: true},
		{name: "opaque", classConfig: map[string]any{"endpointURL": "https:router.huggingface.co"}, wantErr: true},
		{name: "loopback", classConfig: map[string]any{"endpointURL": "http://127.0.0.1:8080"}, wantErr: true},

		{name: "listed self-hosted server", classConfig: map[string]any{"endpointURL": "http://tei:8080/embed"}, allowlist: "http://tei:8080"},
		{name: "listed host on another port", classConfig: map[string]any{"endpointURL": "http://tei:9090"}, allowlist: "http://tei:8080", wantErr: true},
		{name: "listed host with another scheme", classConfig: map[string]any{"endpointURL": "https://tei:8080"}, allowlist: "http://tei:8080", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(modulecomponents.AllowedBaseURLsEnv, tt.allowlist)
			err := NewClassSettings(fakeClassConfig(tt.classConfig)).Validate(class)
			if tt.wantErr {
				assert.ErrorContains(t, err, "endpoint")
				return
			}
			assert.NoError(t, err)
		})
	}
}

type fakeClassConfig map[string]any

func (f fakeClassConfig) Class() map[string]any                           { return f }
func (f fakeClassConfig) ClassByModuleName(string) map[string]any         { return f }
func (f fakeClassConfig) Property(string) map[string]any                  { return nil }
func (f fakeClassConfig) Tenant() string                                  { return "" }
func (f fakeClassConfig) TargetVector() string                            { return "" }
func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType { return nil }
func (f fakeClassConfig) Config() *config.Config                          { return nil }
