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

package rest

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authorization/adminlist"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/cluster"
	ucfg "github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestSkipSensitiveConfig(t *testing.T) {
	tests := []struct {
		name                 string
		input                ucfg.Config
		expectAuthZero       bool
		expectAuthzZero      bool
		expectBasicAuthEmpty bool
		expectIntact         func(*testing.T, ucfg.Config)
	}{
		{
			name: "authentication with API keys is skipped",
			input: ucfg.Config{
				Authentication: ucfg.Authentication{
					APIKey: ucfg.StaticAPIKey{
						Enabled:     true,
						AllowedKeys: []string{"secret-key"},
					},
				},
				Persistence: ucfg.Persistence{DataPath: "/test/data"},
			},
			expectAuthZero:       true,
			expectAuthzZero:      true,
			expectBasicAuthEmpty: true,
			expectIntact: func(t *testing.T, result ucfg.Config) {
				assert.Equal(t, "/test/data", result.Persistence.DataPath)
			},
		},
		{
			name: "authorization with users is skipped",
			input: ucfg.Config{
				Authorization: ucfg.Authorization{
					AdminList: adminlist.Config{
						Enabled: true,
						Users:   []string{"admin"},
					},
				},
				Persistence: ucfg.Persistence{DataPath: "/test/data"},
			},
			expectAuthZero:       true,
			expectAuthzZero:      true,
			expectBasicAuthEmpty: true,
			expectIntact: func(t *testing.T, result ucfg.Config) {
				assert.Equal(t, "/test/data", result.Persistence.DataPath)
			},
		},
		{
			name: "cluster basic auth credentials are cleared",
			input: ucfg.Config{
				Cluster: cluster.Config{
					AuthConfig: cluster.AuthConfig{
						BasicAuth: cluster.BasicAuth{
							Username: "admin",
							Password: "secret",
						},
					},
					Hostname: "test-host",
				},
				Persistence: ucfg.Persistence{DataPath: "/test/data"},
			},
			expectAuthZero:       true,
			expectAuthzZero:      true,
			expectBasicAuthEmpty: true,
			expectIntact: func(t *testing.T, result ucfg.Config) {
				assert.Equal(t, "test-host", result.Cluster.Hostname)
				assert.Equal(t, "/test/data", result.Persistence.DataPath)
			},
		},
		{
			name: "all sensitive sections are skipped",
			input: ucfg.Config{
				Authentication: ucfg.Authentication{
					APIKey: ucfg.StaticAPIKey{
						Enabled:     true,
						AllowedKeys: []string{"secret-key"},
					},
				},
				Authorization: ucfg.Authorization{
					Rbac: rbacconf.Config{
						Enabled:   true,
						RootUsers: []string{"root"},
					},
				},
				Cluster: cluster.Config{
					AuthConfig: cluster.AuthConfig{
						BasicAuth: cluster.BasicAuth{
							Username: "user",
							Password: "pass",
						},
					},
				},
				Persistence:   ucfg.Persistence{DataPath: "/test/data"},
				QueryDefaults: ucfg.QueryDefaults{Limit: 100},
			},
			expectAuthZero:       true,
			expectAuthzZero:      true,
			expectBasicAuthEmpty: true,
			expectIntact: func(t *testing.T, result ucfg.Config) {
				assert.Equal(t, "/test/data", result.Persistence.DataPath)
				assert.Equal(t, int64(100), result.QueryDefaults.Limit)
			},
		},
		{
			name: "non-sensitive config remains intact",
			input: ucfg.Config{
				Persistence:             ucfg.Persistence{DataPath: "/test/data"},
				QueryDefaults:           ucfg.QueryDefaults{Limit: 10},
				DefaultVectorizerModule: "text2vec-openai",
			},
			expectAuthZero:       true,
			expectAuthzZero:      true,
			expectBasicAuthEmpty: true,
			expectIntact: func(t *testing.T, result ucfg.Config) {
				assert.Equal(t, "/test/data", result.Persistence.DataPath)
				assert.Equal(t, int64(10), result.QueryDefaults.Limit)
				assert.Equal(t, "text2vec-openai", result.DefaultVectorizerModule)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input

			if tt.expectAuthZero {
				assert.Equal(t, ucfg.Authentication{}, result.Authentication)
			}
			if tt.expectAuthzZero {
				assert.Equal(t, ucfg.Authorization{}, result.Authorization)
			}
			if tt.expectBasicAuthEmpty {
				assert.Equal(t, "", result.Cluster.AuthConfig.BasicAuth.Username)
				assert.Equal(t, "", result.Cluster.AuthConfig.BasicAuth.Password)
			}
			if tt.expectIntact != nil {
				tt.expectIntact(t, result)
			}
		})
	}
}

func TestDebugDumpConfig_RuntimeDynamicValues(t *testing.T) {
	// Test that runtime DynamicValue updates are reflected in the marshaled output
	cfg := ucfg.Config{
		// Set default value for a DynamicValue field
		QuerySlowLogEnabled: configRuntime.NewDynamicValue(false), // default: false
		Persistence:         ucfg.Persistence{DataPath: "/test/data"},
	}

	jsonBytesBefore, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	jsonStrBefore := string(jsonBytesBefore)

	assert.Contains(t, jsonStrBefore, `"query_slow_log_enabled": false`, "should show default value false before runtime update")

	cfg.QuerySlowLogEnabled.SetValue(true) // runtime: true

	// Second call: Marshal after runtime update - should show updated runtime value
	jsonBytesAfter, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	jsonStrAfter := string(jsonBytesAfter)

	assert.Contains(t, jsonStrAfter, `"query_slow_log_enabled": true`, "should reflect runtime value true after update")
	assert.NotContains(t, jsonStrAfter, `"query_slow_log_enabled": false`, "should not contain default value false after update")
}
