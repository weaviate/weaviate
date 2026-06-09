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

	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/cluster"
	ucfg "github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

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

func TestRedactDebugConfigSecrets(t *testing.T) {
	t.Run("redacts api keys, cluster password, and sentry dsn", func(t *testing.T) {
		m := map[string]any{
			"authentication": map[string]any{
				"APIKey": map[string]any{
					"enabled":      true,
					"users":        []any{"root-user", "regular-user"},
					"allowed_keys": []any{"root-secret", "user-secret"},
				},
			},
			"cluster": map[string]any{
				"auth": map[string]any{
					"basic": map[string]any{
						"username": "cluster-user",
						"password": "cluster-secret",
					},
				},
			},
			"sentry": map[string]any{
				"enabled":       true,
				"dsn":           "https://abc123@sentry.io/12345",
				"cluster_id":    "node-0",
				"cluster_owner": "team-storage",
			},
		}

		redactDebugConfigSecrets(m)

		// API keys redacted (each entry replaced; count preserved)
		apiKey := m["authentication"].(map[string]any)["APIKey"].(map[string]any)
		assert.Equal(t, []any{"<redacted>", "<redacted>"}, apiKey["allowed_keys"])
		// Sibling fields untouched
		assert.Equal(t, true, apiKey["enabled"])
		assert.Equal(t, []any{"root-user", "regular-user"}, apiKey["users"])

		// Cluster password redacted, username untouched
		basic := m["cluster"].(map[string]any)["auth"].(map[string]any)["basic"].(map[string]any)
		assert.Equal(t, "<redacted>", basic["password"])
		assert.Equal(t, "cluster-user", basic["username"])

		// Sentry DSN redacted, sibling identifiers untouched
		sentry := m["sentry"].(map[string]any)
		assert.Equal(t, "<redacted>", sentry["dsn"])
		assert.Equal(t, "node-0", sentry["cluster_id"])
		assert.Equal(t, "team-storage", sentry["cluster_owner"])
	})

	t.Run("no-op when sensitive fields are absent", func(t *testing.T) {
		m := map[string]any{
			"authentication": map[string]any{
				"APIKey": map[string]any{"enabled": false},
			},
		}
		redactDebugConfigSecrets(m)
		// Did not introduce a key that wasn't there
		_, ok := m["authentication"].(map[string]any)["APIKey"].(map[string]any)["allowed_keys"]
		assert.False(t, ok)
	})
}

// Runs a populated config.Config through the exact /debug/config pipeline. The
// hand-built-map subtests above can't catch a json tag drifting away from a
// redactPath path (e.g. APIKey gaining a json tag) — this fails when that
// happens, instead of silently re-exposing the secret in production.
func TestRedactDebugConfigSecrets_RealConfigPipeline(t *testing.T) {
	const (
		apiKey1     = "secret-api-key-1"
		apiKey2     = "secret-api-key-2"
		clusterPass = "secret-cluster-password"
		sentryDSN   = "https://publickey@o0.ingest.sentry.io/12345"
	)

	cfg := ucfg.Config{
		Authentication: ucfg.Authentication{
			APIKey: ucfg.StaticAPIKey{
				Enabled:     true,
				Users:       []string{"root-user"},
				AllowedKeys: []string{apiKey1, apiKey2},
			},
		},
		Cluster: cluster.Config{
			AuthConfig: cluster.AuthConfig{
				BasicAuth: cluster.BasicAuth{
					Username: "cluster-user",
					Password: clusterPass,
				},
			},
		},
		Sentry: &entsentry.ConfigOpts{Enabled: true, DSN: sentryDSN},
	}

	// Mirror the /debug/config handler exactly.
	jsonBytes, err := json.Marshal(cfg)
	require.NoError(t, err)
	var configMap map[string]any
	require.NoError(t, json.Unmarshal(jsonBytes, &configMap))
	cleaned := cleanEmptyValues(configMap)
	redactDebugConfigSecrets(cleaned)

	// No secret value may survive to the wire. (MarshalIndent HTML-escapes, but
	// none of these secrets contain <, > or &, so a leak would appear verbatim.)
	out, err := json.MarshalIndent(cleaned, "", "  ")
	require.NoError(t, err)
	outStr := string(out)
	for _, secret := range []string{apiKey1, apiKey2, clusterPass, sentryDSN} {
		assert.NotContainsf(t, outStr, secret,
			"secret leaked into /debug/config — a json-tag drift likely broke a redactPath path")
	}

	// Each secret is gone because it was redacted, not because the section was
	// dropped: assert redaction fired AND the non-secret siblings still survive.
	authn, _ := cleaned["authentication"].(map[string]any)
	apiKey, _ := authn["APIKey"].(map[string]any)
	assert.Equal(t, []any{"<redacted>", "<redacted>"}, apiKey["allowed_keys"])
	assert.Equal(t, []any{"root-user"}, apiKey["users"])

	clusterM, _ := cleaned["cluster"].(map[string]any)
	authM, _ := clusterM["auth"].(map[string]any)
	basicM, _ := authM["basic"].(map[string]any)
	assert.Equal(t, "<redacted>", basicM["password"])
	assert.Equal(t, "cluster-user", basicM["username"])

	sentryM, _ := cleaned["sentry"].(map[string]any)
	assert.Equal(t, "<redacted>", sentryM["dsn"])
}
