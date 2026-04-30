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
