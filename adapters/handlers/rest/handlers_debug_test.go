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
