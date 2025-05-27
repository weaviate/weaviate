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

package config

import (
	"regexp"
	"testing"

	"github.com/go-jose/go-jose/v4/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"gopkg.in/yaml.v3"
)

func TestParseRuntimeConfig(t *testing.T) {
	// parser should fail if any unknown fields exist in the file
	t.Run("parser should fail if any unknown fields exist in the file", func(t *testing.T) {
		// rationale: Catch and fail early if any typo on the config file.

		buf := []byte(`autoschema_enabled: true`)
		cfg, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)
		assert.Equal(t, true, cfg.AutoschemaEnabled.Get())

		buf = []byte(`autoschema_enbaled: false`) // note: typo.
		cfg, err = ParseRuntimeConfig(buf)
		require.ErrorContains(t, err, "autoschema_enbaled") // should contain misspelled field
		assert.Nil(t, cfg)
	})

	t.Run("YAML tag should be lower_snake_case", func(t *testing.T) {
		var r WeaviateRuntimeConfig

		jd, err := json.Marshal(r)
		require.NoError(t, err)

		var vv map[string]any
		require.NoError(t, json.Unmarshal(jd, &vv))

		for k := range vv {
			// check if all the keys lower_snake_case.
			assertConfigKey(t, k)
		}
	})

	t.Run("JSON tag should be lower_snake_case in the runtime config", func(t *testing.T) {
		var r WeaviateRuntimeConfig

		yd, err := yaml.Marshal(r)
		require.NoError(t, err)

		var vv map[string]any
		require.NoError(t, yaml.Unmarshal(yd, &vv))

		for k := range vv {
			// check if all the keys lower_snake_case.
			assertConfigKey(t, k)
		}
	})
}

// assertConfigKey asserts if the `yaml` key is standard `lower_snake_case` (e.g: not `UPPER_CASE`)
func assertConfigKey(t *testing.T, key string) {
	t.Helper()

	re := regexp.MustCompile(`^[a-z]+(_[a-z]+)*$`)
	if !re.MatchString(key) {
		t.Fatalf("given key %v is not lower snake case. The json/yaml tag for runtime config should be all lower snake case (e.g my_key, not MY_KEY)", key)
	}
}

func TestUpdateRuntimeConfig(t *testing.T) {
	t.Run("updating should reflect changes in registered configs", func(t *testing.T) {
		var (
			colCount   runtime.DynamicValue[int]
			autoSchema runtime.DynamicValue[bool]
			asyncRep   runtime.DynamicValue[bool]
		)

		reg := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount: &colCount,
			AutoschemaEnabled:              &autoSchema,
			AsyncReplicationDisabled:       &asyncRep,
		}

		// parsed from yaml configs for example
		buf := []byte(`autoschema_enabled: true
maximum_allowed_collections_count: 13`)
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update (zero values)
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())

		require.NoError(t, UpdateRuntimeConfig(reg, parsed))

		// after update (reflect from parsed values)
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
	})

	t.Run("updating priorities", func(t *testing.T) {
		// invariants:
		// 1. If field doesn't exist, should return default value
		// 2. If field exist, but removed next time, should return default value not the old value.

		var (
			colCount   runtime.DynamicValue[int]
			autoSchema runtime.DynamicValue[bool]
			asyncRep   runtime.DynamicValue[bool]
		)

		reg := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount: &colCount,
			AutoschemaEnabled:              &autoSchema,
			AsyncReplicationDisabled:       &asyncRep,
		}

		// parsed from yaml configs for example
		buf := []byte(`autoschema_enabled: true
maximum_allowed_collections_count: 13`)
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update (zero values)
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file.

		require.NoError(t, UpdateRuntimeConfig(reg, parsed))

		// after update (reflect from parsed values)
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.

		// removing `maximum_allowed_collection_count` from config
		buf = []byte(`autoschema_enabled: false`)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update. Should have old values
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.

		require.NoError(t, UpdateRuntimeConfig(reg, parsed))

		// after update.
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())     // this should still return `default` value. not old value
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.
	})
}
