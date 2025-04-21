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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/config/runtime"
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
		require.ErrorContains(t, err, "autoschema_enbaled") // should contain mispelled field
		assert.Nil(t, cfg)
	})
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

		UpdateRuntimeConfig(reg, parsed)

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

		UpdateRuntimeConfig(reg, parsed)

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

		UpdateRuntimeConfig(reg, parsed)

		// after update.
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())     // this should still return `default` value. not old value
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.

		colCount.SetDefault(20)             // changing the default.
		assert.Equal(t, 20, colCount.Get()) // this should still return `default` value
	})

	// should reflect changes on registered configs
	// should prioritize default and old value if new value is nil
}

// func TestRuntimeConfig(t *testing.T) {
// 	cm := &mockManager{c: &WeaviateRuntimeConfig{}}
// 	rm := NewWeaviateRuntimeConfig(cm)

// 	t.Run("setting explicitly value for auto schema enabled", func(t *testing.T) {
// 		b := true

// 		cm.c.AutoSchemaEnabled = &b
// 		val := rm.GetAutoSchemaEnabled()
// 		require.NotNil(t, val)
// 		require.Equal(t, true, *val)

// 		b = false
// 		cm.c.AutoSchemaEnabled = &b
// 		val = rm.GetAutoSchemaEnabled()
// 		require.NotNil(t, val)
// 		require.Equal(t, false, *val)
// 	})

// 	t.Run("auto schema not being set should return nil", func(t *testing.T) {
// 		cm.c.AutoSchemaEnabled = nil
// 		val := rm.GetAutoSchemaEnabled()
// 		require.Nil(t, val)
// 	})

// 	t.Run("auto schema not being set should return nil", func(t *testing.T) {
// 		cm.c.AutoSchemaEnabled = nil
// 		val := rm.GetAutoSchemaEnabled()
// 		require.Nil(t, val)
// 	})

// 	t.Run("maximum collection limit not being set should return nil", func(t *testing.T) {
// 		cm.c.MaximumAllowedCollectionsCount = nil
// 		val := rm.GetMaximumAllowedCollectionsCount()
// 		require.Nil(t, val)
// 	})

// 	t.Run("async replicsation disabled not being set should return nil", func(t *testing.T) {
// 		cm.c.AsyncReplicationDisabled = nil
// 		val := rm.GetAsyncReplicationDisabled()
// 		require.Nil(t, val)
// 	})
// }

// func TestParseYaml(t *testing.T) {
// 	t.Run("empty bytes shouldn't return error", func(t *testing.T) {
// 		b := []byte("")
// 		v, err := ParseYaml(b)
// 		require.NoError(t, err)
// 		require.NotNil(t, v)
// 	})
// 	t.Run("strict parsing should fail for non-existing field", func(t *testing.T) {
// 		val := `
// maximum_allowed_collections_count: 5
// `
// 		b := []byte(val)
// 		v, err := ParseYaml(b)
// 		require.NoError(t, err)
// 		require.NotNil(t, v)

// 		val = `
// maximum_allowed_collections_count: 5
// non_exist_filed: 78
// `
// 		b = []byte(val)
// 		_, err = ParseYaml(b)
// 		require.Error(t, err)
// 	})
// }
