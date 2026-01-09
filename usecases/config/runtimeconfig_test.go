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

package config

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4/json"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/weaviate/weaviate/entities/cron"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestParseRuntimeConfig(t *testing.T) {
	// parser should not fail if any unknown fields exist in the file
	t.Run("parser should not fail if any unknown fields exist in the file", func(t *testing.T) {
		// rationale: in case of downgrade, the config file might contain
		// fields that are not known to the current version. We should ignore
		// them, not fail.

		// note: typo and unknown field should be ignored and known fields should be parsed correctly
		buf := []byte(`unknown_field: true
autoschema_enbaled: true
maximum_allowed_collections_count: 13
`)
		cfg, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)
		// typo should be ignored, default value should be returned
		assert.Equal(t, false, cfg.AutoschemaEnabled.Get())
		// valid field should be parsed correctly
		assert.Equal(t, 13, cfg.MaximumAllowedCollectionsCount.Get())
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

func TestUpdateRuntimeConfig(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	t.Run("updating should reflect changes in registered configs", func(t *testing.T) {
		var (
			colCount                 runtime.DynamicValue[int]
			autoSchema               runtime.DynamicValue[bool]
			asyncRep                 runtime.DynamicValue[bool]
			readLogLevel             runtime.DynamicValue[string]
			writeLogLevel            runtime.DynamicValue[string]
			revectorizeCheckDisabled runtime.DynamicValue[bool]
			minFinWait               runtime.DynamicValue[time.Duration]
			raftDrainSleep           runtime.DynamicValue[time.Duration]
			raftTimeoutsMultiplier   runtime.DynamicValue[int]
		)

		reg := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount:  &colCount,
			AutoschemaEnabled:               &autoSchema,
			AsyncReplicationDisabled:        &asyncRep,
			TenantActivityReadLogLevel:      &readLogLevel,
			TenantActivityWriteLogLevel:     &writeLogLevel,
			RevectorizeCheckDisabled:        &revectorizeCheckDisabled,
			ReplicaMovementMinimumAsyncWait: &minFinWait,
			RaftDrainSleep:                  &raftDrainSleep,
			RaftTimoutsMultiplier:           &raftTimeoutsMultiplier,
		}

		// parsed from yaml configs for example
		buf := []byte(`autoschema_enabled: true
maximum_allowed_collections_count: 13
replica_movement_minimum_async_wait: 10s`)
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update (zero values)
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())
		assert.Equal(t, 0*time.Second, minFinWait.Get())

		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))

		// after update (reflect from parsed values)
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
		assert.Equal(t, 10*time.Second, minFinWait.Get())
	})

	t.Run("Add and remove workflow", func(t *testing.T) {
		// 1. We start with empty overrides and see it doesn't change the .Get() value of source configs.
		// 2. We add some overrides. Check .Get() value
		// 3. Remove the overrides. check .Get() value goes back to default

		source := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount: runtime.NewDynamicValue(10),
			AutoschemaEnabled:              runtime.NewDynamicValue(true),
			AsyncReplicationDisabled:       runtime.NewDynamicValue(true),
			TenantActivityReadLogLevel:     runtime.NewDynamicValue("INFO"),
			TenantActivityWriteLogLevel:    runtime.NewDynamicValue("INFO"),
			RevectorizeCheckDisabled:       runtime.NewDynamicValue(true),
		}

		assert.Equal(t, 10, source.MaximumAllowedCollectionsCount.Get())
		assert.Equal(t, true, source.AutoschemaEnabled.Get())
		assert.Equal(t, true, source.AsyncReplicationDisabled.Get())
		assert.Equal(t, "INFO", source.TenantActivityReadLogLevel.Get())
		assert.Equal(t, "INFO", source.TenantActivityWriteLogLevel.Get())
		assert.Equal(t, true, source.RevectorizeCheckDisabled.Get())

		// Empty Parsing
		buf := []byte("")
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		assert.Nil(t, parsed.AsyncReplicationDisabled)
		assert.Nil(t, parsed.MaximumAllowedCollectionsCount)
		assert.Nil(t, parsed.AutoschemaEnabled)
		assert.Nil(t, parsed.TenantActivityReadLogLevel)
		assert.Nil(t, parsed.TenantActivityWriteLogLevel)
		assert.Nil(t, parsed.RevectorizeCheckDisabled)

		require.NoError(t, UpdateRuntimeConfig(log, source, parsed, nil))
		assert.Equal(t, 10, source.MaximumAllowedCollectionsCount.Get())
		assert.Equal(t, true, source.AutoschemaEnabled.Get())
		assert.Equal(t, true, source.AsyncReplicationDisabled.Get())
		assert.Equal(t, "INFO", source.TenantActivityReadLogLevel.Get())
		assert.Equal(t, "INFO", source.TenantActivityWriteLogLevel.Get())
		assert.Equal(t, true, source.RevectorizeCheckDisabled.Get())

		// Non-empty parsing
		buf = []byte(`autoschema_enabled: false
maximum_allowed_collections_count: 13`) // leaving out `asyncRep` config
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)

		require.NoError(t, UpdateRuntimeConfig(log, source, parsed, nil))
		assert.Equal(t, 13, source.MaximumAllowedCollectionsCount.Get()) // changed
		assert.Equal(t, false, source.AutoschemaEnabled.Get())           // changed
		assert.Equal(t, true, source.AsyncReplicationDisabled.Get())
		assert.Equal(t, "INFO", source.TenantActivityReadLogLevel.Get())
		assert.Equal(t, "INFO", source.TenantActivityWriteLogLevel.Get())
		assert.Equal(t, true, source.RevectorizeCheckDisabled.Get())

		// Empty parsing again. Should go back to default values
		buf = []byte("")
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)

		require.NoError(t, UpdateRuntimeConfig(log, source, parsed, nil))
		assert.Equal(t, 10, source.MaximumAllowedCollectionsCount.Get())
		assert.Equal(t, true, source.AutoschemaEnabled.Get())
		assert.Equal(t, true, source.AsyncReplicationDisabled.Get())
		assert.Equal(t, "INFO", source.TenantActivityReadLogLevel.Get())
		assert.Equal(t, "INFO", source.TenantActivityWriteLogLevel.Get())
		assert.Equal(t, true, source.RevectorizeCheckDisabled.Get())
	})

	t.Run("Reset() of non-exist config values in parsed yaml shouldn't panic", func(t *testing.T) {
		var (
			colCount   runtime.DynamicValue[int]
			autoSchema runtime.DynamicValue[bool]
			// leaving out `asyncRep` config
		)

		reg := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount: &colCount,
			AutoschemaEnabled:              &autoSchema,
			// leaving out `asyncRep` config
		}

		// parsed from yaml configs for example
		buf := []byte(`autoschema_enabled: true
maximum_allowed_collections_count: 13`) // leaving out `asyncRep` config
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update (zero values)
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())

		require.NotPanics(t, func() { UpdateRuntimeConfig(log, reg, parsed, nil) })

		// after update (reflect from parsed values)
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
	})

	t.Run("updating config should split out corresponding log lines", func(t *testing.T) {
		log := logrus.New()
		logs := bytes.Buffer{}
		log.SetOutput(&logs)

		var (
			colCount   = runtime.NewDynamicValue(7)
			autoSchema runtime.DynamicValue[bool]
		)

		reg := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount: colCount,
			AutoschemaEnabled:              &autoSchema,
		}

		// parsed from yaml configs for example
		buf := []byte(`autoschema_enabled: true
maximum_allowed_collections_count: 13`) // leaving out `asyncRep` config
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update (zero values)
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 7, colCount.Get())

		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Contains(t, logs.String(), `level=info msg="runtime overrides: config 'MaximumAllowedCollectionsCount' changed from '7' to '13'" action=runtime_overrides_changed field=MaximumAllowedCollectionsCount new_value=13 old_value=7`)
		assert.Contains(t, logs.String(), `level=info msg="runtime overrides: config 'AutoschemaEnabled' changed from 'false' to 'true'" action=runtime_overrides_changed field=AutoschemaEnabled new_value=true old_value=false`)
		logs.Reset()

		// change configs
		buf = []byte(`autoschema_enabled: false
maximum_allowed_collections_count: 10`)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)

		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Contains(t, logs.String(), `level=info msg="runtime overrides: config 'MaximumAllowedCollectionsCount' changed from '13' to '10'" action=runtime_overrides_changed field=MaximumAllowedCollectionsCount new_value=10 old_value=13`)
		assert.Contains(t, logs.String(), `level=info msg="runtime overrides: config 'AutoschemaEnabled' changed from 'true' to 'false'" action=runtime_overrides_changed field=AutoschemaEnabled new_value=false old_value=true`)
		logs.Reset()

		// remove configs (`maximum_allowed_collections_count`)
		buf = []byte(`autoschema_enabled: false`)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)

		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Contains(t, logs.String(), `level=info msg="runtime overrides: config 'MaximumAllowedCollectionsCount' changed from '10' to '7'" action=runtime_overrides_changed field=MaximumAllowedCollectionsCount new_value=7 old_value=10`)
	})

	t.Run("updating priorities", func(t *testing.T) {
		// invariants:
		// 1. If field doesn't exist, should return default value
		// 2. If field exist, but removed next time, should return default value not the old value.

		var (
			colCount                 runtime.DynamicValue[int]
			autoSchema               runtime.DynamicValue[bool]
			asyncRep                 runtime.DynamicValue[bool]
			readLogLevel             runtime.DynamicValue[string]
			writeLogLevel            runtime.DynamicValue[string]
			revectorizeCheckDisabled runtime.DynamicValue[bool]
			minFinWait               runtime.DynamicValue[time.Duration]
		)

		reg := &WeaviateRuntimeConfig{
			MaximumAllowedCollectionsCount:  &colCount,
			AutoschemaEnabled:               &autoSchema,
			AsyncReplicationDisabled:        &asyncRep,
			TenantActivityReadLogLevel:      &readLogLevel,
			TenantActivityWriteLogLevel:     &writeLogLevel,
			RevectorizeCheckDisabled:        &revectorizeCheckDisabled,
			ReplicaMovementMinimumAsyncWait: &minFinWait,
		}

		// parsed from yaml configs for example
		buf := []byte(`autoschema_enabled: true
maximum_allowed_collections_count: 13
replica_movement_minimum_async_wait: 10s`)
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update (zero values)
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file.
		assert.Equal(t, 0*time.Second, minFinWait.Get())

		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))

		// after update (reflect from parsed values)
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.
		assert.Equal(t, 10*time.Second, minFinWait.Get())

		// removing `maximum_allowed_collection_count` from config
		buf = []byte(`autoschema_enabled: false`)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)

		// before update. Should have old values
		assert.Equal(t, true, autoSchema.Get())
		assert.Equal(t, 13, colCount.Get())
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.

		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))

		// after update.
		assert.Equal(t, false, autoSchema.Get())
		assert.Equal(t, 0, colCount.Get())     // this should still return `default` value. not old value
		assert.Equal(t, false, asyncRep.Get()) // this field doesn't exist in original config file, should return default value.
	})

	t.Run("updating raft_drain_sleep", func(t *testing.T) {
		var raftDrainSleep runtime.DynamicValue[time.Duration]

		reg := &WeaviateRuntimeConfig{
			RaftDrainSleep: &raftDrainSleep,
		}

		// initial default
		assert.Equal(t, 0*time.Second, raftDrainSleep.Get())

		// set to 5s
		buf := []byte(`raft_drain_sleep: 5s`)
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Equal(t, 5*time.Second, raftDrainSleep.Get())

		// update to 10s
		buf = []byte(`raft_drain_sleep: 10s`)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Equal(t, 10*time.Second, raftDrainSleep.Get())

		// remove -> back to default
		buf = []byte(``)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Equal(t, 0*time.Second, raftDrainSleep.Get())
	})

	t.Run("updating raft_timeouts_multiplier", func(t *testing.T) {
		var raftTimeoutsMultiplier runtime.DynamicValue[int]

		reg := &WeaviateRuntimeConfig{
			RaftTimoutsMultiplier: &raftTimeoutsMultiplier,
		}

		// initial default
		assert.Equal(t, 0, raftTimeoutsMultiplier.Get())

		// set to 2
		buf := []byte(`raft_timeouts_multiplier: 2`)
		parsed, err := ParseRuntimeConfig(buf)
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Equal(t, 2, raftTimeoutsMultiplier.Get())

		// update to 3
		buf = []byte(`raft_timeouts_multiplier: 3`)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Equal(t, 3, raftTimeoutsMultiplier.Get())

		// remove -> back to default
		buf = []byte(``)
		parsed, err = ParseRuntimeConfig(buf)
		require.NoError(t, err)
		require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
		assert.Equal(t, 0, raftTimeoutsMultiplier.Get())
	})

	t.Run("updating objects ttl", func(t *testing.T) {
		deleteSchedule, _ := runtime.NewDynamicValueWithValidation("@every 1h", func(val string) error {
			if _, err := cron.StandardParser().Parse(val); err != nil {
				return fmt.Errorf("delete_schedule: %w", err)
			}
			return nil
		})
		findBatchSize, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLFindBatchSize, func(val int) error {
			return validatePositiveInt(val, "find_batch_size")
		})
		deleteBatchSize, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLDeleteBatchSize, func(val int) error {
			return validatePositiveInt(val, "delete_batch_size")
		})
		concurrencyFactor, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLConcurrencyFactor, func(val float64) error {
			return validatePositiveFloat(val, "concurrency_factor")
		})

		emptyBuf := []byte("")
		reg := &WeaviateRuntimeConfig{
			ObjectsTTLDeleteSchedule:    deleteSchedule,
			ObjectsTTLFindBatchSize:     findBatchSize,
			ObjectsTTLDeleteBatchSize:   deleteBatchSize,
			ObjectsTTLConcurrencyFactor: concurrencyFactor,
		}

		t.Run("delete schedule", func(t *testing.T) {
			buf := func(val string) []byte {
				return fmt.Appendf(nil, "objects_ttl_delete_schedule: %q", val)
			}

			// initial default
			assert.Equal(t, "@every 1h", deleteSchedule.Get())

			// set to 2h
			parsed, err := ParseRuntimeConfig(buf("@every 2h"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "@every 2h", deleteSchedule.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf("* * * * * *"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "@every 2h", deleteSchedule.Get())

			// update to 3h
			parsed, err = ParseRuntimeConfig(buf("@every 3h"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "@every 3h", deleteSchedule.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "@every 1h", deleteSchedule.Get())
		})

		t.Run("find batch size", func(t *testing.T) {
			buf := func(val int) []byte {
				return fmt.Appendf(nil, "objects_ttl_find_batch_size: %d", val)
			}

			// initial default
			assert.Equal(t, DefaultObjectsTTLFindBatchSize, findBatchSize.Get())

			// set to 20k
			parsed, err := ParseRuntimeConfig(buf(20_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20_000, findBatchSize.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf(-10_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20_000, findBatchSize.Get())

			// update to 30k
			parsed, err = ParseRuntimeConfig(buf(30_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 30_000, findBatchSize.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, DefaultObjectsTTLFindBatchSize, findBatchSize.Get())
		})

		t.Run("delete batch size", func(t *testing.T) {
			buf := func(val int) []byte {
				return fmt.Appendf(nil, "objects_ttl_delete_batch_size: %d", val)
			}

			// initial default
			assert.Equal(t, DefaultObjectsTTLDeleteBatchSize, deleteBatchSize.Get())

			// set to 20k
			parsed, err := ParseRuntimeConfig(buf(20_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20_000, deleteBatchSize.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf(-10_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20_000, deleteBatchSize.Get())

			// update to 30k
			parsed, err = ParseRuntimeConfig(buf(30_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 30_000, deleteBatchSize.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, DefaultObjectsTTLDeleteBatchSize, deleteBatchSize.Get())
		})

		t.Run("concurrency factor", func(t *testing.T) {
			buf := func(val float64) []byte {
				return fmt.Appendf(nil, "objects_ttl_concurrency_factor: %f", val)
			}

			// initial default
			assert.Equal(t, float64(DefaultObjectsTTLConcurrencyFactor), concurrencyFactor.Get())

			// set to 2
			parsed, err := ParseRuntimeConfig(buf(2))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 2., concurrencyFactor.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf(-1))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 2., concurrencyFactor.Get())

			// update to 3
			parsed, err = ParseRuntimeConfig(buf(3))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 3., concurrencyFactor.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, float64(DefaultObjectsTTLConcurrencyFactor), concurrencyFactor.Get())
		})
	})
}

// helpers
// assertConfigKey asserts if the `yaml` key is standard `lower_snake_case` (e.g: not `UPPER_CASE`)
func assertConfigKey(t *testing.T, key string) {
	t.Helper()

	re := regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*$`)
	if !re.MatchString(key) {
		t.Fatalf("given key %v is not lower snake case. The json/yaml tag for runtime config should be all lower snake case (e.g my_key, not MY_KEY)", key)
	}
}
