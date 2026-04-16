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
	"os"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/weaviate/weaviate/usecases/config/parser"
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
		deleteSchedule, _ := runtime.NewDynamicValueWithValidation("@every 1h", parser.ValidateGocronSchedule)
		batchSize, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLBatchSize, parser.ValidateIntGreaterThanEqual0)
		pauseEveryNoBatches, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLPauseEveryNoBatches, parser.ValidateIntGreaterThanEqual0)
		pauseDuration, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLPauseDuration, parser.ValidateDurationGreaterThanEqual0)
		concurrencyFactor, _ := runtime.NewDynamicValueWithValidation(DefaultObjectsTTLConcurrencyFactor, parser.ValidateFloatGreaterThan0)

		emptyBuf := []byte("")
		reg := &WeaviateRuntimeConfig{
			ObjectsTTLDeleteSchedule:      deleteSchedule,
			ObjectsTTLBatchSize:           batchSize,
			ObjectsTTLPauseEveryNoBatches: pauseEveryNoBatches,
			ObjectsTTLPauseDuration:       pauseDuration,
			ObjectsTTLConcurrencyFactor:   concurrencyFactor,
		}

		t.Run("delete schedule", func(t *testing.T) {
			buf := func(val string) []byte {
				return fmt.Appendf(nil, "objects_ttl_delete_schedule: %q", val)
			}

			// initial default
			assert.Equal(t, "@every 1h", deleteSchedule.Get())

			// set to 2h (without seconds)
			parsed, err := ParseRuntimeConfig(buf("0 */2 * * *"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "0 */2 * * *", deleteSchedule.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf("* * * *"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "0 */2 * * *", deleteSchedule.Get())

			// update to 3h (with seconds)
			parsed, err = ParseRuntimeConfig(buf("0 0 */3 * * *"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "0 0 */3 * * *", deleteSchedule.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, "@every 1h", deleteSchedule.Get())
		})

		t.Run("batch size", func(t *testing.T) {
			buf := func(val int) []byte {
				return fmt.Appendf(nil, "objects_ttl_batch_size: %d", val)
			}

			// initial default
			assert.Equal(t, DefaultObjectsTTLBatchSize, batchSize.Get())

			// set to 20k
			parsed, err := ParseRuntimeConfig(buf(20_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20_000, batchSize.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf(-10_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20_000, batchSize.Get())

			// update to 30k
			parsed, err = ParseRuntimeConfig(buf(30_000))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 30_000, batchSize.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, DefaultObjectsTTLBatchSize, batchSize.Get())
		})

		t.Run("pause every number batches", func(t *testing.T) {
			buf := func(val int) []byte {
				return fmt.Appendf(nil, "objects_ttl_pause_every_no_batches: %d", val)
			}

			// initial default
			assert.Equal(t, DefaultObjectsTTLPauseEveryNoBatches, pauseEveryNoBatches.Get())

			// set to 20
			parsed, err := ParseRuntimeConfig(buf(20))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20, pauseEveryNoBatches.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf(-10))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 20, pauseEveryNoBatches.Get())

			// update to 30
			parsed, err = ParseRuntimeConfig(buf(30))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 30, pauseEveryNoBatches.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, DefaultObjectsTTLPauseEveryNoBatches, pauseEveryNoBatches.Get())
		})

		t.Run("pause duration", func(t *testing.T) {
			buf := func(val string) []byte {
				return fmt.Appendf(nil, "objects_ttl_pause_duration: %s", val)
			}

			// initial default
			assert.Equal(t, DefaultObjectsTTLPauseDuration, pauseDuration.Get())

			// set to 2 mins
			parsed, err := ParseRuntimeConfig(buf("2m"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 2*time.Minute, pauseDuration.Get())

			// try set invalid value
			parsed, err = ParseRuntimeConfig(buf("-1h"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 2*time.Minute, pauseDuration.Get())

			// update to 3 hours
			parsed, err = ParseRuntimeConfig(buf("3h"))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, 3*time.Hour, pauseDuration.Get())

			// remove -> back to default
			parsed, err = ParseRuntimeConfig(emptyBuf)
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, reg, parsed, nil))
			assert.Equal(t, DefaultObjectsTTLPauseDuration, pauseDuration.Get())
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

// TestExportDefaultPathRuntimeOverride verifies the end-to-end wiring that
// lets an operator configure exports for the first time via the runtime
// config file (no env var, no startup YAML).
//
// Because Export.IsDefaultPathSet is intentionally not exposed in
// WeaviateRuntimeConfig (it would be a footgun — operators could toggle the
// "set" flag independently of DefaultPath and bypass the path-required
// check), we rely on a hook registered against the "ExportDefaultPath" field
// to flip IsDefaultPathSet whenever DefaultPath is updated via runtime
// overrides. This test asserts that flipping behavior.
//
// Known limitation: the runtime config hook system only fires on value
// *changes* (see updateRuntimeConfig in runtimeconfig.go, which records a
// log entry only when old != new). An operator who runs without any startup
// export config (DefaultPath="") and then writes literally
// `export_default_path: ""` into the runtime config will NOT flip
// IsDefaultPathSet, because the value is the same as the startup default and
// the hook never fires. In practice this is a non-issue: any non-empty
// value works, and operators who genuinely want the empty-prefix case can
// set EXPORT_DEFAULT_PATH="" at startup instead.
func TestExportDefaultPathRuntimeOverride(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	tests := []struct {
		name             string
		initialPath      string // startup value for source.ExportDefaultPath
		initialPathSet   bool   // startup value for IsDefaultPathSet
		runtimeConfig    string // YAML applied via UpdateRuntimeConfig
		expectedPath     string
		expectedPathSet  bool
		assertionMessage string
	}{
		{
			name:             "override with non-empty path flips IsDefaultPathSet from false to true",
			initialPath:      "",
			initialPathSet:   false,
			runtimeConfig:    `export_default_path: "from/runtime"`,
			expectedPath:     "from/runtime",
			expectedPathSet:  true,
			assertionMessage: "hook should have flipped IsDefaultPathSet to true when ExportDefaultPath was updated",
		},
		{
			name:            "override switching non-empty path to another non-empty path keeps it set",
			initialPath:     "initial/path",
			initialPathSet:  true,
			runtimeConfig:   `export_default_path: "new/path"`,
			expectedPath:    "new/path",
			expectedPathSet: true,
		},
		{
			name:             "override from non-empty to empty string keeps it set (empty is a valid explicit choice)",
			initialPath:      "initial/path",
			initialPathSet:   true,
			runtimeConfig:    `export_default_path: ""`,
			expectedPath:     "",
			expectedPathSet:  true,
			assertionMessage: "empty string is a valid explicit choice; hook must fire on any change",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defaultPath := runtime.NewDynamicValue(tt.initialPath)
			defaultPathSet := new(atomic.Bool)
			defaultPathSet.Store(tt.initialPathSet)
			source := &WeaviateRuntimeConfig{
				ExportDefaultPath: defaultPath,
			}
			// this is the hook that configure_api.go registers in production
			hooks := map[string]func() error{
				"ExportDefaultPath": func() error {
					defaultPathSet.Store(true)
					return nil
				},
			}

			parsed, err := ParseRuntimeConfig([]byte(tt.runtimeConfig))
			require.NoError(t, err)
			require.NoError(t, UpdateRuntimeConfig(log, source, parsed, hooks))

			assert.Equal(t, tt.expectedPath, defaultPath.Get())
			assert.Equal(t, tt.expectedPathSet, defaultPathSet.Load(), tt.assertionMessage)
		})
	}

	t.Run("ExportIsDefaultPathSet is not a user-facing runtime config knob", func(t *testing.T) {
		// Regression: if someone ever re-adds ExportIsDefaultPathSet to
		// WeaviateRuntimeConfig, operators could bypass the path-required
		// check. This test ensures the field stays absent.
		parsed, err := ParseRuntimeConfig([]byte(`export_default_path_set: true`))
		require.NoError(t, err) // parser tolerates unknown keys

		// The struct must not carry a field corresponding to the key. We
		// round-trip via YAML marshaling to observe the exposed fields.
		yd, err := yaml.Marshal(parsed)
		require.NoError(t, err)
		var roundTripped map[string]any
		require.NoError(t, yaml.Unmarshal(yd, &roundTripped))
		_, present := roundTripped["export_default_path_set"]
		assert.False(t, present,
			"export_default_path_set must not be a runtime config field; it is a derived internal flag")
	})
}

// TestExportDefaultPathRuntimeOverrideFullFlow exercises the real
// NewConfigManager startup sequence to catch the "initial load runs without
// hooks" bug. The bug: NewConfigManager calls cm.loadConfig() internally
// *before* hooks are registered, so if the runtime config file sets
// export_default_path, SetValue is called on source.ExportDefaultPath but
// the ExportDefaultPath hook never fires. A subsequent forced ReloadConfig
// (to apply hooks) then sees no value change and still doesn't fire the
// hook. Without the explicit post-registration sync in postInitRuntimeOverrides,
// IsDefaultPathSet would stay false and the scheduler would reject exports.
func TestExportDefaultPathRuntimeOverrideFullFlow(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	// newHooks returns the hook that configure_api.go registers in production.
	newHooks := func(defaultPathSet *atomic.Bool) map[string]func() error {
		return map[string]func() error{
			"ExportDefaultPath": func() error {
				defaultPathSet.Store(true)
				return nil
			},
		}
	}

	// syncIsDefaultPathSet implements the post-registration manual sync.
	// configure_api.go's postInitRuntimeOverrides does the same after
	// cm.RegisterHooks to handle the initial-load case (where hooks weren't
	// registered yet).
	syncIsDefaultPathSet := func(source *WeaviateRuntimeConfig, defaultPathSet *atomic.Bool) {
		if source.ExportDefaultPath != nil && source.ExportDefaultPath.Get() != "" {
			defaultPathSet.Store(true)
		}
	}

	writeRuntimeConfig := func(t *testing.T, content string) string {
		t.Helper()
		f, err := os.CreateTemp(t.TempDir(), "runtime_config_*.yaml")
		require.NoError(t, err)
		_, err = f.WriteString(content)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return f.Name()
	}

	tests := []struct {
		name             string
		runtimeConfig    string
		expectedPath     string
		expectedPathSet  bool
		assertionMessage string
	}{
		{
			name:             "non-empty path in runtime config → scheduler accepts exports",
			runtimeConfig:    `export_default_path: "from/runtime"`,
			expectedPath:     "from/runtime",
			expectedPathSet:  true,
			assertionMessage: "manual post-registration sync must flip IsDefaultPathSet when runtime config set a non-empty path",
		},
		{
			name:             "no export_default_path line → IsDefaultPathSet stays false",
			runtimeConfig:    `# no export config here`,
			expectedPath:     "",
			expectedPathSet:  false,
			assertionMessage: "no runtime config entry → nothing should flip IsDefaultPathSet",
		},
		{
			// Documented edge case: an operator with no env/YAML config
			// writing `export_default_path: ""` only via runtime config at
			// startup. The manual sync uses Get() != "" as its heuristic
			// and cannot distinguish "operator set empty" from "nothing
			// set", so IsDefaultPathSet stays false.
			name:             "empty path only via runtime config at startup is a documented limitation",
			runtimeConfig:    `export_default_path: ""`,
			expectedPath:     "",
			expectedPathSet:  false,
			assertionMessage: "documented edge case: empty-string-only via runtime config at startup does not flip the flag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// parseExportConfig at startup with no env and no YAML:
			//   DefaultPath = NewDynamicValue("") [def="", val=nil]
			//   IsDefaultPathSet = new(atomic.Bool) [false]
			source := &WeaviateRuntimeConfig{
				ExportDefaultPath: runtime.NewDynamicValue(""),
			}
			defaultPathSet := new(atomic.Bool)

			path := writeRuntimeConfig(t, tt.runtimeConfig)

			// NewConfigManager runs the initial loadConfig internally with
			// hooks=nil, silently updating source.ExportDefaultPath.
			cm, err := runtime.NewConfigManager(
				path, ParseRuntimeConfig, UpdateRuntimeConfig, source,
				100*time.Millisecond, log, prometheus.NewPedanticRegistry(),
			)
			require.NoError(t, err)

			// Register hooks and force a reload. The forced reload sees
			// oldV == newV, so change-based hooks cannot fire here either.
			// This is the gap that postInitRuntimeOverrides' manual sync
			// exists to close.
			cm.RegisterHooks(newHooks(defaultPathSet))
			require.NoError(t, cm.ReloadConfig())

			// The explicit manual sync in postInitRuntimeOverrides catches
			// the case by observing that DefaultPath is non-empty.
			syncIsDefaultPathSet(source, defaultPathSet)

			assert.Equal(t, tt.expectedPath, source.ExportDefaultPath.Get())
			assert.Equal(t, tt.expectedPathSet, defaultPathSet.Load(), tt.assertionMessage)
		})
	}
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
