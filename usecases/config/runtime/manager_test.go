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

package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	BackupInterval time.Duration `yaml:"backup_interval"`
}

func TestConfigManager_loadConfig(t *testing.T) {
	log, _ := test.NewNullLogger()

	t.Run("non-exist config should fail config manager at the startup", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)
		_, err := NewConfigManager("non-exist.yaml", registered, 10*time.Millisecond, log, reg)
		require.ErrorIs(t, err, ErrFailedToOpenConfig)

		// assert: config_last_load_success=0 and no metric for config_hash
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 0
		`)))
	})

	t.Run("invalid config should fail config manager at the startup", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)

		tmp, err := os.CreateTemp("", "invalid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		_, err = tmp.Write([]byte("backup_interval=10s")) // in-valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		_, err = NewConfigManager(tmp.Name(), registered, 10*time.Millisecond, log, reg)
		require.ErrorIs(t, err, ErrFailedToParseConfig)

		// assert: config_last_load_success=0 and no metric for config_hash
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 0
		`)))
	})

	t.Run("having unregistered configs in config file should failed to reload config", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		_, err = NewConfigManager(tmp.Name(), registered, 10*time.Millisecond, log, reg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnregisteredConfigFound)
	})

	t.Run("valid config should succeed creating config manager at the startup", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)
		registered["backup_interval"] = NewDynamicValue[time.Duration](1 * time.Second)

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		_, err = NewConfigManager(tmp.Name(), registered, 10*time.Millisecond, log, reg)
		fmt.Println("Debug!!! error", err)
		require.NoError(t, err)

		// assert: config_last_load_success=1 and config_hash should be set.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))
	})

	t.Run("changing config file should reload the config", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)
		registered["backup_interval"] = NewDynamicValue(1 * time.Second)

		// changing the `hash` label means reloading the config

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), registered, 10*time.Millisecond, log, reg)
		require.NoError(t, err)

		// assert: config_last_load_success=1 and config_hash should be set.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))

		// Now let's change the config file few times
		var (
			wg          sync.WaitGroup
			ctx, cancel = context.WithCancel(context.Background())
		)
		defer cancel() // being good citizen.

		wg.Add(1)
		go func() {
			defer wg.Done()
			cm.Run(ctx)
		}()

		n := 3 // number of times we change the config file after initial reload
		writeDelay := 100 * time.Millisecond
		for i := 0; i < n; i++ {
			// write different config every time
			buf := []byte(fmt.Sprintf("backup_interval: %ds", i+1))
			err := os.WriteFile(tmp.Name(), buf, 0o777)
			require.NoError(t, err)

			// give enough time to config manager to reload the previously written config
			// assert: new config_last_load_success=1 and config_hash should be changed as well.
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.NoError(c, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))
			}, writeDelay, writeDelay/2)
		}

		// stop the manger
		cancel()
		wg.Wait() // config manager should have stopped correctly.
	})

	t.Run("injecting new invalid config file should keep using old valid config", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)
		registered["backup_interval"] = NewDynamicValue(1 * time.Second)

		// different "hash" => reloading the config

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), registered, 10*time.Millisecond, log, reg)
		require.NoError(t, err)
		// assert: config_last_load_success=1 and config_hash should be set.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))

		// Now let's inject invalid config file
		var (
			wg          sync.WaitGroup
			ctx, cancel = context.WithCancel(context.Background())
		)
		defer cancel() // being good citizen.

		wg.Add(1)
		go func() {
			defer wg.Done()
			cm.Run(ctx)
		}()

		writeDelay := 100 * time.Millisecond
		// write different config every time
		xbuf := []byte(`backup_interval=10s`) // invalid yaml
		err = os.WriteFile(tmp.Name(), xbuf, 0o777)
		require.NoError(t, err)

		// give enough time to config manager to reload the previously written config
		// assert: new config_last_load_success=0 and config_hash shouldn't have changed.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.NoError(c, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 0
		`, fmt.Sprintf("%x", sha256.Sum256(buf)))))) // should have old valid config hash
		}, writeDelay, writeDelay/2)

		// stop the manger
		cancel()
		wg.Wait() // config manager should have stopped correctly.
	})
	t.Run("unchanged config should not reload", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := make(ConfigValues)
		registered["backup_interval"] = NewDynamicValue(1 * time.Second)

		// no `hash` change => not reloaded

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), registered, 10*time.Millisecond, log, reg)
		require.NoError(t, err)

		// assert: config_last_load_success=1 and config_hash should be set.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))

		// Now let's change the config file few times
		var (
			wg          sync.WaitGroup
			ctx, cancel = context.WithCancel(context.Background())
		)
		defer cancel() // being good citizen.

		wg.Add(1)
		go func() {
			defer wg.Done()
			cm.Run(ctx)
		}()

		n := 3 // number of times we change the config file after initial reload
		writeDelay := 100 * time.Millisecond
		for i := 0; i < n; i++ {
			// write same content
			err := os.WriteFile(tmp.Name(), buf, 0o777)
			require.NoError(t, err)

			// give enough time to config manager to reload the previously written config
			// assert: new config_last_load_success=1 and config_hash should be changed as well.
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.NoError(c, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))
			}, writeDelay, writeDelay/2)
		}

		// stop the manger
		cancel()
		wg.Wait() // config manager should have stopped correctly.
	})
}
