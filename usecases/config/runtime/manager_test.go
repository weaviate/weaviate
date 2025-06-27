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
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type testConfig struct {
	BackupInterval *DynamicValue[time.Duration] `yaml:"backup_interval"`
}

func parseYaml(buf []byte) (*testConfig, error) {
	var c testConfig
	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.KnownFields(true)

	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func updater(_ logrus.FieldLogger, source, parsed *testConfig, _ map[string]func() error) error {
	source.BackupInterval.SetValue(parsed.BackupInterval.Get())
	return nil
}

func TestConfigManager_loadConfig(t *testing.T) {
	log, _ := test.NewNullLogger()
	registered := &testConfig{
		BackupInterval: NewDynamicValue(2 * time.Second),
	}

	t.Run("non-exist config should fail config manager at the startup", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		_, err := NewConfigManager("non-exist.yaml", parseYaml, updater, registered, 10*time.Millisecond, log, nil, reg)
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

		tmp, err := os.CreateTemp("", "invalid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		_, err = tmp.Write([]byte("backup_interval=10s")) // in-valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		_, err = NewConfigManager(tmp.Name(), parseYaml, updater, registered, 10*time.Millisecond, log, nil, reg)
		require.ErrorIs(t, err, ErrFailedToParseConfig)

		// assert: config_last_load_success=0 and no metric for config_hash
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 0
		`)))
	})

	t.Run("valid config should succeed creating config manager at the startup", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		_, err = NewConfigManager(tmp.Name(), parseYaml, updater, registered, 10*time.Millisecond, log, nil, reg)
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

		// calling parser => reloading the config
		loadCount := 0
		trackedParser := func(buf []byte) (*testConfig, error) {
			loadCount++
			return parseYaml(buf)
		}

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), trackedParser, updater, registered, 10*time.Millisecond, log, nil, reg)
		require.NoError(t, err)

		// assert: should have called `parser` only once during initial loading.
		assert.Equal(t, 1, loadCount)

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

			// Writing as two step process to avoid any race between writing and manager reading the file.
			writeFile(t, tmp.Name(), buf)

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

		// assert: changing config should have reloaded configs.
		assert.Equal(t, n+1, loadCount) // +1 is the initial loading of config.
	})

	t.Run("injecting new invalid config file should keep using old valid config", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()

		// calling parser => reloading the config
		loadCount := 0
		trackedParser := func(buf []byte) (*testConfig, error) {
			loadCount++
			return parseYaml(buf)
		}

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), trackedParser, updater, registered, 10*time.Millisecond, log, nil, reg)
		require.NoError(t, err)

		// assert: should have called `parser` only once during initial loading.
		assert.Equal(t, 1, loadCount)

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

		// assert: since new config is failing, it should keep re-loading
		assert.Greater(t, loadCount, 1)
	})
	t.Run("unchanged config should not reload", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()

		// calling parser => reloading the config
		var loadCount atomic.Int64
		trackedParser := func(buf []byte) (*testConfig, error) {
			loadCount.Add(1)
			return parseYaml(buf)
		}

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), trackedParser, updater, registered, 10*time.Millisecond, log, nil, reg)
		require.NoError(t, err)

		// assert: should have called `parser` only once during initial loading.
		assert.Equal(t, int64(1), loadCount.Load())

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
			// write same content. Writing as two step process to avoid any race between writing and manager reading the file.
			writeFile(t, tmp.Name(), buf)

			// give enough time to config manager to reload the previously written config
			time.Sleep(writeDelay)
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, int64(1), loadCount.Load())
			}, writeDelay, writeDelay/2)
		}

		// stop the manger
		cancel()
		wg.Wait() // config manager should have stopped correctly.

		// assert: writing same content shouldn't reload the config
		assert.Equal(t, int64(1), loadCount.Load()) // 1 is the initial loading of config.
	})
}

func TestConfigManager_GetConfig(t *testing.T) {
	log, _ := test.NewNullLogger()

	t.Run("receiving config should never block if manager is not reloading the config", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := &testConfig{
			BackupInterval: NewDynamicValue(2 * time.Second),
		}

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf) // valid yaml
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		_, err = NewConfigManager(tmp.Name(), parseYaml, updater, registered, 100*time.Millisecond, log, nil, reg)
		require.NoError(t, err)

		getConfigWait := make(chan struct{})

		var wg sync.WaitGroup

		// NOTE: we are not loading config anymore.

		n := 100 // 100 goroutine
		wg.Add(n)
		for i := 0; i < 100; i++ {
			go func() {
				defer wg.Done()
				<-getConfigWait // wait till all go routines ready to get the config
				assert.Equal(t, 10*time.Second, registered.BackupInterval.Get())
			}()
		}

		close(getConfigWait)
		wg.Wait()
	})

	t.Run("should receive latest config after last reload", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		registered := &testConfig{
			BackupInterval: NewDynamicValue(2 * time.Second),
		}

		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf)
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), parseYaml, updater, registered, 100*time.Millisecond, log, nil, reg)
		require.NoError(t, err)
		assertConfig(t, cm, registered, 10*time.Second)

		// change the config
		buf = []byte(`backup_interval: 20s`)
		require.NoError(t, os.WriteFile(tmp.Name(), buf, 0o777))

		require.NoError(t, cm.loadConfig()) // loading new config
		assertConfig(t, cm, registered, 20*time.Second)
	})
}

// helpers

func assertConfig(t *testing.T, cm *ConfigManager[testConfig], registered *testConfig, expected time.Duration) {
	getConfigWait := make(chan struct{})

	var wg sync.WaitGroup

	n := 100 // 100 goroutine
	wg.Add(n)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			<-getConfigWait // wait till all go routines ready to get the config
			assert.Equal(t, expected, registered.BackupInterval.Get())
		}()
	}

	close(getConfigWait)
	wg.Wait()
}

func writeFile(t *testing.T, path string, buf []byte) {
	t.Helper()

	tm := fmt.Sprintf("%s.tmp", path)
	err := os.WriteFile(tm, buf, 0o777)
	require.NoError(t, err)
	err = os.Rename(tm, path)
	require.NoError(t, err)
}
