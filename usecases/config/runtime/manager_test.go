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
	"gopkg.in/yaml.v2"
)

type testConfig struct {
	BackupInterval time.Duration `yaml:"backup_interval"`
}

func parseYaml(buf []byte) (*testConfig, error) {
	var c testConfig
	err := yaml.UnmarshalStrict(buf, &c)
	return &c, err
}

func TestConfigManager_loadConfig(t *testing.T) {
	log, _ := test.NewNullLogger()

	t.Run("non-exist config should fail config manager at the startup", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		_, err := NewConfigManager("non-exist.yaml", parseYaml, 10*time.Millisecond, log, reg)
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

		_, err = NewConfigManager(tmp.Name(), parseYaml, 10*time.Millisecond, log, reg)
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

		_, err = NewConfigManager(tmp.Name(), parseYaml, 10*time.Millisecond, log, reg)
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

		cm, err := NewConfigManager(tmp.Name(), trackedParser, 10*time.Millisecond, log, reg)
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
			err := os.WriteFile(tmp.Name(), buf, 0o777)
			require.NoError(t, err)
			// give enough time to config manager to reload the previously written config
			time.Sleep(writeDelay)

			// assert: new config_last_load_success=1 and config_hash should be changed as well.
			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 1
		`, fmt.Sprintf("%x", sha256.Sum256(buf))))))

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

		cm, err := NewConfigManager(tmp.Name(), trackedParser, 10*time.Millisecond, log, reg)
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
		time.Sleep(writeDelay)

		// assert: new config_last_load_success=0 and config_hash shouldn't have changed.
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
                # HELP weaviate_runtime_config_hash Hash value of the currently active runtime configuration
        	# TYPE weaviate_runtime_config_hash gauge
        	weaviate_runtime_config_hash{sha256="%s"} 1
		# HELP weaviate_runtime_config_last_load_success Whether the last loading attempt of runtime config was success
		# TYPE weaviate_runtime_config_last_load_success gauge
		weaviate_runtime_config_last_load_success 0
		`, fmt.Sprintf("%x", sha256.Sum256(buf)))))) // should have old valid config hash

		// stop the manger
		cancel()
		wg.Wait() // config manager should have stopped correctly.

		// assert: since new config is failing, it should keep re-loading
		assert.Greater(t, loadCount, 1)
	})
	t.Run("unchanged config should not reload", func(t *testing.T) {
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

		cm, err := NewConfigManager(tmp.Name(), trackedParser, 10*time.Millisecond, log, reg)
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
			// write same content
			err := os.WriteFile(tmp.Name(), buf, 0o777)
			require.NoError(t, err)
			// give enough time to config manager to reload the previously written config
			time.Sleep(writeDelay)
		}

		// stop the manger
		cancel()
		wg.Wait() // config manager should have stopped correctly.

		// assert: writing same content shouldn't reload the config
		assert.Equal(t, 1, loadCount) // 1 is the initial loading of config.
	})
}

func TestConfigManager_GetConfig(t *testing.T) {
	log, _ := test.NewNullLogger()

	t.Run("receiving config should never block if manager is not reloading the config", func(t *testing.T) {
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

		cm, err := NewConfigManager(tmp.Name(), parseYaml, 100*time.Millisecond, log, reg)
		require.NoError(t, err)

		getConfigWait := make(chan struct{})

		var wg sync.WaitGroup

		n := 100 // 100 goroutine
		wg.Add(n)
		for i := 0; i < 100; i++ {
			go func() {
				defer wg.Done()
				<-getConfigWait // wait till all go routines ready to get the config
				c, err := cm.Config()
				require.NoError(t, err)
				assert.Equal(t, 10*time.Second, c.BackupInterval)
			}()
		}

		close(getConfigWait)
		wg.Wait()
	})

	t.Run("should receive latest config after last reload", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		tmp, err := os.CreateTemp("", "valid_config.yaml")
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, os.Remove(tmp.Name()))
		})

		buf := []byte(`backup_interval: 10s`)

		_, err = tmp.Write(buf)
		require.NoError(t, err)
		require.NoError(t, tmp.Close())

		cm, err := NewConfigManager(tmp.Name(), parseYaml, 100*time.Millisecond, log, reg)
		require.NoError(t, err)
		assertConfig(t, cm, 10*time.Second)

		// change the config
		buf = []byte(`backup_interval: 20s`)
		require.NoError(t, os.WriteFile(tmp.Name(), buf, 0o777))

		require.NoError(t, cm.loadConfig()) // loading new config
		assertConfig(t, cm, 20*time.Second)
	})
}

// helpers

func assertConfig(t *testing.T, cm *ConfigManager[testConfig], expected time.Duration) {
	getConfigWait := make(chan struct{})

	var wg sync.WaitGroup

	n := 100 // 100 goroutine
	wg.Add(n)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			<-getConfigWait // wait till all go routines ready to get the config
			c, err := cm.Config()
			require.NoError(t, err)
			assert.Equal(t, expected, c.BackupInterval)
		}()
	}

	close(getConfigWait)
	wg.Wait()
}
