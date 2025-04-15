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
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

var (
	ErrEmptyConfig             = errors.New("empty runtime config")
	ErrFailedToOpenConfig      = errors.New("failed to open runtime config")
	ErrFailedToReadConfig      = errors.New("failed to read runtime config ")
	ErrFailedToParseConfig     = errors.New("failed to parse runtime config ")
	ErrUnregisteredConfigFound = errors.New("unregistered config found")
)

// ConfigValue is anything whose value can be `settable` by the config manager.
type ConfigValue interface {
	SetValue(t any) error
}

// ConfigValues represent dynamic config values that config manager manage.
type ConfigValues map[string]ConfigValue

// ConfigManager takes care of periodically loading the config from
// given filepath for every interval period.
type ConfigManager struct {
	// path is file path of config to load and unmarshal from
	path string
	// interval is how often config manager trigger loading the config file.
	interval time.Duration

	// currentConfig is last successfully loaded config.
	// ConfigManager keep using this config if there are any
	// failures to load new configs.
	currentConfig ConfigValues
	currentHash   string
	mu            sync.RWMutex // protects currentConfig

	log             logrus.FieldLogger
	lastLoadSuccess prometheus.Gauge
	configHash      *prometheus.GaugeVec

	// registered is the registered config values. This is used when marshal/unmarshal from
	// config file. Anything conflicts with registered values is invalid config file.
	registered ConfigValues
}

func NewConfigManager(
	filepath string,
	registered ConfigValues,
	interval time.Duration,
	log logrus.FieldLogger,
	r prometheus.Registerer,
) (*ConfigManager, error) {
	// catch empty filepath early
	if len(strings.TrimSpace(filepath)) == 0 {
		return nil, errors.New("filepath to load runtimeconfig is empty")
	}

	cm := &ConfigManager{
		path:     filepath,
		interval: interval,
		log:      log,
		lastLoadSuccess: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_runtime_config_last_load_success",
			Help: "Whether the last loading attempt of runtime config was success",
		}),
		configHash: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_runtime_config_hash",
			Help: "Hash value of the currently active runtime configuration",
		}, []string{"sha256"}), // sha256 is type of checksum and hard-coded for now
		registered:    registered,
		currentConfig: registered, // `loadConfig` would update to latest.
	}

	// try to load it once to fail early if configs are invalid
	if err := cm.loadConfig(); err != nil {
		return nil, err
	}

	return cm, nil
}

// Run is a blocking call that starts the configmanager actor. Consumer probably want to
// call it in different groutine. It also respects the passed in `ctx`.
// Meaning, cancelling the passed `ctx` stops the actor.
func (cm *ConfigManager) Run(ctx context.Context) error {
	return cm.loop(ctx)
}

// loadConfig reads and unmarshal the config from the file location.
func (cm *ConfigManager) loadConfig() error {
	f, err := os.Open(cm.path)
	if err != nil {
		cm.lastLoadSuccess.Set(0)
		return errors.Join(ErrFailedToOpenConfig, err)
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		cm.lastLoadSuccess.Set(0)
		return errors.Join(ErrFailedToReadConfig, err)
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(b))
	if hash == cm.currentHash {
		cm.lastLoadSuccess.Set(1)
		return nil // same file. no change
	}

	cfg, err := cm.parse(b)
	if err != nil {
		cm.lastLoadSuccess.Set(0)
		return errors.Join(ErrFailedToParseConfig, err)
	}

	if err := cm.updateConfig(cfg, hash); err != nil {
		return err
	}

	cm.lastLoadSuccess.Set(1)
	cm.configHash.Reset()
	cm.configHash.WithLabelValues(hash).Set(1)

	return nil
}

// updateConfig mutates the shared config
func (cm *ConfigManager) updateConfig(newcfg map[string]any, hash string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// invariant1: `newcfg` should not have any fields other than registered fileds
	// invariant2: `newcfg` can have subset of registered values.

	unknown := make([]string, 0)
	parseErrs := make([]error, 0)

	for k := range newcfg {
		_, ok := cm.registered[k]
		if !ok {
			unknown = append(unknown, k)
		}
	}

	if len(unknown) > 0 {
		return fmt.Errorf("%w: %v", ErrUnregisteredConfigFound, unknown)
	}

	for k, v := range newcfg {
		if err := cm.currentConfig[k].SetValue(v); err != nil {
			parseErrs = append(parseErrs, err)
		}
	}

	if len(parseErrs) > 0 {
		return errors.Join(parseErrs...)
	}

	cm.currentHash = hash

	return nil
}

// loop is a actor loop that runs forever till config manager is stopped.
// it orchestrates between "loading" configs and "stopping" the config manager
func (cm *ConfigManager) loop(ctx context.Context) error {
	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	// SIGHUP handler to trigger reload
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)

	for {
		select {
		case <-ticker.C:
			if err := cm.loadConfig(); err != nil {
				cm.log.Errorf("loading runtime config every %s failed, using old config: %w", cm.interval, err)
			}
		case <-sighup:
			if err := cm.loadConfig(); err != nil {
				cm.log.Error("loading runtime config through SIGHUP failed, using old config: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (cm *ConfigManager) parse(buf []byte) (map[string]any, error) {
	var values map[string]any

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.SetStrict(true)

	if err := dec.Decode(&values); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return values, nil
}

// // GetOverrides takes a config value and func to get it's runtime value.
// // It returns a value from func if available otherwise the passed in `val` if failing
// // to get value from the func().
// func GetOverrides[T any](val T, f func() *T) T {
// 	if f == nil {
// 		return val
// 	}
// 	x := f()

// 	if x == nil {
// 		return val
// 	}

// 	return *x
// }
