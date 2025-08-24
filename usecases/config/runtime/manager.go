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
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

var (
	ErrEmptyConfig             = errors.New("empty runtime config")
	ErrFailedToOpenConfig      = errors.New("failed to open runtime config")
	ErrFailedToReadConfig      = errors.New("failed to read runtime config ")
	ErrFailedToParseConfig     = errors.New("failed to parse runtime config ")
	ErrUnregisteredConfigFound = errors.New("unregistered config found")
)

// Parser takes care of unmarshaling a config struct
// from given raw bytes(e.g: YAML, JSON, etc).
type Parser[T any] func([]byte) (*T, error)

// Updater try to update `source` config with newly `parsed` config.
type Updater[T any] func(log logrus.FieldLogger, source, parsed *T, hooks map[string]func() error) error

// ConfigManager takes care of periodically loading the config from
// given filepath for every interval period.
type ConfigManager[T any] struct {
	// path is file path of config to load and unmarshal from
	path string
	// interval is how often config manager trigger loading the config file.
	interval time.Duration

	parse  Parser[T]
	update Updater[T]

	// currentConfig is last successfully loaded config.
	// ConfigManager keep using this config if there are any
	// failures to load new configs.
	currentConfig *T
	currentHash   string

	log             logrus.FieldLogger
	lastLoadSuccess prometheus.Gauge
	configHash      *prometheus.GaugeVec

	// exp hooks
	hooks map[string]func() error
}

func NewConfigManager[T any](
	filepath string,
	parser Parser[T],
	updater Updater[T],
	registered *T,
	interval time.Duration,
	log logrus.FieldLogger,
	hooks map[string]func() error,
	r prometheus.Registerer,
) (*ConfigManager[T], error) {
	// catch empty filepath early
	if len(strings.TrimSpace(filepath)) == 0 {
		return nil, errors.New("filepath to load runtimeconfig is empty")
	}

	cm := &ConfigManager[T]{
		path:     filepath,
		interval: interval,
		log:      log,
		parse:    parser,
		update:   updater,
		lastLoadSuccess: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "weaviate_runtime_config_last_load_success",
			Help: "Whether the last loading attempt of runtime config was success",
		}),
		configHash: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "weaviate_runtime_config_hash",
			Help: "Hash value of the currently active runtime configuration",
		}, []string{"sha256"}), // sha256 is type of checksum and hard-coded for now
		currentConfig: registered,
		hooks:         hooks,
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
func (cm *ConfigManager[T]) Run(ctx context.Context) error {
	return cm.loop(ctx)
}

// loadConfig reads and unmarshal the config from the file location.
func (cm *ConfigManager[T]) loadConfig() error {
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

	if err := cm.update(cm.log, cm.currentConfig, cfg, cm.hooks); err != nil {
		return err
	}

	cm.lastLoadSuccess.Set(1)
	cm.configHash.Reset()
	cm.configHash.WithLabelValues(hash).Set(1)
	cm.currentHash = hash

	return nil
}

// loop is a actor loop that runs forever till config manager is stopped.
// it orchestrates between "loading" configs and "stopping" the config manager
func (cm *ConfigManager[T]) loop(ctx context.Context) error {
	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	// SIGHUP handler to trigger reload
	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)

	for {
		select {
		case <-ticker.C:
			if err := cm.loadConfig(); err != nil {
				cm.log.Errorf("loading runtime config every %s failed, using old config: %v", cm.interval, err)
			}
		case <-sighup:
			if err := cm.loadConfig(); err != nil {
				cm.log.Error("loading runtime config through SIGHUP failed, using old config: %v", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
