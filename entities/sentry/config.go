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

package sentry

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/config"
)

// ConfigOpts all map to environment variables. For example:
//   - SENTRY_ENABLED=true -> ConfigOpts.Enabled=true
type ConfigOpts struct {
	Enabled        bool              `json:"enabled" yaml:"enabled"`
	DSN            string            `json:"dsn" yaml:"dsn"`
	Debug          bool              `json:"debug" yaml:"debug"`
	Tags           map[string]string `json:"tags" yaml:"tags"`
	Release        string            `json:"release" yaml:"release"`
	Environment    string            `json:"environment" yaml:"environment"`
	TracingEnabled bool              `json:"tracing_enabled" yaml:"tracing_enabled"`
	SampleRate     float64           `json:"sample_rate" yaml:"sample_rate"`
}

// Config Global Singleton that can be accessed from anywhere in the app. This
// is required because panic recovery can happen anywhere in the app.
var Config *ConfigOpts

// InitSentryConfig from environment. Errors if called more	than once.
func InitSentryConfig() (*ConfigOpts, error) {
	if Config != nil {
		return nil, fmt.Errorf("sentry config already initialized")
	} else {
		Config = &ConfigOpts{}
	}

	Config.Enabled = config.Enabled(os.Getenv("SENTRY_ENABLED"))
	if !Config.Enabled {
		return Config, nil
	}

	Config.DSN = os.Getenv("SENTRY_DSN")
	if Config.DSN == "" {
		return nil, fmt.Errorf("sentry enabled but no DSN provided")
	}

	Config.Environment = os.Getenv("SENTRY_ENVIRONMENT")
	if Config.Environment == "" {
		Config.Environment = "unknown"
	}

	Config.TracingEnabled = config.Enabled(os.Getenv("SENTRY_TRACING_ENABLED"))
	if sampleRate, err := strconv.ParseFloat(os.Getenv("SENTRY_SAMPLE_RATE"), 64); err == nil && sampleRate <= 1.0 && sampleRate >= 0.0 {
		Config.SampleRate = sampleRate
	} else {
		// By default always assume a very conservative sampling rate
		Config.SampleRate = 0.05
	}

	Config.Debug = config.Enabled(os.Getenv("SENTRY_DEBUG"))
	Config.Release = os.Getenv("SENTRY_RELEASE")

	if tags, err := parseTags(); err != nil {
		return nil, err
	} else {
		Config.Tags = tags
	}

	return Config, nil
}

var (
	tagKeyPattern   = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,32}$`)
	tagValuePattern = regexp.MustCompile(`^[^\n]{1,200}$`)
)

func parseTags() (map[string]string, error) {
	tags := make(map[string]string)
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "SENTRY_TAG_") {
			continue
		}

		parts := strings.SplitN(env[len("SENTRY_TAG_"):], "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]
		if !tagKeyPattern.MatchString(key) {
			return nil, errors.New("invalid tag key: " + key)
		}
		if !tagValuePattern.MatchString(value) {
			return nil, errors.New("invalid tag value for key: " + key)
		}
		tags[key] = value
	}
	return tags, nil
}

func Enabled() bool {
	if Config == nil {
		return false
	}
	return Config.Enabled
}
