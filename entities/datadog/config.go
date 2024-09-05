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

package datadog

import (
	"errors"
	"os"
	"strings"

	"github.com/weaviate/weaviate/entities/config"
)

type ConfigOpts struct {
	Enabled           bool     `json:"enabled" yaml:"enabled"`
	Environment       string   `json:"environment" yaml:"environment"`
	Service           string   `json:"service" yaml:"service"`
	Version           string   `json:"version" yaml:"version"`
	Tags              []string `json:"tags" yaml:"tags"`
	ProfilingAllTypes bool     `json:"profiling_all_types" yaml:"profiling_all_types"`
}

// Config Global Singleton that can be accessed from anywhere in the app. This
// is required because panic recovery can happen anywhere in the app.
var Config *ConfigOpts

// InitDatadogConfig from environment. Errors if called more	than once.
func InitDatadogConfig() (*ConfigOpts, error) {
	if Config != nil {
		return nil, errors.New("datadog config already initialized")
	} else {
		Config = &ConfigOpts{}
	}

	Config.Enabled = config.Enabled(os.Getenv("DD_ENABLED"))
	if !Config.Enabled {
		return Config, nil
	}

	Config.Environment = os.Getenv("DD_ENV")
	Config.Service = os.Getenv("DD_SERVICE")
	Config.Version = os.Getenv("DD_VERSION")
	if tags := os.Getenv("DD_TAGS"); tags != "" {
		Config.Tags = strings.Split(tags, ",")
	} else {
		Config.Tags = []string{}
	}
	Config.ProfilingAllTypes = config.Enabled(os.Getenv("DD_PROFILING_ALL_TYPES"))

	return Config, nil
}

func Enabled() bool {
	if Config == nil {
		return false
	}
	return Config.Enabled
}
