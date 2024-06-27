package sentry

import (
	"fmt"
	"os"

	"github.com/weaviate/weaviate/entities/config"
)

// ConfigOpts all map to environment variables. For example:
//   - SENTRY_ENABLED=true -> ConfigOpts.Enabled=true
type ConfigOpts struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	DSN     string `json:"dsn" yaml:"dsn"`
	Debug   bool   `json:"debug" yaml:"debug"`
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

	Config.Debug = config.Enabled(os.Getenv("SENTRY_DEBUG"))
	return Config, nil
}

func Enabled() bool {
	if Config == nil {
		return false
	}
	return Config.Enabled
}
