package janusgraph

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/mitchellh/mapstructure"
)

// Config represents the config outline for Janusgraph. The Database config shoud be of the following form:
// "database_config" : {
//     "url": "http://127.0.0.1:8182"
// }
type Config struct {
	URL             string          `mapstructure:"url"`
	AnalyticsEngine AnalyticsEngine `mapstructure:"analytics_engine"`
}

// AnalyticsEngine that can be used with Janusgraph connector are: spark-analtyics-api
type AnalyticsEngine struct {
	Enabled bool   `mapstructure:"enabled"`
	Type    string `mapstructure:"type"`
	URL     string `mapstructure:"url"`
}

func (c Config) validate(appConfig config.Environment) error {
	if c.URL == "" {
		return fmt.Errorf("url cannot be empty")
	}

	if c.AnalyticsEngine.Enabled != appConfig.AnalyticsEngine.Enabled {
		return fmt.Errorf("connector-specific field 'analytics_engine.enabled' must "+
			"match app-wide field 'analytics_engine.enabled: got '%v' for app-wide, but '%v' "+
			"for connector-specific config",
			c.AnalyticsEngine.Enabled, appConfig.AnalyticsEngine.Enabled)
	}

	if c.AnalyticsEngine.Enabled == false {
		// no need to validate these fields if it's not desired
		return nil
	}

	if c.AnalyticsEngine.Type != "spark-analytics-api" {
		return fmt.Errorf("invalid analtyics engine config: unsupported type '%s', allowed is 'spark-analytics-api'",
			c.AnalyticsEngine.Type)
	}

	if c.AnalyticsEngine.URL != "" {
		return fmt.Errorf("invalid analtyics engine config: url cannot be empty")
	}

	return nil
}

// setConfig sets variables, which can be placed in the config file section "database_config: {}"
// can be custom for any connector, in the example below there is only host and port available.
//
// Important to bear in mind;
// 1. You need to add these to the struct Config in this document.
// 2. They will become available via f.config.[variable-name]
//
// 	"database": {
// 		"name": "janusgraph",
// 		"database_config" : {
// 			"url": "http://127.0.0.1:8081"
// 		}
// 	},
func (f *Janusgraph) setConfig(config interface{}) error {
	// Mandatory: needed to add the JSON config represented as a map in f.config
	err := mapstructure.Decode(config, &f.config)
	if err != nil {
		return fmt.Errorf("could not decode connector-specific config: %v", err)
	}

	err = f.config.validate(f.appConfig)
	if err != nil {
		return fmt.Errorf("invalid config for field 'database_config': %v", err)
	}

	return nil
}
