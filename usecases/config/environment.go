//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package config

import (
	"os"
	"strconv"

	"github.com/pkg/errors"
)

// FromEnv takes a *Config as it will respect initial config that has been
// provided by other means (e.g. a config file) and will only extend those that
// are set
func FromEnv(config *Config) error {
	if enabled(os.Getenv("AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED")) {
		config.Authentication.AnonymousAccess.Enabled = true
	}

	if enabled(os.Getenv("AUTHENTICATION_OIDC_ENABLED")) {
		config.Authentication.OIDC.Enabled = true

		if enabled(os.Getenv("AUTHENTICATION_OIDC_SKIP_CLIENT_ID_CHECK")) {
			config.Authentication.OIDC.SkipClientIDCheck = true
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_ISSUER"); v != "" {
			config.Authentication.OIDC.Issuer = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_CLIENT_ID"); v != "" {
			config.Authentication.OIDC.ClientID = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_USERNAME_CLAIM"); v != "" {
			config.Authentication.OIDC.UsernameClaim = v
		}

		if v := os.Getenv("AUTHENTICATION_OIDC_GROUPS_CLAIM"); v != "" {
			config.Authentication.OIDC.GroupsClaim = v
		}
	}

	if enabled(os.Getenv("STANDALONE_MODE")) {
		config.Standalone = true

		if v := os.Getenv("PERSISTENCE_DATA_PATH"); v != "" {
			config.Persistence.DataPath = v
		}
	}

	if v := os.Getenv("CONFIGURATION_STORAGE_URL"); v != "" {
		config.ConfigurationStorage.URL = v
	}

	if v := os.Getenv("ORIGIN"); v != "" {
		config.Origin = v
	}

	if v := os.Getenv("CONTEXTIONARY_URL"); v != "" {
		config.Contextionary.URL = v
	}

	if v := os.Getenv("QUERY_DEFAULTS_LIMIT"); v != "" {
		asInt, err := strconv.Atoi(v)
		if err != nil {
			return errors.Wrapf(err, "parse QUERY_DEFAULTS_LIMIT as int")
		}

		config.QueryDefaults.Limit = int64(asInt)
	}

	if v := os.Getenv("ESVECTOR_URL"); v != "" {
		config.VectorIndex.URL = v

		if v := os.Getenv("ESVECTOR_NUMBER_OF_SHARDS"); v != "" {
			asInt, err := strconv.Atoi(v)
			if err != nil {
				return errors.Wrapf(err, "parse ESVECTOR_NUMBER_OF_SHARDS as int")
			}

			config.VectorIndex.NumberOfShards = &asInt
		}

		if v := os.Getenv("ESVECTOR_AUTO_EXPAND_REPLICAS"); v != "" {
			config.VectorIndex.AutoExpandReplicas = &v
		}
	}

	// Disable by default
	config.Telemetry.Disabled = true
	if v := os.Getenv("TELEMETRY_DISABLED"); v != "false" {
		config.Telemetry.Disabled = false

		if v := os.Getenv("TELEMETRY_INTERVAL"); v != "" {
			asInt, err := strconv.Atoi(v)
			if err != nil {
				return errors.Wrapf(err, "parse TELEMETRY_INTERVAL as int")
			}

			config.Telemetry.Interval = asInt
		}

		if v := os.Getenv("TELEMETRY_REMOTE_URL"); v != "" {
			config.Telemetry.RemoteURL = v
		}
	}

	return nil
}

func enabled(value string) bool {
	if value == "" {
		return false
	}

	if value == "on" ||
		value == "enabeld" ||
		value == "1" ||
		value == "true" {
		return true
	}

	return false
}
