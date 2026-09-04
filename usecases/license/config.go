//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package license wires the Weaviate license client (usecases/license/protocol)
// into a node: configuration from the environment, a logrus bridge, a
// Prometheus gauge, and the meta information exposed on /v1/meta.
//
// A node without LICENSE_KEY runs in community mode and never contacts the
// license service.
package license

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/usecases/license/protocol"
)

// Environment variables.
const (
	EnvKey         = "LICENSE_KEY"
	EnvKeyFile     = "LICENSE_KEY_FILE"
	EnvServerURL   = "LICENSE_SERVER_URL"
	EnvServerKeys  = "LICENSE_SERVER_KEYS"
	EnvClusterID   = "LICENSE_CLUSTER_ID"
	EnvEnforce     = "LICENSE_ENFORCE"
	EnvGracePeriod = "LICENSE_GRACE_PERIOD"
	EnvCachePath   = "LICENSE_CACHE_PATH"
)

// Config is the parsed license configuration.
type Config struct {
	// Key is the customer license key (wv8.<id>.<seed>). Empty means
	// community mode. Never logged.
	Key string `json:"-" yaml:"-"`
	// ServerURL overrides the license service, for testing.
	ServerURL string `json:"server_url" yaml:"server_url"`
	// ServerKeys are additional trusted server public keys as id:base64url,
	// merged with the keys embedded in the binary.
	ServerKeys []string `json:"server_keys" yaml:"server_keys"`
	// ClusterID, when set, is reported instead of the raft cluster ID.
	ClusterID string `json:"cluster_id" yaml:"cluster_id"`
	// Enforce degrades enterprise features after GracePeriod without a
	// valid answer. Off by default (log only).
	Enforce bool `json:"enforce" yaml:"enforce"`
	// GracePeriod defaults to 7 days.
	GracePeriod time.Duration `json:"grace_period" yaml:"grace_period"`
	// CachePath for the signed response cache; defaults to <dataPath>/license.json.
	CachePath string `json:"cache_path" yaml:"cache_path"`
}

// Enabled reports whether a key is configured.
func (c Config) Enabled() bool { return c.Key != "" }

// FromEnv reads the license configuration. dataPath is the persistence
// directory used for the default cache location.
func FromEnv(dataPath string) (Config, error) {
	var c Config
	c.Key = strings.TrimSpace(os.Getenv(EnvKey))
	if file := os.Getenv(EnvKeyFile); file != "" {
		if c.Key != "" {
			return c, fmt.Errorf("%s and %s are mutually exclusive", EnvKey, EnvKeyFile)
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return c, fmt.Errorf("%s: %w", EnvKeyFile, err)
		}
		c.Key = strings.TrimSpace(string(raw))
	}
	if c.Key != "" {
		if _, _, err := protocol.ParseKey(c.Key); err != nil {
			return c, fmt.Errorf("%s: %w", EnvKey, err)
		}
	}
	c.ServerURL = strings.TrimRight(strings.TrimSpace(os.Getenv(EnvServerURL)), "/")
	for _, k := range strings.Split(os.Getenv(EnvServerKeys), ",") {
		if k = strings.TrimSpace(k); k != "" {
			c.ServerKeys = append(c.ServerKeys, k)
		}
	}
	c.ClusterID = strings.TrimSpace(os.Getenv(EnvClusterID))
	c.Enforce = entcfg.Enabled(os.Getenv(EnvEnforce))
	c.GracePeriod = protocol.DefaultGracePeriod
	if v := os.Getenv(EnvGracePeriod); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return c, fmt.Errorf("%s must be a positive duration, got %q", EnvGracePeriod, v)
		}
		c.GracePeriod = d
	}
	c.CachePath = os.Getenv(EnvCachePath)
	if c.CachePath == "" && dataPath != "" {
		c.CachePath = filepath.Join(dataPath, "license.json")
	}
	return c, nil
}
