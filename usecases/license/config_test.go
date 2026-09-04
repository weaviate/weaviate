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

package license

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/license/protocol"
)

func TestFromEnv(t *testing.T) {
	lic, err := protocol.Generate()
	require.NoError(t, err)
	key := lic.Key()

	t.Run("community mode when unset", func(t *testing.T) {
		cfg, err := FromEnv("/data")
		require.NoError(t, err)
		assert.False(t, cfg.Enabled())
		assert.Equal(t, protocol.DefaultGracePeriod, cfg.GracePeriod)
		assert.Equal(t, filepath.Join("/data", "license.json"), cfg.CachePath)
		assert.False(t, cfg.Enforce)
	})

	t.Run("full configuration", func(t *testing.T) {
		t.Setenv(EnvKey, "  "+key+"\n")
		t.Setenv(EnvServerURL, "http://localhost:8080/")
		t.Setenv(EnvServerKeys, "a:x, b:y")
		t.Setenv(EnvClusterID, "c-1")
		t.Setenv(EnvEnforce, "true")
		t.Setenv(EnvGracePeriod, "48h")
		t.Setenv(EnvCachePath, "/tmp/l.json")
		cfg, err := FromEnv("/data")
		require.NoError(t, err)
		assert.Equal(t, key, cfg.Key)
		assert.Equal(t, "http://localhost:8080", cfg.ServerURL)
		assert.Equal(t, []string{"a:x", "b:y"}, cfg.ServerKeys)
		assert.Equal(t, "c-1", cfg.ClusterID)
		assert.True(t, cfg.Enforce)
		assert.Equal(t, 48*time.Hour, cfg.GracePeriod)
		assert.Equal(t, "/tmp/l.json", cfg.CachePath)
	})

	t.Run("key from file", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "k")
		require.NoError(t, os.WriteFile(f, []byte(key+"\n"), 0o600))
		t.Setenv(EnvKeyFile, f)
		cfg, err := FromEnv("")
		require.NoError(t, err)
		assert.Equal(t, key, cfg.Key)
		assert.Empty(t, cfg.CachePath)
	})

	t.Run("errors", func(t *testing.T) {
		t.Setenv(EnvKey, "wv1.nope")
		_, err := FromEnv("")
		assert.Error(t, err)

		t.Setenv(EnvKey, key)
		t.Setenv(EnvKeyFile, "/x")
		_, err = FromEnv("")
		assert.ErrorContains(t, err, "mutually exclusive")

		t.Setenv(EnvKeyFile, "")
		t.Setenv(EnvGracePeriod, "-1h")
		_, err = FromEnv("")
		assert.ErrorContains(t, err, EnvGracePeriod)
	})
}

func TestTrustedServerKeys(t *testing.T) {
	set, err := TrustedServerKeys(Config{})
	require.NoError(t, err)
	assert.Len(t, set, len(embeddedServerKeys))

	set, err = TrustedServerKeys(Config{ServerKeys: []string{"k1:" + b64pub(t)}})
	require.NoError(t, err)
	assert.Contains(t, set, "k1")

	for _, bad := range []string{"noid", ":abc", "k:short"} {
		_, err := TrustedServerKeys(Config{ServerKeys: []string{bad}})
		assert.Error(t, err, bad)
	}
}
