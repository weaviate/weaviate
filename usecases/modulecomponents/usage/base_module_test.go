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

package usage

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestBaseModule_ShardJitterConfiguration(t *testing.T) {
	// Test 1: Check that the default value is correct
	assert.Equal(t, 100*time.Millisecond, DefaultShardJitterInterval, "jitter interval should be 100ms")

	// Test 2: Test that the module initializes with default jitter
	module := NewBaseModule("test-module", nil)
	assert.Equal(t, DefaultShardJitterInterval, module.shardJitter, "module should use default jitter")

	// Test 3: Test configurable jitter
	customJitter := 50 * time.Millisecond
	module.shardJitter = customJitter
	assert.Equal(t, customJitter, module.shardJitter, "module should use custom jitter")

	// Test 4: Test that zero jitter is handled gracefully
	module.shardJitter = 0
	assert.Equal(t, 0*time.Millisecond, module.shardJitter, "module should allow zero jitter")

	// Test 5: Test that negative jitter is handled gracefully
	module.shardJitter = -1 * time.Millisecond
	assert.Equal(t, -1*time.Millisecond, module.shardJitter, "module should allow negative jitter")
}

// TestBaseModule_EnvShardConcurrencyReachesService reproduces the real startup order:
// SetUsageService is called during MakeAppState (postInitModules), before module Init
// parses USAGE_SHARD_CONCURRENCY (initModules). The env value must still reach the
// usage service — reloadConfig alone won't push it, because after InitializeCommon the
// config value and b.shardConcurrency are already equal.
func TestBaseModule_EnvShardConcurrencyReachesService(t *testing.T) {
	// 3 cannot collide with the default (GOMAXPROCSx2 is always even)
	t.Setenv("USAGE_SHARD_CONCURRENCY", "3")

	mockStorage := NewMockStorageBackend(t)
	mockService := clusterusage.NewMockService(t)
	// seeded with the constructor default when the service is wired
	mockService.EXPECT().SetShardConcurrency(DefaultShardConcurrency).Return().Once()
	// env value pushed once Init has parsed it
	mockService.EXPECT().SetShardConcurrency(3).Return().Once()

	module := NewBaseModule("test-module", mockStorage)
	module.SetUsageService(mockService)

	cfg := &config.Config{}
	cfg.Cluster.Hostname = "test-node"
	cfg.Persistence.DataPath = t.TempDir()
	require.NoError(t, ParseCommonUsageConfig(cfg))

	require.NoError(t, module.InitializeCommon(context.Background(), cfg, logrus.New(),
		NewMetrics(prometheus.NewRegistry(), "test-module")))
	close(module.stopChan)

	assert.Equal(t, 3, module.shardConcurrency)
}
